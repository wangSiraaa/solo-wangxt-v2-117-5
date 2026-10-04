'use strict';

const crypto = require('node:crypto');
const db = require('./db');
const yutil = require('./yutil');
const { rebuildVerified, recoverFromStore } = require('./compaction');

// Copy contract:
//   A copy is a NEW document whose entire content is the source room's
//   current consistent Yjs state, persisted as an initial snapshot
//   (through_seq = 0, update_count = 0). No update history and no
//   memberships are carried over; the initiator becomes the sole owner of
//   the copy. Afterwards source and copy evolve independently: separate
//   update logs, separate snapshots, separate rooms.
//
// Runs inside the source room's serial queue, so the encoded state cannot
// interleave with an in-flight update or compaction of the source.

const MAX_ID_ATTEMPTS = 5;

async function copyDocument(room, { title, userId, tenantId }) {
  return room.enqueue(async () => {
    // Consistent cut of the live document: the queue guarantees no update
    // is applied to room.doc while we encode and persist it.
    const stateBytes = yutil.encodeState(room.doc);
    const stateHash = yutil.sha256(stateBytes);

    for (let attempt = 0; attempt < MAX_ID_ATTEMPTS; attempt++) {
      const newDocId = `doc-${crypto.randomBytes(8).toString('hex')}`;
      const client = await db.getClient();
      try {
        await client.query('BEGIN');
        const docIns = await client.query(
          `INSERT INTO documents (id, tenant_id, title)
           VALUES ($1,$2,$3)
           ON CONFLICT (id) DO NOTHING
           RETURNING id`,
          [newDocId, tenantId, title],
        );
        if (docIns.rowCount === 0) {
          // Vanishingly unlikely id collision: roll back and draw a new id.
          await client.query('ROLLBACK');
          continue;
        }
        // Sole member of the copy: the initiator, as owner. Source
        // memberships are deliberately NOT carried over.
        await client.query(
          `INSERT INTO document_members (doc_id, user_id, role)
           VALUES ($1,$2,'owner')`,
          [newDocId, userId],
        );
        // The copy starts with no update log; its initial state is a
        // snapshot, so recovery is snapshot(0) + tail(seq > 0) from the
        // very first edit on.
        const snapIns = await client.query(
          `INSERT INTO doc_snapshots (doc_id, through_seq, state_bytes, state_hash, update_count)
           VALUES ($1, 0, $2, $3, 0)
           RETURNING id`,
          [newDocId, stateBytes, stateHash],
        );
        const snapshotId = snapIns.rows[0].id;

        // Verification #1 (pre-commit): rebuild purely from the rows this
        // transaction just wrote and demand byte-equality with the live
        // source state. Any mismatch rolls back, leaving no half copy.
        const rebuilt = await rebuildVerified(client, newDocId);
        if (!yutil.encodeState(rebuilt).equals(stateBytes)) {
          await client.query('ROLLBACK');
          throw new Error('copy verification mismatch: stored snapshot != source state');
        }
        await client.query('COMMIT');

        // Verification #2 (post-commit): recover through a fresh pooled
        // connection — exactly what a restarted process would load.
        const recovered = await recoverFromStore(newDocId);
        if (!yutil.encodeState(recovered).equals(stateBytes)) {
          throw new Error('post-copy recovery mismatch: stored snapshot != source state');
        }

        return {
          copied: true,
          docId: newDocId,
          title,
          sourceDocId: room.docId,
          stateHash,
          stateLen: stateBytes.length,
          snapshotId,
        };
      } catch (e) {
        await client.query('ROLLBACK').catch(() => {});
        throw e;
      } finally {
        client.release();
      }
    }
    throw new Error('could not allocate a fresh document id');
  });
}

module.exports = { copyDocument };
