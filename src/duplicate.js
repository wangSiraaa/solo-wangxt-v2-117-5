'use strict';

const crypto = require('node:crypto');
const db = require('./db');
const yutil = require('./yutil');

// Duplicate a document: take the source room's current consistent Yjs
// state and materialize it as a brand-new document — one `documents` row,
// exactly one membership (the initiator as owner), and one initial
// snapshot the copy can recover from. No update history and no other
// members are carried over; source and copy then evolve independently.

const TITLE_MAX = 200;
const DOC_ID_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;

// A valid title is a non-empty string (after trimming) within the length
// cap. Returns the normalized title or null.
function validateTitle(title) {
  if (typeof title !== 'string') return null;
  const t = title.trim();
  if (!t || t.length > TITLE_MAX) return null;
  return t;
}

function validateRequestedId(id) {
  return typeof id === 'string' && DOC_ID_RE.test(id);
}

async function duplicateDocument(room, { session, title, requestedId }) {
  // Serialized on the source room's queue together with updates and
  // compaction: the copied state is a consistent cut — every update
  // persisted before this point is included, none after can interleave
  // with the read.
  return room.enqueue(async () => {
    const stateBytes = yutil.encodeState(room.doc);
    const stateHash = yutil.sha256(stateBytes);
    const sourceSeq = room.loadedSeq;
    const newDocId = requestedId || `doc-${crypto.randomBytes(8).toString('hex')}`;

    const client = await db.getClient();
    try {
      await client.query('BEGIN');
      // One transaction for all three rows: a rejected or conflicting
      // request rolls back cleanly and leaves no half-created document.
      await client.query(
        `INSERT INTO documents (id, tenant_id, title) VALUES ($1,$2,$3)`,
        [newDocId, session.tenant_id, title],
      );
      await client.query(
        `INSERT INTO document_members (doc_id, user_id, role) VALUES ($1,$2,'owner')`,
        [newDocId, session.user_id],
      );
      // through_seq = 0: the copy has no update log yet, so recovery is
      // snapshot(0) + updates(seq > 0) — the same path compaction uses.
      await client.query(
        `INSERT INTO doc_snapshots (doc_id, through_seq, state_bytes, state_hash, update_count)
         VALUES ($1, 0, $2, $3, 0)`,
        [newDocId, stateBytes, stateHash],
      );
      await client.query('COMMIT');
    } catch (e) {
      await client.query('ROLLBACK').catch(() => {});
      if (e && e.code === '23505') {
        const err = new Error(`document id already exists: ${newDocId}`);
        err.code = 'DOC_ID_TAKEN';
        throw err;
      }
      throw e;
    } finally {
      client.release();
    }

    return {
      docId: newDocId,
      title,
      stateHash,
      stateLen: stateBytes.length,
      sourceDocId: room.docId,
      sourceSeq,
    };
  });
}

module.exports = { duplicateDocument, validateTitle, validateRequestedId };
