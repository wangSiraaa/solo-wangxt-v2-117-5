#!/usr/bin/env node
'use strict';

// Demo client "copy": asks the gateway to copy the current document into a
// new document (POST /v1/docs/:docId/copy), then connects to the copy over
// WebSocket and prints its structural state hash — cross-checked against
// the copy response and the source document's recovered-state hash.

const http = require('node:http');
const { DocClient } = require('./lib-client');

const BASE = process.env.BASE_URL || 'http://127.0.0.1:7777';
const WS_URL = process.env.WS_URL || 'ws://127.0.0.1:7777/ws';
const SRC_DOC = process.env.DOC_ID || 'doc-demo';
const TOKEN = process.env.TOKEN || 'user-owner';
const TITLE = process.env.TITLE || `Drill copy of ${SRC_DOC} (${new Date().toISOString()})`;

function postJson(path, token, body) {
  return new Promise((resolve, reject) => {
    const data = JSON.stringify(body);
    const req = http.request(`${BASE}${path}`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-auth-token': token,
        'content-length': Buffer.byteLength(data),
      },
    }, (res) => {
      let b = '';
      res.on('data', (d) => { b += d; });
      res.on('end', () => resolve({ status: res.statusCode, body: b }));
    });
    req.on('error', reject);
    req.write(data);
    req.end();
  });
}

function getJson(path, token) {
  return new Promise((resolve, reject) => {
    const req = http.get(`${BASE}${path}`, {
      headers: { 'x-auth-token': token },
    }, (res) => {
      let b = '';
      res.on('data', (d) => { b += d; });
      res.on('end', () => resolve({ status: res.statusCode, body: b }));
    });
    req.on('error', reject);
  });
}

async function main() {
  console.log(`copying ${SRC_DOC} as "${TITLE}" (initiator ${TOKEN}) ...`);
  const copy = await postJson(`/v1/docs/${SRC_DOC}/copy`, TOKEN, { title: TITLE });
  if (copy.status !== 200) {
    console.error(`copy rejected: HTTP ${copy.status} ${copy.body}`);
    process.exit(1);
  }
  const { docId, stateHash } = JSON.parse(copy.body);
  console.log(`copy created: docId=${docId}`);
  console.log(`copy stateHash (from copy response):   ${stateHash}`);

  const src = JSON.parse((await getJson(`/v1/docs/${SRC_DOC}/recovered-state`, TOKEN)).body);
  console.log(`source stateHash (PostgreSQL rebuild): ${src.stateHash}`);
  console.log(`source == copy at copy time:           ${src.stateHash === stateHash}`);

  // Connect to the copy like any client would: the room is rebuilt from
  // the initial snapshot written by the copy endpoint.
  const c = new DocClient({ url: WS_URL, token: TOKEN, docId, name: 'copy', verbose: true });
  await c.connect();
  console.log(`connected to copy: role=${c.role} seq=${c.seq}`);
  console.log(`client-side stateHash of the copy:     ${c.stateHash()}`);
  console.log(`client matches copy snapshot:          ${c.stateHash() === stateHash}`);
  console.log(`copy text: ${JSON.stringify(c.text)}`);
  c.close();
  process.exit(0);
}

main().catch((e) => { console.error('copy demo failed:', e); process.exit(1); });
