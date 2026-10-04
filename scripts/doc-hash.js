#!/usr/bin/env node
'use strict';

// Connect to a document and print its structural state hash. Works for any
// document the token can access — including copies created via
// POST /v1/docs/:docId/duplicate.
//
//   DOC_ID=doc-abc123 TOKEN=user-owner npm run demo:hash

const { DocClient } = require('./lib-client');

const URL = process.env.WS_URL || 'ws://127.0.0.1:7777/ws';
const DOC = process.env.DOC_ID || 'doc-demo';
const TOKEN = process.env.TOKEN || 'user-owner';

async function main() {
  const c = new DocClient({ url: URL, token: TOKEN, docId: DOC, name: 'hash' });
  await c.connect();
  console.log(`doc=${DOC} role=${c.role} seq=${c.seq}`);
  console.log(`text=${JSON.stringify(c.text)}`);
  console.log(`stateHash=${c.stateHash()}`);
  c.close();
  process.exit(0);
}

main().catch((e) => { console.error('failed:', e.message); process.exit(1); });
