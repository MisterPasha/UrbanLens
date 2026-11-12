import assert from 'node:assert/strict';
import { test } from 'node:test';
import { onRequestGet } from '../functions/api/[[path]].ts';
const { Request, Response } = globalThis;

test('service cutoff and invalid origins never forward paid API requests', async () => {
  const original = globalThis.fetch;
  let calls = 0;
  globalThis.fetch = async () => { calls++; throw new Error('unexpected'); };
  try {
    for (const env of [{}, { SERVICE_ENABLED: 'true', API_ORIGIN: 'https://example.com' }]) {
      const response = await onRequestGet({ request: new Request('https://urbanlens.example/api/v1/meta'), env });
      assert.equal(response.status, 503);
      assert.equal((await response.json()).error.code, 'service_limited');
    }
    assert.equal(calls, 0);
  } finally { globalThis.fetch = original; }
});

test('proxy forwards queries without browser credentials and preserves errors', async () => {
  const original = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    assert.equal(String(url), 'https://abc123.execute-api.eu-west-2.amazonaws.com/api/v1/areas?release_id=test');
    assert.deepEqual(init.headers, { Accept: 'application/json' });
    return Response.json({ error: { code: 'data_unavailable' } }, { status: 503 });
  };
  try {
    const response = await onRequestGet({ request: new Request('https://urbanlens.example/api/v1/areas?release_id=test', { headers: { Cookie: 'private' } }),
      env: { SERVICE_ENABLED: 'true', API_ORIGIN: 'https://abc123.execute-api.eu-west-2.amazonaws.com' } });
    assert.equal(response.status, 503);
    assert.equal(response.headers.get('Cache-Control'), 'no-store');
  } finally { globalThis.fetch = original; }
});
