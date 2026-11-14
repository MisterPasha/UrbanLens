import assert from 'node:assert/strict';
import { test } from 'node:test';
import process from 'node:process';
import { URL } from 'node:url';
import { onRequestGet } from '../functions/api/[[path]].ts';
import handler from '../api/proxy.mjs';
const { Request, Response, Headers } = globalThis;

function vercelResponse(request, rewrittenQuery = {}) {
  const headers = new Headers();
  return new Promise((resolve, reject) => {
    const response = {
      statusCode: 200,
      setHeader: (key, value) => headers.set(key, value),
      end: body => resolve(new Response(body, { status: response.statusCode, headers })),
    };
    handler({ url: request.url, method: request.method, query: rewrittenQuery }, response).catch(reject);
  });
}

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

test('Vercel adapter uses server settings, allows only reads, and forwards health', async () => {
  const original = globalThis.fetch;
  const saved = { API_ORIGIN: process.env.API_ORIGIN, SERVICE_ENABLED: process.env.SERVICE_ENABLED };
  let calls = 0;
  globalThis.fetch = async (url, init) => {
    calls++;
    assert.equal(init.redirect, 'error');
    assert.deepEqual(init.headers, { Accept: 'application/json' });
    assert.equal(new URL(url).origin, 'https://abc123.execute-api.eu-west-2.amazonaws.com');
    if (new URL(url).pathname === '/health') return Response.json({ status: 'ok', service: 'urbanlens-api' });
    assert.equal(new URL(url).pathname, '/api/v1/meta');
    return Response.json({ release_id: 'published' });
  };
  try {
    process.env.API_ORIGIN = 'https://abc123.execute-api.eu-west-2.amazonaws.com';
    process.env.SERVICE_ENABLED = 'false';
    const request = new Request('https://urbanlens.vercel.app/api/v1/meta');
    assert.equal((await vercelResponse(request)).status, 503);
    assert.equal(calls, 0);
    process.env.SERVICE_ENABLED = 'true';
    assert.equal((await vercelResponse(new Request(request.url, { method: 'POST' }))).status, 405);
    assert.equal(calls, 0);
    const response = await vercelResponse(new Request('https://urbanlens.vercel.app/api/proxy?__urbanlens_path=meta'));
    assert.equal(response.status, 200);
    assert.equal(response.headers.get('Cache-Control'), 'no-store');
    assert.equal((await response.json()).release_id, 'published');
    assert.equal((await vercelResponse(new Request('https://urbanlens.vercel.app/health'), { __urbanlens_health: '1' })).status, 200);
    assert.equal(calls, 2);
  } finally {
    globalThis.fetch = original;
    for (const [key, value] of Object.entries(saved)) {
      if (value === undefined) delete process.env[key]; else process.env[key] = value;
    }
  }
});
