import { proxyRequest } from '../server/api-proxy.mjs';

/**
 * @param {import('node:http').IncomingMessage & { query?: Record<string, string | string[]> }} request
 * @param {import('node:http').ServerResponse} response
 */
export default async function handler(request, response) {
  const incoming = new URL(request.url ?? '/', 'https://urbanlens.invalid');
  const endpoint = request.query?.__urbanlens_path ?? incoming.searchParams.get('__urbanlens_path');
  const health = request.query?.__urbanlens_health ?? incoming.searchParams.get('__urbanlens_health');
  incoming.searchParams.delete('__urbanlens_path');
  incoming.searchParams.delete('__urbanlens_health');
  // Vercel also adds the named rewrite capture to the forwarded query string.
  incoming.searchParams.delete('endpoint');
  if (health === '1') incoming.pathname = '/health';
  else if (!incoming.pathname.startsWith('/api/v1/') && typeof endpoint === 'string') {
    incoming.pathname = `/api/v1/${endpoint}`;
  }
  const result = await proxyRequest(new Request(incoming, { method: request.method ?? 'GET' }), {
    API_ORIGIN: process.env.API_ORIGIN,
    SERVICE_ENABLED: process.env.SERVICE_ENABLED,
  });
  response.statusCode = result.status;
  result.headers.forEach((value, key) => response.setHeader(key, value));
  response.end(Buffer.from(await result.arrayBuffer()));
}
