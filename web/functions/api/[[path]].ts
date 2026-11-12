interface Context {
  request: Request;
  env: { API_ORIGIN?: string; SERVICE_ENABLED?: string };
}

const unavailable = () => Response.json({ error: { code: 'service_limited',
  message: 'The service is temporarily limited. Please try again later.', request_id: '' } },
{ status: 503, headers: { 'Cache-Control': 'no-store' } });

export async function onRequestGet({ request, env }: Context): Promise<Response> {
  if (env.SERVICE_ENABLED !== 'true' || !env.API_ORIGIN) return unavailable();
  let origin: URL;
  try { origin = new URL(env.API_ORIGIN); } catch { return unavailable(); }
  if (origin.protocol !== 'https:' || !/^[a-z0-9]+\.execute-api\.eu-west-2\.amazonaws\.com$/.test(origin.hostname)
    || origin.port || origin.username || origin.password || origin.pathname !== '/' || origin.search || origin.hash) return unavailable();
  const incoming = new URL(request.url);
  if (!incoming.pathname.startsWith('/api/v1/') || incoming.search.length > 4096) return unavailable();
  try {
    const response = await fetch(new URL(incoming.pathname + incoming.search, origin), {
      headers: { Accept: 'application/json' }, redirect: 'error', signal: AbortSignal.timeout(12000),
    });
    return new Response(response.body, { status: response.status, headers: {
      'Content-Type': 'application/json', 'Cache-Control': 'no-store',
      ...(response.headers.has('Retry-After') ? { 'Retry-After': response.headers.get('Retry-After')! } : {}),
    } });
  } catch { return unavailable(); }
}
