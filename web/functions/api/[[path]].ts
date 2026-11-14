import { proxyRequest } from '../../server/api-proxy.mjs';

interface ProxyEnvironment { API_ORIGIN?: string; SERVICE_ENABLED?: string }

export function onRequestGet({ request, env }: { request: Request; env: ProxyEnvironment }): Promise<Response> {
  return proxyRequest(request, env);
}
