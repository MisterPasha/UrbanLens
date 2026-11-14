import { proxyRequest, type ProxyEnvironment } from '../../server/api-proxy.ts';

export function onRequestGet({ request, env }: { request: Request; env: ProxyEnvironment }): Promise<Response> {
  return proxyRequest(request, env);
}
