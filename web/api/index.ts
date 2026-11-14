import { proxyRequest } from '../server/api-proxy.ts';

export default {
  fetch(request: Request): Promise<Response> {
    return proxyRequest(request, {
      API_ORIGIN: process.env.API_ORIGIN,
      SERVICE_ENABLED: process.env.SERVICE_ENABLED,
    });
  },
};
