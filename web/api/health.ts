import proxy from './[...path].ts';

export default {
  fetch(request: Request): Promise<Response> {
    return proxy.fetch(new Request(new URL('/health', request.url), request));
  },
};
