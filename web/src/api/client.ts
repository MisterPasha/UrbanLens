import type { ApiRoutes, ErrorResponse } from './contracts';

export class ApiError extends Error {
  constructor(public readonly status: number, public readonly body: ErrorResponse) {
    super(body.error.message);
  }
}

// Server responses are validated by Pydantic. Query/path types come from the same OpenAPI.
export async function apiGet<R extends keyof ApiRoutes>(
  route: R,
  params: ApiRoutes[R]['params'],
  signal?: AbortSignal,
): Promise<ApiRoutes[R]['response']> {
  let path = route.slice(4);
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined) continue;
    if (path.includes(`{${key}}`)) path = path.replace(`{${key}}`, encodeURIComponent(String(value)));
    else if (Array.isArray(value)) {
      if (key === 'categories' && value.length === 0) search.set('empty_categories', 'true');
      else value.forEach((item: unknown) => search.append(key, String(item)));
    }
    else search.append(key, String(value));
  }
  const response = await fetch(`${path}?${search}`, { signal, headers: { Accept: 'application/json' } });
  const body: unknown = await response.json();
  if (!response.ok) throw new ApiError(response.status, body as ErrorResponse);
  return body as ApiRoutes[R]['response'];
}
