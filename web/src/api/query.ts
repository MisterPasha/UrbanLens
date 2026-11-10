import { useCallback, useEffect, useSyncExternalStore } from 'react';
import { apiGet, ApiError } from './client';
import type { ApiRoutes } from './contracts';

type Snapshot<T> = { status: 'loading'; data?: never; error?: never }
  | { status: 'success'; data: T; error?: never }
  | { status: 'error'; data?: never; error: Error };
interface Entry {
  snapshot: Snapshot<unknown>;
  listeners: Set<() => void>;
  controller?: AbortController;
}
const loading = { status: 'loading' } as const;
export type QueryResult<T> = Snapshot<T> & { retry: () => void };

// Memory-only, bounded cache. Every analytical key includes its pinned release.
// A changed key immediately reads a different snapshot, even before effects run.
export class QueryClient {
  private entries = new Map<string, Entry>();
  constructor(private readonly expired: (releaseId: string) => void) {}

  entry(key: string): Entry {
    let entry = this.entries.get(key);
    if (!entry) {
      for (const [oldKey, old] of this.entries) {
        if (this.entries.size < 96) break;
        if (!old.listeners.size) { old.controller?.abort(); this.entries.delete(oldKey); }
      }
      entry = { snapshot: loading, listeners: new Set() };
      this.entries.set(key, entry);
    }
    return entry;
  }

  subscribe(key: string, listener: () => void): () => void {
    const entry = this.entry(key);
    entry.listeners.add(listener);
    return () => {
      entry.listeners.delete(listener);
      if (!entry.listeners.size && entry.controller) {
        entry.controller.abort();
        entry.controller = undefined;
        entry.snapshot = loading;
      }
    };
  }

  async load(key: string, retry = false): Promise<void> {
    const entry = this.entry(key);
    if (entry.controller || (!retry && entry.snapshot.status !== 'loading')) return;
    const controller = new AbortController();
    entry.controller = controller;
    entry.snapshot = loading;
    entry.listeners.forEach(listener => listener());
    const [route, params] = JSON.parse(key) as [keyof ApiRoutes, ApiRoutes[keyof ApiRoutes]['params']];
    const timer = window.setTimeout(() => controller.abort('timeout'), 15_000);
    try {
      const response = await apiGet(route, params, controller.signal);
      if (controller.signal.aborted || entry.controller !== controller) return;
      if (!('release_id' in response) || response.release_id !== params.release_id) {
        throw new Error('The data version did not match. Please try again.');
      }
      entry.snapshot = { status: 'success', data: response };
    } catch (error) {
      if (entry.controller !== controller) return;
      if (controller.signal.aborted && controller.signal.reason !== 'timeout') return;
      entry.snapshot = { status: 'error', error: controller.signal.reason === 'timeout'
        ? new Error('The request took too long. Please try again.')
        : error instanceof Error ? error : new Error('Data is temporarily unavailable.') };
      if (error instanceof ApiError && error.status === 410 && typeof params.release_id === 'string') {
        this.expired(params.release_id);
      }
    } finally {
      window.clearTimeout(timer);
      if (entry.controller === controller) {
        entry.controller = undefined;
        entry.listeners.forEach(listener => listener());
      }
    }
  }

  clear(): void {
    for (const entry of this.entries.values()) { entry.controller?.abort(); entry.controller = undefined; }
    this.entries.clear();
  }
}

export function useApiQuery<R extends keyof ApiRoutes>(
  client: QueryClient, route: R, params: (ApiRoutes[R]['params'] & { release_id: string }) | null,
): QueryResult<ApiRoutes[R]['response']> {
  const key = params === null ? null : JSON.stringify([route,
    Object.fromEntries(Object.entries(params).map(([key, value]) => [key,
      key === 'categories' && Array.isArray(value) ? [...value].sort() : value])
      .sort(([a], [b]) => String(a).localeCompare(String(b))))]);
  const subscribe = useCallback((listener: () => void) => key === null
    ? () => {} : client.subscribe(key, listener), [client, key]);
  const snapshot = useSyncExternalStore(subscribe,
    useCallback(() => key === null ? loading : client.entry(key).snapshot, [client, key]));
  useEffect(() => { if (key !== null) void client.load(key); }, [client, key]);
  return { ...snapshot, retry: () => { if (key !== null) void client.load(key, true); } } as
    QueryResult<ApiRoutes[R]['response']>;
}
