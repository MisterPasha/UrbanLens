import type { ReactNode } from 'react';
import { ApiError } from '../api/client';
import type { QueryResult } from '../api/query';

export function QueryContent<T>({ query, children }: {
  query: QueryResult<T>; children: (data: T) => ReactNode;
}) {
  if (query.status === 'loading') return <Loading label="Loading this view…" />;
  if (query.status === 'error') return <Unavailable error={query.error} retry={query.retry} />;
  return <>{children(query.data)}</>;
}

export function Stat({ label, value, note }: { label: string; value: number | null; note: string }) {
  return <div className="stat"><span>{label}</span><strong>{value === null ? 'Unavailable'
    : new Intl.NumberFormat('en-GB', { maximumFractionDigits: 1 }).format(value)}</strong><small>{note}</small></div>;
}

export function Loading({ label = 'Loading the latest published data…' }: { label?: string }) {
  return <div className="feedback loading" role="status"><span className="spinner" aria-hidden="true" />{label}</div>;
}

export function Empty({ title, children }: { title: string; children: ReactNode }) {
  return <div className="feedback empty" role="status"><h3>{title}</h3><p>{children}</p></div>;
}

export function Notice({ title, children, kind = 'stale' }: {
  title: string; children: ReactNode; kind?: 'stale' | 'limited';
}) {
  return <aside className={`notice ${kind}`} role="status"><strong>{title}</strong><div>{children}</div></aside>;
}

export function Unavailable({ error, retry }: { error: Error; retry: () => void }) {
  const limited = error instanceof ApiError && error.status === 429;
  const serviceLimited = error instanceof ApiError && ['service_limited', 'cost_limit', 'quota_exceeded'].includes(error.body.error.code);
  const timeout = error instanceof ApiError ? error.body.error.code === 'query_timeout' : error.message.includes('request took too long');
  return <div className="feedback unavailable" role="alert">
    <h3>{serviceLimited ? 'Service temporarily limited' : timeout ? 'Request timed out' : limited ? 'Service is busy' : 'Data is temporarily unavailable'}</h3>
    <p>{serviceLimited ? 'A service limit has paused this request. Try again after service is restored; unavailable data does not mean zero results.'
      : timeout ? 'The request took too long. Your selections are kept; you can try again.' : limited ? 'Please wait a moment before trying again.'
      : error instanceof ApiError ? error.message : 'We could not load this view. Your selections are still here.'}</p>
    <button className="button secondary" onClick={retry}>Try again</button>
  </div>;
}
