import { useEffect, useRef } from 'react';
import type { Dispatch } from 'react';
import type { ActivityEvent, Envelope_Meta_ } from '../api/contracts';
import { useApiQuery } from '../api/query';
import type { QueryClient } from '../api/query';
import type { Action, Session } from '../state/session';
import { Empty, QueryContent, Stat } from './Feedback';

const PAGE_SIZE = 25;
const eventLabels: Record<ActivityEvent['event_type'], string> = {
  newly_observed: 'Newly observed', no_longer_observed: 'No longer observed', reappeared: 'Reappeared in source data',
  source_changed: 'Source details changed', company_incorporated: 'Company incorporated',
  company_dissolved: 'Company dissolved', processing_correction: 'Processing correction',
};
const entities: Record<ActivityEvent['entity_type'], string> = {
  establishment: 'Venue record', company: 'Legal company', source: 'Provider record',
};
const fields: Record<string, string> = {
  display_name: 'Venue name', name: 'Source name', legal_name: 'Legal name', eligibility: 'Hospitality eligibility',
  primary_category: 'Primary category', duplicate_status: 'Identity matching', location_quality: 'Location quality',
  observation_state: 'Observation status', legal_status: 'Legal status', company_type: 'Company type',
  sic_codes: 'SIC industry codes', incorporation_date: 'Incorporation date', cessation_date: 'Cessation date',
  retired: 'Record retired', state: 'Record state',
};
const readable = (value: string) => value.replaceAll('_', ' ');
function date(value: string | null, time = false) {
  if (!value || !Number.isFinite(Date.parse(value))) return 'Unavailable';
  const parsed = new Date(value);
  return parsed.toLocaleDateString('en-GB', { timeZone: 'UTC' })
    + (time ? `, ${parsed.toISOString().slice(11, 16)} UTC` : '');
}
function context(value: Record<string, unknown> | null, depth = 0): Record<string, string> {
  if (!value || depth > 2) return {};
  const result: Record<string, string> = {};
  for (const [field, item] of Object.entries(value)) {
    if (field === 'value' && item && typeof item === 'object' && !Array.isArray(item)) {
      Object.assign(result, context(item as Record<string, unknown>, depth + 1));
    } else if (Object.hasOwn(fields, field)) {
      if (item === null) result[field] = 'Unavailable';
      else if (typeof item === 'string') result[field] = readable(item.slice(0, 500));
      else if (typeof item === 'boolean') result[field] = item ? 'Yes' : 'No';
      else if (field === 'sic_codes' && Array.isArray(item)) result[field] = item.filter(value => typeof value === 'string').slice(0, 20).join(', ') || 'Unavailable';
    }
  }
  return result;
}
function EventCard({ event }: { event: ActivityEvent }) {
  const before = context(event.before_value), after = context(event.after_value);
  const changed = [...new Set([...Object.keys(before), ...Object.keys(after)])].filter(field => before[field] !== after[field]);
  const name = after.display_name ?? after.legal_name ?? after.name ?? before.display_name ?? before.legal_name ?? before.name;
  const provider = typeof event.evidence.provider === 'string' ? ({ fsa: 'Food Standards Agency', osm: 'OpenStreetMap',
    'companies-house': 'Companies House' } as Record<string, string>)[event.evidence.provider] ?? event.evidence.provider : null;
  return <article className="activity-event card" aria-label={`${eventLabels[event.event_type]} — ${entities[event.entity_type]}`}>
    <div className="card-heading"><h4>{eventLabels[event.event_type]}</h4><span className="tag">{entities[event.entity_type]}</span></div>
    {name && <p className="event-name">{name}</p>}
    <p className="muted">Observed {date(event.observed_at, true)}{provider ? ` · ${provider}` : ''}
      {event.company_number ? ` · Company ${event.company_number}` : ''}</p>
    {event.source_event_date && <p className="muted">Source-reported {event.entity_type === 'company' ? 'legal ' : ''}event date: {date(event.source_event_date)}. This differs from collection time.</p>}
    {event.event_type === 'no_longer_observed' && <p>Absent from the monitored source records. This is not a verified business closure.</p>}
    {event.event_type === 'newly_observed' && <p>First seen in this monitored history. This is not a verified opening date.</p>}
    {event.event_type === 'reappeared' && <p>Observed again after an absence. This does not prove the business reopened.</p>}
    {event.event_type === 'processing_correction' && <p>A data-processing or review correction; it does not establish a real-world business change.</p>}
    {event.entity_type === 'company' && <p className="muted">Legal company activity does not establish when a venue opened or closed.</p>}
    {event.entity_type === 'source' && <p className="muted">This is a provider-record event; several records may describe one venue. Do not count it as an additional venue.</p>}
    {changed.length > 0 ? <div className="event-table"><table><caption>Published field changes</caption><thead><tr><th scope="col">Field</th><th scope="col">Before</th><th scope="col">After</th></tr></thead>
      <tbody>{changed.map(field => <tr key={field}><th scope="row">{fields[field]}</th><td>{before[field] ?? 'Not supplied'}</td><td>{after[field] ?? 'Not supplied'}</td></tr>)}</tbody></table></div>
      : <p className="muted">No public field comparison is available for this event.</p>}
  </article>;
}

export function Activity({ state, dispatch, client, release }: {
  state: Session; dispatch: Dispatch<Action>; client: QueryClient; release: Envelope_Meta_;
}) {
  const query = useApiQuery(client, 'GET /api/v1/activity', { release_id: release.release_id,
    entity_type: state.activityFilters.entity || null, event_type: state.activityFilters.event || null,
    cursor: state.pagination.activity, limit: PAGE_SIZE });
  const heading = useRef<HTMLHeadingElement>(null);
  const pageFocus = useRef<string | null | undefined>(undefined);
  useEffect(() => {
    if (query.status === 'success' && pageFocus.current !== undefined && pageFocus.current === state.pagination.activity) {
      heading.current?.focus({ preventScroll: true }); pageFocus.current = undefined;
    }
  }, [query.status, state.pagination.activity]);
  return <>
    <p className="activity-intro">Changes observed in accepted monthly collections. Newly observed and missing records are not verified openings or closures.</p>
    <div className="activity-filters"><label className="field">Record type<select value={state.activityFilters.entity}
      onChange={event => dispatch({ type: 'activityFilters', value: { entity: event.target.value as Session['activityFilters']['entity'] } })}>
      <option value="">All record types</option>{Object.entries(entities).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label>
      <label className="field">Change type<select value={state.activityFilters.event}
        onChange={event => dispatch({ type: 'activityFilters', value: { event: event.target.value as Session['activityFilters']['event'] } })}>
        <option value="">All change types</option>{Object.entries(eventLabels).map(([value, label]) => <option key={value} value={value}>{label}</option>)}</select></label></div>
    <QueryContent query={query}>{({ data }) => {
      const filtered = Boolean(state.activityFilters.entity || state.activityFilters.event);
      const noBaseline = !data.baselines.length || data.baselines.some(baseline => baseline.state === 'baseline_unavailable' || !baseline.established_at);
      return <>
        <div className="stats"><Stat label="Observed changes" value={data.total} note="Matching retained events; not a count of businesses" /></div>
        {data.total === 0 ? <Empty title={filtered ? 'No matching events' : noBaseline ? 'History is not ready for a full comparison' : 'No observed changes recorded'}>
          {filtered ? 'No retained events match these filters.' : noBaseline
            ? 'Some starting baselines are missing. Zero recorded events does not establish that nothing changed.'
            : 'Starting baselines are available. No events are recorded in this release; baseline-only history cannot establish a trend.'}</Empty>
          : <section className="activity-list" aria-label="Observed event list"><div className="card-heading"><h3 ref={heading} tabIndex={-1}>Recorded events</h3>
            <span className="list-range">Showing {state.activityHistory.length * PAGE_SIZE + 1}–{state.activityHistory.length * PAGE_SIZE + data.items.length} of {data.total.toLocaleString('en-GB')}</span></div>
            {data.items.map(event => <EventCard key={event.event_id} event={event} />)}
            <nav className="list-pagination" aria-label="Activity pages"><button className="button secondary" disabled={!state.activityHistory.length}
              onClick={() => { pageFocus.current = state.activityHistory.at(-1) ?? null; dispatch({ type: 'activityBack' }); }}>Previous events</button>
              <span>Page {state.activityHistory.length + 1}</span><button className="button secondary" disabled={!data.has_more || !data.next_cursor}
                onClick={() => { if (data.next_cursor) { pageFocus.current = data.next_cursor; dispatch({ type: 'activityNext', cursor: data.next_cursor }); } }}>Next events</button></nav>
          </section>}
      </>;
    }}</QueryContent>
  </>;
}
