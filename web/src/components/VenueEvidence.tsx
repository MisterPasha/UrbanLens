import type { Metadata, VenueDetail } from '../api/contracts';
import { categories } from '../state/session';

const labels: Record<string, string> = {
  observed: 'Observed in source data', no_longer_observed: 'No longer observed in source data',
  withheld: 'Withheld by source', missing: 'Missing', invalid: 'Invalid',
  fsa: 'Food Standards Agency', osm: 'OpenStreetMap', owner_report: 'Owner review',
};
const readable = (value: string) => labels[value] ?? value.replaceAll('_', ' ');
const text = (value: unknown) => typeof value === 'string' && value.trim() ? value : null;
const record = (value: unknown): Record<string, unknown> => value && typeof value === 'object' && !Array.isArray(value)
  ? value as Record<string, unknown> : {};
function date(value: unknown) {
  const raw = text(value);
  return raw && Number.isFinite(Date.parse(raw)) ? new Date(raw).toLocaleDateString('en-GB', { timeZone: 'UTC' }) : 'Unavailable';
}
// Only links to the supported public evidence pages are rendered.
function publicUrl(value: unknown) {
  const raw = text(value);
  if (!raw) return null;
  try {
    const url = new URL(raw);
    if (url.protocol !== 'https:' || url.username || url.password || url.port || url.search || url.hash) return null;
    return (url.hostname === 'ratings.food.gov.uk' && /^\/business\/\d+\/?$/.test(url.pathname))
      || (url.hostname === 'www.openstreetmap.org' && /^\/(node|way|relation)\/\d+\/?$/.test(url.pathname)) ? url.href : null;
  } catch { return null; }
}
const ratingStates: Record<string, string> = {
  awaiting_inspection: 'Awaiting inspection', awaiting_publication: 'Awaiting publication',
  exempt: 'Exempt', missing: 'Rating unavailable', unknown: 'Rating status unknown',
};

export function VenueEvidence({ venue, metadata, offPage }: {
  venue: VenueDetail; metadata: Metadata; offPage: boolean;
}) {
  const withheld = venue.location_quality === 'withheld';
  const category = categories.find(item => item.value === venue.primary_category)?.label ?? 'Category unavailable';
  const fsaHealth = metadata.sources.filter(source => source.provider === 'fsa');
  return <div className="venue-evidence">
    <div aria-live="polite" aria-atomic="true"><h4>{venue.name_unavailable ? 'Name unavailable' : venue.display_name ?? 'Name unavailable'}</h4>
      <p>{withheld ? 'Address withheld by source' : venue.address ?? 'Address unavailable'}
        {!withheld && venue.postcode ? ` · ${venue.postcode}` : ''}</p>
      <p><span className="tag">{category}</span> · Last observed {date(venue.last_observed_at)}</p>
      {offPage && <p className="muted">This matching venue is outside the current list page. Its selection is shown on the map.</p>}
    </div>
    <section aria-label="Food hygiene evidence"><h5>Food hygiene — separate source evidence</h5>
      <p>FSA ratings describe food hygiene at assessment. They are not a competition score, popularity measure or guarantee of current operation.</p>
      {!venue.hygiene.length ? <p>No FSA hygiene information is linked in this release. This does not mean a zero rating.</p>
        : <ul className="evidence-list">{venue.hygiene.map((item, index) => {
          const rating = record(item.rating), assessed = record(rating.assessed_on);
          const numeric = rating.state === 'rated' && typeof rating.score === 'number' && Number.isInteger(rating.score)
            && rating.score >= 0 && rating.score <= 5;
          return <li key={`${item.source_key}:${index}`}><strong>{numeric ? `Food hygiene rating: ${rating.score} / 5`
            : ratingStates[String(rating.state)] ?? 'Rating unavailable'}</strong>
            <dl className="evidence-grid"><div><dt>Rating date</dt><dd>{assessed.state === 'published' ? date(assessed.value) : 'Unavailable'}
              {assessed.state && assessed.state !== 'published' ? ` — ${readable(String(assessed.state))}` : ''}</dd></div>
              <div><dt>Record collected</dt><dd>{date(item.observed_at)}</dd></div></dl>
            {text(item.source_key) && <p className="muted">FSA record {String(item.source_key)}. Its rating belongs to this source record.</p>}
          </li>;
        })}</ul>}
      {fsaHealth.map((health, index) => <p className="muted" key={index}>FSA source freshness: {text(health.status) ? readable(String(health.status)) : 'Unavailable'}
        . Last successful collection: {date(health.last_success_at)}.{health.retained_older_snapshot === true && ' An older accepted snapshot is retained after a later collection problem.'}</p>)}
      {!fsaHealth.length && <p className="muted">FSA source freshness is unavailable in this release.</p>}
    </section>
    <section aria-label="Contributing sources"><h5>Source records</h5>
      {!venue.sources.length ? <p>No public source records are supplied in this release.</p> : <ul className="evidence-list">{venue.sources.map((source, index) => {
        const url = publicUrl(source.url);
        return <li key={`${source.provider}:${source.source_key}:${index}`}><strong>{readable(String(source.provider ?? 'Source unavailable'))}</strong>
          <p>{text(source.source_key) ?? 'Record identifier unavailable'} · Collected {date(source.observed_at)} · {readable(String(source.state ?? 'Status unavailable'))}</p>
          {url ? <a href={url} target="_blank" rel="noreferrer">View public source record</a> : <p className="muted">Public source link unavailable.</p>}</li>;
      })}</ul>}
      <p className="muted">Public source pages may have changed since this snapshot. Several source records may contribute to one accepted venue.</p>
    </section>
  </div>;
}
