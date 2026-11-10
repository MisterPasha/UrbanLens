import { useCallback, useState } from 'react';
import type { Dispatch } from 'react';
import type { AreaMetrics, Envelope_Meta_ } from '../api/contracts';
import { useApiQuery } from '../api/query';
import type { QueryClient } from '../api/query';
import type { Action, Session } from '../state/session';
import { AreaSelect } from './AreaSelect';
import { AreaMap } from './AreaMap';
import { CategoryMix } from './CategoryMix';
import { Empty, QueryContent } from './Feedback';

const format = (value: number | null, reason?: string | null, precision = 1) => value === null
  ? `Unavailable — ${reason?.replaceAll('_', ' ') || 'not provided in this release'}`
  : new Intl.NumberFormat('en-GB', { maximumFractionDigits: precision }).format(value);
const date = (value: string) => new Date(value).toLocaleDateString('en-GB', { timeZone: 'UTC' });
function Metrics({ area }: { area: AreaMetrics }) {
  return <dl className="area-metrics">
    <div><dt>Matching venues</dt><dd>{format(area.establishment_count)}</dd></div>
    <div><dt>Venues per land km²</dt><dd>{format(area.establishments_per_land_km2, area.land_rate_unavailable_reason)}</dd></div>
    <div><dt>Venues per 1,000 residents</dt><dd>{format(area.establishments_per_1000_residents, area.resident_rate_unavailable_reason)}</dd></div>
    <div><dt>Population</dt><dd>{format(area.population)}</dd></div>
    <div><dt>Official land area (km²)</dt><dd>{format(area.official_land_km2, area.land_rate_unavailable_reason, 3)}</dd></div>
    <div><dt>Residents per land km²</dt><dd>{format(area.population_per_land_km2, area.population_density_unavailable_reason)}</dd></div>
    {([['0–15', area.age_0_15, area.age_0_15_percent], ['16–64', area.age_16_64, area.age_16_64_percent],
      ['65+', area.age_65_plus, area.age_65_plus_percent]] as const).map(([label, count, percent]) =>
      <div key={label}><dt>Residents aged {label}</dt><dd>{format(count, area.age_mix_unavailable_reason)}
        {percent === null ? ` · Share ${format(null, area.age_mix_unavailable_reason)}` : ` · ${format(percent)}%`}</dd></div>)}
  </dl>;
}
function AreaCard({ area, state, published }: { area: AreaMetrics; state: Session; published: string }) {
  return <section className="card area-card"><span className="eyebrow">{area.area_code ?? 'CITY BENCHMARK'}</span><h3>{area.area_name}</h3>
    <p className="muted">Venue snapshot published {date(published)} · Population reference {date(area.population_reference_date)}</p>
    <Metrics area={area} /><CategoryMix mix={area.category_mix} total={area.establishment_count} selected={state.categories} />
  </section>;
}

export function Compare({ state, dispatch, client, release }: {
  state: Session; dispatch: Dispatch<Action>; client: QueryClient; release: Envelope_Meta_;
}) {
  const [slot, setSlot] = useState<0 | 1>(0);
  const areas = useApiQuery(client, 'GET /api/v1/areas', { release_id: release.release_id });
  const selected = state.comparison.filter((code): code is string => code !== null);
  const ready = selected.length === 2 && selected[0] !== selected[1];
  const query = useApiQuery(client, 'GET /api/v1/compare', ready ? {
    release_id: release.release_id, categories: state.categories, area_codes: selected,
  } : null);
  const single = useApiQuery(client, 'GET /api/v1/areas/{code}/summary', !ready && selected.length === 1 ? {
    release_id: release.release_id, categories: state.categories, code: selected[0],
  } : null);
  const choose = useCallback((code: string) => {
    if (state.comparison[slot === 0 ? 1 : 0] === code) return;
    dispatch({ type: 'compare', slot, area: code });
    if (slot === 0 && !state.comparison[1]) setSlot(1);
  }, [dispatch, slot, state.comparison]);
  const consistency = (values: AreaMetrics[]) => values.every(area => area.geography_version === release.metadata.geography_version
    && area.population_edition === release.metadata.population_edition && area.population_reference_date === release.metadata.population_reference_date);
  return <>
    <section className="card"><h3>Find your two neighbourhoods</h3>
      <p>LSOAs (Lower Layer Super Output Areas) are small official statistical areas. Choose them by location on the map;
        you do not need to know their official names.</p>
      <QueryContent query={areas}>{result => <>
        <div className="area-pickers">{([0, 1] as const).map(index => <AreaSelect key={index}
          label={index === 0 ? 'First neighbourhood' : 'Second neighbourhood'} value={state.comparison[index]}
          excluded={state.comparison[index === 0 ? 1 : 0]} areas={result.data}
          onChange={area => { dispatch({ type: 'compare', slot: index, area }); setSlot(index === 0 && area && !state.comparison[1] ? 1 : index); }} />)}</div>
        <fieldset className="map-slot"><legend>Clicking the map chooses</legend>{([0, 1] as const).map(index =>
          <label key={index}><input type="radio" name="map-slot" checked={slot === index} onChange={() => setSlot(index)} />
            {index === 0 ? 'First neighbourhood' : 'Second neighbourhood'}</label>)}</fieldset>
        <p className="muted">Choose two distinct areas. An area already selected in the other slot cannot be selected again.</p>
        <AreaMap areas={result.data} selected={state.comparison} onSelect={choose} />
      </>}</QueryContent>
    </section>
    <p className="coverage-note">Counts include eligible, resolved venues with precise published locations. Approximate or missing locations and uncertain matches are excluded.
      Rates describe venue concentration relative to land or residents. Residents are not a measure of customers, demand, footfall or business success.</p>
    {!ready ? <>{selected.length === 1 ? <QueryContent query={single}>{({ data, metadata }) => data.area.area_code === selected[0] && consistency([data.area, data.city_benchmark])
      ? <div className="comparison-cards single-preview"><AreaCard area={data.area} state={state} published={metadata.published_at} /></div>
      : <p role="alert">Area editions do not match this release. Reload the published data.</p>}</QueryContent> : null}
      <Empty title="Two places, one consistent comparison">{selected.length === 1 ? 'This area’s data is shown above. Choose a second area to compare.'
        : 'Your venue categories apply to both areas and the city benchmark.'}</Empty></>
      : <QueryContent query={query}>{({ data, metadata }) => {
        const ordered = selected.map(code => data.areas.find(area => area.area_code === code));
        if (data.areas.length !== 2 || ordered.some(area => !area) || !consistency([...data.areas, data.city_benchmark])) return <p role="alert">Comparison areas or data editions do not match this selection. Reload the published data.</p>;
        return <><div className="comparison-cards">{ordered.map(area => <AreaCard key={area!.area_code} area={area!} state={state} published={metadata.published_at} />)}</div>
          <div className="city-comparison"><AreaCard area={data.city_benchmark} state={state} published={metadata.published_at} /></div>
          <p className="muted">The city benchmark uses the same categories and release, with total city population and land denominators; it is not an average of neighbourhood rates.
            Venue observations and resident estimates refer to different dates. Population is not changed by venue category filters.</p></>;
      }}</QueryContent>}
  </>;
}
