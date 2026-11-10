import type { Envelope_Meta_ } from '../api/contracts';

const providers: Record<string, string> = { fsa: 'Food Standards Agency', osm: 'OpenStreetMap',
  'companies-house': 'Companies House', ons: 'ONS', 'postcodes-io': 'Postcodes.io' };
const dimensions: Record<string, string> = { accepted_source_mix: 'Source contribution', category_detail: 'Category detail',
  coordinate_quality: 'Location quality', duplicates: 'Identity matching', eligibility: 'Hospitality eligibility',
  verified_company_link: 'Verified company links', primary_spatial_inclusion: 'Inclusion in precise counts', exclusion_reason: 'Exclusion reasons' };
const labels: Record<string, string> = { source_point: 'Source-provided point', point_on_surface: 'Point within premises outline',
  postcode_fallback: 'Approximate postcode', fsa_only: 'FSA only', osm_only: 'OSM only', 'fsa+osm': 'FSA and OSM',
  owner_report_only: 'Owner report only', broad: 'Broader category', specific: 'Specific category', unavailable: 'Unavailable',
  unresolved: 'Unresolved', eligible: 'Eligible', excluded: 'Excluded', resolved: 'Resolved', included: 'Included',
  linked: 'Verified link', unlinked: 'No verified link', missing: 'Missing', withheld: 'Withheld', invalid: 'Invalid' };
const readable = (value: string) => labels[value] ?? value.replaceAll('_', ' ');
const finite = (value: unknown): value is number => typeof value === 'number' && Number.isFinite(value) && value >= 0;
const count = (value: unknown) => finite(value) ? value.toLocaleString('en-GB') : 'Unavailable';
const hiddenCoverageGroups = new Set(['current canonical roster', 'primary spatial venues', 'non-excluded venue records']);
function date(value: unknown, time = false) {
  if (typeof value !== 'string' || !Number.isFinite(Date.parse(value))) return 'Not supplied';
  const parsed = new Date(value);
  return parsed.toLocaleDateString('en-GB', { timeZone: 'UTC' }) + (time ? `, ${parsed.toISOString().slice(11, 16)} UTC` : '');
}
export function Data({ release }: { release: Envelope_Meta_ }) {
  const groups = new Map<string, Map<string, Record<string, unknown>[]>>();
  for (const row of release.coverage) {
    if (typeof row.universe !== 'string' || hiddenCoverageGroups.has(row.universe)
      || typeof row.dimension !== 'string' || !Object.hasOwn(dimensions, row.dimension)) continue;
    if (!groups.has(row.universe)) groups.set(row.universe, new Map());
    const group = groups.get(row.universe)!;
    if (!group.has(row.dimension)) group.set(row.dimension, []);
    group.get(row.dimension)!.push(row);
  }
  return <>
    <section className="card methodology-section"><h3>Freshness and reference dates</h3>
      <p>Sources are collected or checked monthly. Publication time is separate from collection time and the period described by population estimates.</p>
      <dl className="details"><div><dt>Published release</dt><dd>{date(release.metadata.published_at, true)}</dd></div>
        <div><dt>Population reference</dt><dd>{date(release.metadata.population_reference_date)}</dd></div>
        <div><dt>Area geography</dt><dd>{release.metadata.geography_version}</dd></div></dl>
      <div className="data-table-scroll" role="region" aria-label="Source freshness table" tabIndex={0}><table className="data-table"><caption>Source collection, edition and health</caption>
        <thead><tr><th scope="col">Source</th><th scope="col">Last successful collection</th><th scope="col">Edition / extract</th><th scope="col">Health</th></tr></thead>
        <tbody>{release.metadata.sources.map((source, index) => <tr key={`${source.provider}:${index}`}>
          <th scope="row">{providers[String(source.provider)] ?? 'Source not identified'}
            {source.provider === 'companies-house' && <small>{String(source.scope_key).endsWith('-profiles') ? 'Tracked profiles' : 'Registered-office snapshot'}</small>}</th>
          <td>{date(source.last_success_at, true)}</td><td>{typeof source.source_edition === 'string' ? source.source_edition : 'Edition not supplied'}
            <small>Extract: {date(source.source_extract_date)}</small><small>Provider publication: {date(source.source_published_at)}</small></td>
          <td>{typeof source.status === 'string' ? readable(source.status) : 'Unavailable'}
            {source.retained_older_snapshot === true && <small>Older accepted snapshot retained after a later collection problem.</small>}
            <small>Next scheduled check: {date(source.next_due_at, true)}</small></td>
        </tr>)}</tbody></table></div>
      {!release.metadata.sources.length && <p>Source health and collection dates are unavailable in this release.</p>}
      <p className="muted">A newer release does not make older observations newer. Reference datasets can remain on the same edition after a successful monthly check.</p>
    </section>
    <section className="card methodology-section"><h3>Measured coverage</h3>
      <p>These are release-wide measurements, independent of your selected categories or search circle. They describe accepted source coverage, not a complete census of businesses.</p>
      {!groups.size ? <p>Coverage measurements are unavailable in this release; this does not mean zero coverage.</p>
        : [...groups].map(([universe, group]) => <details className="coverage-group" key={universe}>
          <summary>{readable(universe)}</summary><div className="coverage-grid">{[...group].map(([dimension, rows]) =>
            <div key={dimension}><table className="data-table"><caption>{dimensions[dimension]}</caption>
              <thead><tr><th scope="col">Status</th><th scope="col">Records</th><th scope="col">Share</th></tr></thead>
              <tbody>{rows.map((row, index) => <tr key={index}><th scope="row">{typeof row.bucket === 'string' ? readable(row.bucket) : 'Unavailable'}</th>
                <td>{count(row.n)}</td><td>{finite(row.denominator) && row.denominator > 0 && finite(row.percent)
                  ? `${row.percent.toFixed(1)}%` : 'Unavailable'}</td></tr>)}</tbody></table>
              {rows.some(row => row.reasons_overlap === true) && <p className="muted">Reasons overlap; one record can appear more than once.</p>}</div>)}</div>
        </details>)}
      <p className="muted">Each table has its own counting population and denominator. Do not add counts across dimensions or combine the populations. Unresolved records and missing locations are not invented radius counts.</p>
    </section>
    <section className="card methodology-section"><h3>How to read the figures</h3><dl className="method-definitions">
      <div><dt>Counting unit</dt><dd>One resolved hospitality venue, with one primary category. A pub and a separately identified kitchen can remain distinct businesses at the same address.</dd></div>
      <div><dt>Precise venue counts</dt><dd>Only eligible, resolved venues with supported precise locations enter maps and area/radius counts. Approximate postcode points, missing/withheld locations and uncertain matches are excluded.</dd></div>
      <div><dt>Categories</dt><dd>Other food service is a broader category where sources do not support finer detail. Schools, care premises and retailers outside the hospitality scope are excluded.</dd></div>
      <div><dt>Neighbourhoods and residents</dt><dd>LSOAs are 2021 statistical areas, not familiar neighbourhood borders. Rates use official land area or resident population; city benchmarks use city totals. Missing denominators stay unavailable. No population or age estimates are invented for search circles.</dd></div>
      <div><dt>Companies and history</dt><dd>A registered office is not proof of a trading premises. Verified operators require reviewed links. New or missing source records do not prove openings or closures; company legal dates and processing corrections are separate.</dd></div>
      <div><dt>Interpretation</dt><dd>Concentration and resident context do not measure footfall, demand or business success. Zero means no accepted matching records in valid coverage; unavailable means the necessary evidence is missing.</dd></div>
    </dl></section>
    <section className="card methodology-section"><h3>Source limitations</h3><ul className="source-limitations">
      <li><strong>FSA:</strong> food-registration and hygiene records can lag real-world changes; ratings describe assessment dates.</li>
      <li><strong>OpenStreetMap:</strong> community coverage and detail vary; missing tags are not proof a venue does not exist.</li>
      <li><strong>Companies House:</strong> self-reported legal/industry information and office geography do not establish local operation or ownership.</li>
      <li><strong>ONS:</strong> population estimates describe a reference period and statistical geography, not current visitors.</li>
      <li><strong>Postcodes.io:</strong> postcode centres are approximate, and responses may not identify the exact underlying dataset edition.</li>
    </ul></section>
    <section className="card methodology-section"><h3>Source attribution</h3>
      <ul className="attribution">{release.metadata.attribution.map(item => <li key={item}>{item}</li>)}</ul>
      <p>Contains public sector information licensed under the <a href="https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/">Open Government Licence v3.0</a>.</p>
      <ul className="attribution"><li><a href="https://ratings.food.gov.uk/terms-and-conditions">Food Standards Agency terms</a></li>
        <li>© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap contributors</a> · <a href="https://opendatacommons.org/licenses/odbl/1-0/">ODbL 1.0</a></li>
        <li>Source: Office for National Statistics licensed under the Open Government Licence v.3.0. Contains OS data © Crown copyright and database right 2026.</li>
        <li>Area measurements have been created by the Office for National Statistics. Crown Copyright. All rights reserved 2022. <a href="https://www.ons.gov.uk/methodology/geography/licences">ONS geographic licences</a></li>
        <li><a href="https://www.gov.uk/government/publications/companies-house-accreditation-to-information-fair-traders-scheme/public-task-copyright-and-crown-copyright">Companies House reuse terms</a></li>
        <li>Postcodes.io: Contains Ordnance Survey data © Crown copyright and database right 2025. Contains Royal Mail data © Royal Mail copyright and database right 2025.
          Contains National Statistics data © Crown copyright and database right 2025. Contains NRS data © Crown copyright and database right 2025. <a href="https://postcodes.io/docs/licences/">Postcode licences</a></li></ul>
      <p><a href="/data-reuse/index.html">Data reuse and downloadable-data status</a>. Source rights apply separately from application code licensing.</p>
    </section>
  </>;
}
