import { useEffect, useRef, useState } from 'react';
import type { Dispatch, SubmitEvent } from 'react';
import type { CompanyDetail, Envelope_Meta_ } from '../api/contracts';
import { useApiQuery } from '../api/query';
import type { QueryClient } from '../api/query';
import type { Action, Session } from '../state/session';
import { Empty, QueryContent, Stat } from './Feedback';
import { VenueEvidence } from './VenueEvidence';

const PAGE_SIZE = 25;
const readable = (value: string) => value.replaceAll('_', ' ');
const date = (value: string | null) => value && Number.isFinite(Date.parse(value))
  ? new Date(value).toLocaleDateString('en-GB', { timeZone: 'UTC' }) : 'Unavailable';
function officialLink(value: string) {
  try {
    const url = new URL(value);
    return url.protocol === 'https:' && url.hostname === 'find-and-update.company-information.service.gov.uk'
      && !url.username && !url.password && !url.port && !url.search && !url.hash
      && /^\/company\/[A-Z0-9]{8}\/?$/.test(url.pathname) ? url.href : null;
  } catch { return null; }
}
function NameSearch({ value, onApply }: { value: string; onApply: (value: string) => void }) {
  const [draft, setDraft] = useState(value);
  function submit(event: SubmitEvent<HTMLFormElement>) { event.preventDefault(); onApply(draft.trim()); }
  return <form onSubmit={submit} className="company-search"><label className="field">Company name
    <input type="search" value={draft} minLength={2} maxLength={80} onChange={event => setDraft(event.target.value)} placeholder="Search legal names" /></label>
    <button className="button secondary">Search companies</button>
    {value && <button type="button" className="text-button" onClick={() => { setDraft(''); onApply(''); }}>Clear company search</button>}</form>;
}

function CompanyCard({ company, releaseId, client }: { company: CompanyDetail; releaseId: string; client: QueryClient }) {
  const [venueId, setVenueId] = useState<string | null>(null);
  const venue = useApiQuery(client, 'GET /api/v1/establishments/{establishment_id}', venueId ? {
    release_id: releaseId, establishment_id: venueId,
  } : null);
  const office = company.registered_office;
  const published = office?.address_state === 'published';
  const address = published && typeof office?.address === 'string' && office.address.trim() ? office.address : null;
  const postcode = published && typeof office?.postcode === 'string' ? office.postcode : published ? company.registered_office_postcode : null;
  const url = officialLink(company.url);
  return <>
    <h4>{company.legal_name}</h4><p className="muted">Company number <strong>{company.company_number}</strong></p>
    <dl className="evidence-grid company-facts"><div><dt>Legal status</dt><dd>{readable(company.legal_status)}</dd></div>
      <div><dt>Company type</dt><dd>{company.company_type === 'ltd' ? 'Private limited company' : readable(company.company_type)}</dd></div>
      <div><dt>Incorporated</dt><dd>{date(company.incorporation_date)}</dd></div>
      <div><dt>Ceased / dissolved</dt><dd>{company.cessation_date ? date(company.cessation_date) : 'No cessation date supplied'}</dd></div>
      <div><dt>Reported SIC industry codes</dt><dd>{company.sic_codes.length ? company.sic_codes.join(', ') : 'Unavailable'}</dd></div>
      <div><dt>Last observed</dt><dd>{date(company.last_observed_at)}</dd></div></dl>
    <section aria-label="Registered office"><h5>Registered office</h5><p>{address ?? (office?.address_state === 'withheld' ? 'Address withheld' : 'Address unavailable')}
      {postcode ? ` · ${postcode}` : ''}</p><p className="muted">A registered office is a legal contact address; it may be a home or accountant’s office rather than a trading premises.</p></section>
    <p className="muted">Incorporation and cessation are legal dates, not venue opening or closure dates. SIC codes are reported activities, not verified current trading.</p>
    {company.profile_observed_at && <p className="muted">Company profile collected {date(company.profile_observed_at)}.</p>}
    {url ? <p><a href={url} target="_blank" rel="noreferrer">View Companies House record</a></p> : <p className="muted">Official company link unavailable.</p>}
    <section aria-label="Verified premises"><h5>Verified venue links</h5>
      {!company.verified_premises.length ? <p>No verified venue links in this release. A similar name or address does not establish an operator relationship.</p>
        : <ul className="evidence-list">{company.verified_premises.map((link, index) => <li key={`${link.establishment_id}:${index}`}>
          <button className="text-button" aria-pressed={venueId === link.establishment_id} onClick={() => setVenueId(link.establishment_id)}>
            {link.display_name ?? 'Venue name unavailable'}</button>
          <p className="muted">Verified {link.relationship} · Reviewed {date(link.reviewed_on ?? link.reviewed_at)}
            {!link.primary_spatial_eligible && ' · Outside precise venue counts'}</p></li>)}</ul>}
      {venueId && <div className="linked-company-venue"><div className="card-heading"><h5>Linked venue</h5>
        <button className="text-button" onClick={() => setVenueId(null)}>Close linked venue</button></div>
        <QueryContent query={venue}>{({ data, metadata }) => <VenueEvidence venue={data} metadata={metadata} offPage={false} />}</QueryContent></div>}
    </section>
  </>;
}

export function Companies({ state, dispatch, client, release }: {
  state: Session; dispatch: Dispatch<Action>; client: QueryClient; release: Envelope_Meta_;
}) {
  const query = useApiQuery(client, 'GET /api/v1/companies', { release_id: release.release_id,
    cohort: 'registered_office', name: state.companyName || null, cursor: state.pagination.companies, limit: PAGE_SIZE });
  const selected = useApiQuery(client, 'GET /api/v1/companies/{number}', state.selectedCompany ? {
    release_id: release.release_id, number: state.selectedCompany,
  } : null);
  const heading = useRef<HTMLHeadingElement>(null);
  const pageFocus = useRef<string | null | undefined>(undefined);
  useEffect(() => {
    if (query.status === 'success' && pageFocus.current !== undefined && pageFocus.current === state.pagination.companies) {
      heading.current?.focus({ preventScroll: true }); pageFocus.current = undefined;
    }
  }, [query.status, state.pagination.companies]);
  return <>
    <div className="field"><span>Company scope</span><strong>Registered offices in Brighton &amp; Hove</strong></div>
    <p className="company-scope-note">Companies with registered offices in Brighton &amp; Hove and reported hospitality SIC codes. These are legal entities, not a list of local competitors.</p>
    <NameSearch key={state.companyName} value={state.companyName} onApply={value => dispatch({ type: 'companyName', value })} />
    <QueryContent query={query}>{({ data }) => <>
      <div className="stats"><Stat label="Companies in this scope" value={data.total} note="Legal entities, counted separately from venues" /></div>
      {data.total === 0 ? <Empty title="No companies in this scope">{state.companyName ? 'No legal names match your search in this scope. Try another name or clear the search.'
        : 'No companies match this scope in the published release.'}</Empty> : null}
      {state.selectedCompany && <section className="selected-company card" aria-label="Selected company">
        <div className="card-heading"><h3>Selected company</h3><button className="text-button" onClick={() => dispatch({ type: 'company', number: null })}>Close company</button></div>
        <QueryContent query={selected}>{({ data: company }) => <CompanyCard key={company.company_number} company={company} releaseId={release.release_id} client={client} />}</QueryContent>
      </section>}
      {data.total > 0 && <section className="company-list card" aria-label="Company list"><div className="card-heading"><h3 ref={heading} tabIndex={-1}>Company list</h3>
        <span className="list-range">Showing {state.companyHistory.length * PAGE_SIZE + 1}–{state.companyHistory.length * PAGE_SIZE + data.items.length} of {data.total.toLocaleString('en-GB')}</span></div>
        <ol start={state.companyHistory.length * PAGE_SIZE + 1}>{data.items.map(company => <li key={company.company_number}>
          <button className={`venue-row company-row${state.selectedCompany === company.company_number ? ' is-selected' : ''}`}
            aria-pressed={state.selectedCompany === company.company_number} onClick={() => dispatch({ type: 'company', number: company.company_number })}>
            <strong>{company.legal_name}</strong><span>{company.company_number} · {readable(company.legal_status)}</span>
          </button></li>)}</ol>
        <nav className="list-pagination" aria-label="Company list pages"><button className="button secondary" disabled={!state.companyHistory.length}
          onClick={() => { pageFocus.current = state.companyHistory.at(-1) ?? null; dispatch({ type: 'companyBack' }); }}>Previous companies</button>
          <span>Page {state.companyHistory.length + 1}</span><button className="button secondary" disabled={!data.has_more || !data.next_cursor}
            onClick={() => { if (data.next_cursor) { pageFocus.current = data.next_cursor; dispatch({ type: 'companyNext', cursor: data.next_cursor }); } }}>Next companies</button></nav>
      </section>}
    </>}</QueryContent>
  </>;
}
