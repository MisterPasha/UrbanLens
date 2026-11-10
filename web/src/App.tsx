import { useEffect, useReducer, useRef, useState } from 'react';
import { apiGet, ApiError } from './api/client';
import { QueryClient, useApiQuery } from './api/query';
import { Loading, Notice, Unavailable } from './components/Feedback';
import { Filters, Workspace } from './components/Workspace';
import { initialSession, sectionFromHash, sections, sessionReducer } from './state/session';
import urbanLensLogo from './assets/UrbanLens_logo_part.png';

const copy = {
  explore: ['Explore local competition', 'A clearer view of hospitality in Brighton & Hove.'],
  compare: ['Compare neighbourhoods', 'Put two places side by side, using the same evidence.'],
  activity: ['Follow observed changes', 'Understand what has changed in the source records.'],
  companies: ['Look beyond the venue name', 'Explore the legal companies behind the local business landscape.'],
  data: ['Know what is behind the numbers', 'Sources, dates and the limits of the evidence.'],
};

export function App() {
  const [state, dispatch] = useReducer(sessionReducer, undefined, initialSession);
  const [client] = useState(() => new QueryClient(releaseId => dispatch({ type: 'expired', releaseId })));
  const heading = useRef<HTMLHeadingElement>(null);
  const previousSection = useRef(state.section);
  useEffect(() => {
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort('timeout'), 15_000);
    apiGet('GET /api/v1/meta', {}, controller.signal).then(release => {
      if (!controller.signal.aborted) dispatch({ type: 'pin', release });
    }).catch((error: unknown) => {
      if (controller.signal.aborted && controller.signal.reason !== 'timeout') return;
      dispatch({ type: 'bootError', message: error instanceof ApiError ? error.message
        : 'We could not load published data. Please try again.' });
    }).finally(() => window.clearTimeout(timer));
    return () => { controller.abort(); window.clearTimeout(timer); };
  }, [state.refresh]);
  useEffect(() => {
    const navigate = () => {
      if (sections.some(section => `#${section.value}` === window.location.hash)) {
        dispatch({ type: 'navigate', section: sectionFromHash() });
      }
    };
    window.addEventListener('hashchange', navigate);
    return () => window.removeEventListener('hashchange', navigate);
  }, []);
  useEffect(() => {
    document.title = `${sections.find(section => section.value === state.section)?.label} · UrbanLens`;
    if (previousSection.current !== state.section) heading.current?.focus({ preventScroll: true });
    previousSection.current = state.section;
  }, [state.section]);
  const release = state.release;
  const areas = useApiQuery(client, 'GET /api/v1/areas', release && !state.expired
    ? { release_id: release.release_id } : null);
  const refresh = () => { client.clear(); dispatch({ type: 'refresh' }); };
  const sources = release?.metadata.sources ?? [];
  const stale = sources.some(source => source.status === 'stale');
  const limited = sources.some(source => ['unavailable', 'delayed', 'partial', 'failed', 'held'].includes(String(source.status))
    || source.retained_older_snapshot === true);
  return <>
    <a className="skip-link" href="#workspace">Skip to research</a>
    <header className="site-header"><a href="#explore" className="brand" aria-label="UrbanLens home">
      <h1><img src={urbanLensLogo} alt="" width="48" height="48" />
        <span className="brand-wordmark">Urban<span className="brand-lens">Lens</span></span></h1>
      </a><span className="header-scope">Brighton &amp; Hove <span aria-hidden="true">/</span> Hospitality</span>
      <span className="public-label">Open location research</span></header>
    <nav className="navigation" aria-label="Main navigation">{sections.map(section =>
      <a href={`#${section.value}`} key={section.value} aria-current={state.section === section.value ? 'page' : undefined}>
        {section.label}</a>)}</nav>
    <div className="app-layout"><Filters state={state} dispatch={dispatch} areas={areas} client={client} />
      <main id="workspace" tabIndex={-1}><div className="page-heading"><span className="eyebrow">BRIGHTON &amp; HOVE · HOSPITALITY</span>
        <h2 ref={heading} tabIndex={-1}>{copy[state.section][0]}</h2><p>{copy[state.section][1]}</p></div>
        <div className="release-bar"><span className={`status-dot${release ? '' : ' pending'}`} aria-hidden="true" />
          <span>{release ? `Data published ${new Date(release.metadata.published_at).toLocaleDateString('en-GB')}` : 'Connecting to published data'}</span>
          <span className="monthly">Monthly refresh</span></div>
        {state.expired ? <section className="feedback unavailable" role="alert"><h3>This data version has expired</h3>
          <p>Continue with the current published version. Your filters and area selections will be kept.</p>
          <button className="button" onClick={refresh}>Continue with current data</button></section>
          : state.bootError ? <Unavailable error={new Error(state.bootError)} retry={refresh} />
          : !release ? <Loading /> : <>
            {stale && <Notice title="Some source data is older than expected">You are viewing the last accepted observations. Publication dates do not make older source records newer.</Notice>}
            {limited && <Notice kind="limited" title="Some sources have limited availability">Accepted data remains available; a recent source collection did not complete normally.</Notice>}
            <Workspace state={state} dispatch={dispatch} client={client} release={release} />
          </>}
      </main>
    </div>
  </>;
}
