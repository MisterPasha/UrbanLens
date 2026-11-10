import { useState } from 'react';
import type { Dispatch, SubmitEvent } from 'react';
import type { Area, Envelope_Meta_ } from '../api/contracts';
import type { QueryClient, QueryResult } from '../api/query';
import { categories } from '../state/session';
import type { Action, Radius, Session } from '../state/session';
import { QueryContent } from './Feedback';
import { Explore } from './Exploration';
import { RadiusSearch } from './RadiusSearch';
import { Compare } from './Comparison';
import { Companies } from './Companies';
import { Activity } from './Activity';
import { Data } from './Methodology';
import { AreaSelect } from './AreaSelect';

function CentreForm({ state, dispatch }: { state: Session; dispatch: Dispatch<Action> }) {
  const [longitude, setLongitude] = useState(state.search.centre?.longitude.toString() ?? '');
  const [latitude, setLatitude] = useState(state.search.centre?.latitude.toString() ?? '');
  function apply(event: SubmitEvent<HTMLFormElement>) {
    event.preventDefault();
    dispatch({ type: 'proposeCentre', value: {
      longitude: Number(longitude), latitude: Number(latitude), label: 'Coordinates', source: 'coordinates',
    } });
  }
  return <form className="centre-form" onSubmit={apply}>
    <p className="muted">Use a point within Brighton &amp; Hove.</p>
    <label className="field">Longitude<input type="number" step="any" min="-180" max="180"
      required value={longitude} onChange={event => setLongitude(event.target.value)} /></label>
    <label className="field">Latitude<input type="number" step="any" min="-90" max="90"
      required value={latitude} onChange={event => setLatitude(event.target.value)} /></label>
    <button className="button secondary" type="submit">Apply centre</button>
  </form>;
}

export function Filters({ state, dispatch, areas, client }: {
  state: Session; dispatch: Dispatch<Action>; areas: QueryResult<{ data: Area[] }>; client: QueryClient;
}) {
  const relevant = state.section === 'explore' || state.section === 'compare';
  return <aside className="filters" aria-label="Research filters">
    <div className="filter-title"><h2>Your research</h2><span className="tag">This session</span></div>
    <div className="fixed-scope"><span className="eyebrow">LOCATION</span><strong>Brighton &amp; Hove</strong>
      <span className="eyebrow">SECTOR</span><strong>Hospitality</strong></div>
    <fieldset disabled={!relevant}><legend>Venue categories</legend>
      <p className="filter-note">Shared by Explore and Compare.</p>
      {categories.map(category => <label className="check" key={category.value}>
        <input type="checkbox" checked={state.categories.includes(category.value)}
          onChange={() => dispatch({ type: 'category', category: category.value })} />{category.label}
      </label>)}
      <div className="filter-actions"><button onClick={() => dispatch({ type: 'categories',
        values: categories.map(category => category.value) })}>Select all</button>
        <button onClick={() => dispatch({ type: 'categories', values: [] })}>Clear</button></div>
    </fieldset>
    {state.section === 'explore' && <div className="scope-controls">
      <label className="field">Search scope<select value={state.search.mode} onChange={event =>
        dispatch({ type: 'search', value: { mode: event.target.value as Session['search']['mode'] } })}>
        <option value="city">Whole city</option><option value="area">One neighbourhood</option>
        <option value="radius">Around a point</option></select></label>
      {state.search.mode === 'area' && <QueryContent query={areas}>{result => <AreaSelect
        label="Neighbourhood" areas={result.data} value={state.search.areaCode}
        onChange={areaCode => dispatch({ type: 'search', value: { areaCode } })} />}</QueryContent>}
      {state.search.mode === 'radius' && <>
        <label className="field">Search radius<select value={state.search.radius} onChange={event =>
          dispatch({ type: 'search', value: { radius: Number(event.target.value) as Radius } })}>
          {[250, 500, 1000, 2000].map(radius => <option key={radius} value={radius}>{radius < 1000
            ? `${radius} m` : `${radius / 1000} km`}</option>)}</select></label>
        <RadiusSearch key={state.release?.release_id ?? 'loading'} state={state} dispatch={dispatch} client={client} />
        <details className="coordinate-entry"><summary>Enter coordinates</summary>
          <CentreForm key={`${state.search.centre?.longitude}:${state.search.centre?.latitude}`} state={state} dispatch={dispatch} /></details>
      </>}
    </div>}
    {!relevant && <p className="filter-note">Your venue filters are kept while you browse this section.
      Company and activity records have their own scope.</p>}
    <p className="session-note">Selections stay with you as you move between sections. Nothing is saved to an account.</p>
  </aside>;
}

interface Props { state: Session; dispatch: Dispatch<Action>; client: QueryClient; release: Envelope_Meta_ }

export function Workspace(props: Props) {
  switch (props.state.section) {
    case 'explore': return <Explore {...props} />;
    case 'compare': return <Compare {...props} />;
    case 'activity': return <Activity {...props} />;
    case 'companies': return <Companies {...props} />;
    case 'data': return <Data {...props} />;
  }
}
