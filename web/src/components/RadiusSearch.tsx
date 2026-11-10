import { useEffect, useRef, useState } from 'react';
import type { Dispatch, SubmitEvent } from 'react';
import { ApiError, apiGet } from '../api/client';
import type { Postcode } from '../api/contracts';
import { explorationParams } from '../api/exploration';
import { useApiQuery } from '../api/query';
import type { QueryClient } from '../api/query';
import type { Action, Session } from '../state/session';

const postcodePattern = /^(?:GIR0AA|(?:[A-PR-UWYZ][0-9][0-9A-HJKPSTUW]?|[A-PR-UWYZ][A-HK-Y][0-9][0-9ABEHMNPRVWXY]?)[0-9][ABD-HJLNP-UW-Z]{2})$/;
const postcodeMessages: Record<Exclude<Postcode['state'], 'resolved'>, string> = {
  unmatched: 'That postcode was not found. Check the full postcode and try again.',
  outside: 'That postcode is outside the supported Brighton & Hove area.',
  incompatible_geography: 'That postcode cannot be matched to the supported area geography.',
  provider_failure: 'The postcode service is temporarily unavailable. Please try again.',
  throttled: 'Postcode lookups are temporarily limited. Please wait before trying again.',
};

export function RadiusSearch({ state, dispatch, client }: {
  state: Session; dispatch: Dispatch<Action>; client: QueryClient;
}) {
  const [postcode, setPostcode] = useState(state.search.centreSource === 'postcode' ? state.search.centreLabel ?? '' : '');
  const [lookup, setLookup] = useState<{ token: string; loading: boolean; message?: string } | null>(null);
  const controller = useRef<AbortController | null>(null);
  const candidate = state.radiusCandidate;
  const checked = useApiQuery(client, 'GET /api/v1/establishments',
    candidate && state.release && !state.expired ? explorationParams(state, state.release.release_id, candidate) : null);
  useEffect(() => {
    if (candidate && checked.status === 'success') dispatch({ type: 'acceptCentre', value: candidate });
  }, [candidate, checked.status, checked.data, dispatch]);
  useEffect(() => {
    if (lookup?.token !== state.centreIntent) controller.current?.abort();
  }, [state.centreIntent, lookup?.token]);
  useEffect(() => () => controller.current?.abort(), []);

  async function submit(event: SubmitEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!state.release || state.expired) return;
    controller.current?.abort();
    const token = crypto.randomUUID();
    const key = postcode.toUpperCase().replace(/\s/g, '');
    dispatch({ type: 'lookupStart', token });
    if (!postcodePattern.test(key)) {
      setLookup({ token, loading: false, message: 'Enter a valid full UK postcode, for example BN1 1AA.' });
      return;
    }
    const pending = new AbortController();
    controller.current = pending;
    const timeout = window.setTimeout(() => pending.abort('timeout'), 15_000);
    setLookup({ token, loading: true });
    try {
      const result = await apiGet('GET /api/v1/postcodes/{postcode}', {
        postcode: key, release_id: state.release.release_id,
      }, pending.signal);
      if (pending.signal.aborted || controller.current !== pending) return;
      if (result.release_id !== state.release.release_id) throw new Error('Mismatched data version');
      const point = result.data;
      if (point.state === 'resolved' && point.can_define_search
        && typeof point.longitude === 'number' && typeof point.latitude === 'number'
        && Number.isFinite(point.longitude) && Number.isFinite(point.latitude)) {
        const label = point.postcode.replace(/\s/g, '').replace(/(.{3})$/, ' $1').trim();
        dispatch({ type: 'proposeCentre', token, value: {
          longitude: point.longitude, latitude: point.latitude, label, source: 'postcode',
        } });
        setLookup(null);
      } else setLookup({ token, loading: false, message: point.state === 'resolved'
        ? postcodeMessages.provider_failure : postcodeMessages[point.state] });
    } catch (error) {
      if (controller.current !== pending || (pending.signal.aborted && pending.signal.reason !== 'timeout')) return;
      if (error instanceof ApiError && error.status === 410) dispatch({ type: 'expired', releaseId: state.release.release_id });
      setLookup({ token, loading: false, message: error instanceof ApiError && error.status === 422
        ? 'Enter a valid full UK postcode, for example BN1 1AA.'
        : error instanceof ApiError && error.status === 429 ? postcodeMessages.throttled
          : pending.signal.reason === 'timeout' ? 'The postcode lookup timed out. Please try again.' : postcodeMessages.provider_failure });
    } finally {
      window.clearTimeout(timeout);
      if (controller.current === pending) controller.current = null;
    }
  }

  const visibleLookup = lookup?.token === state.centreIntent ? lookup : null;
  return <div className="radius-search">
    <form onSubmit={submit} noValidate className="postcode-form">
      <label className="field">Full UK postcode<input type="text" autoComplete="postal-code"
        placeholder="e.g. BN1 1AA" maxLength={10} spellCheck={false} value={postcode}
        onChange={event => { controller.current?.abort(); dispatch({ type: 'cancelCentre' }); setPostcode(event.target.value); setLookup(null); }} /></label>
      <button className="button secondary" disabled={!state.release || state.expired || visibleLookup?.loading}>Use postcode</button>
    </form>
    {visibleLookup?.loading && <p className="centre-feedback" role="status">Looking up postcode…</p>}
    {visibleLookup?.message && <p className="centre-feedback error" role="alert">{visibleLookup.message} Your current search has not moved.</p>}
    {candidate && checked.status === 'loading' && <p className="centre-feedback" role="status">Checking the new centre… Your current search stays in place.</p>}
    {candidate && checked.status === 'error' && <div className="centre-feedback error" role="alert">
      <p>{checked.error instanceof ApiError && checked.error.status === 422
        ? 'This point is outside the supported Brighton & Hove area.'
        : checked.error instanceof ApiError && checked.error.status === 429
          ? 'Search is temporarily limited. Please wait before trying again.'
          : 'We could not check the new search centre. Please try again.'} Your current search has not moved.</p>
      {!(checked.error instanceof ApiError && checked.error.status === 422)
        && <button className="text-button" onClick={checked.retry}>Retry centre check</button>}
    </div>}
    {(candidate || state.pickingCentre || visibleLookup?.loading) && <button className="text-button" onClick={() => {
      controller.current?.abort(); dispatch({ type: 'cancelCentre' }); setLookup(null);
    }}>Cancel new centre</button>}
    <p className="filter-note">Or choose a point on the map. A postcode gives an approximate search centre, not a venue location.</p>
  </div>;
}
