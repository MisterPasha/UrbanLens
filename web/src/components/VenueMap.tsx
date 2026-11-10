import { useEffect, useRef, useState } from 'react';
import * as L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import type { MapPoint, Venue } from '../api/contracts';
import type { MapView } from '../state/session';
import { categories, categoryColours } from '../state/session';

export interface SearchCircle { longitude: number; latitude: number; radius: number; label: string }

interface Props {
  points: MapPoint[];
  items: Venue[];
  selectedId: string | null;
  selectedPoint: MapPoint | null;
  initialView: MapView | null;
  onSelect: (id: string) => void;
  onViewChange: (view: MapView) => void;
  circle: SearchCircle | null;
  allowCentreSelection: boolean;
  pickingCentre: boolean;
  awaitingCentre?: boolean;
  onPickCentre: (point: { longitude: number; latitude: number }) => void;
  onPickingChange: (active: boolean) => void;
}

export function VenueMap({ points, items, selectedId, selectedPoint, initialView, onSelect, onViewChange,
  circle, allowCentreSelection, pickingCentre, awaitingCentre, onPickCentre, onPickingChange }: Props) {
  const container = useRef<HTMLDivElement>(null);
  const instance = useRef<L.Map | null>(null);
  const tileLayer = useRef<L.TileLayer | null>(null);
  const markers = useRef(new Map<string, L.CircleMarker>());
  const selectedMarker = useRef<L.CircleMarker | null>(null);
  const startView = useRef(initialView);
  const initialCircle = useRef(circle);
  const circleLayer = useRef<L.Circle | null>(null);
  const fitted = useRef(false);
  const [background, setBackground] = useState<'loading' | 'ready' | 'failed'>('loading');
  const [unavailable, setUnavailable] = useState(false);

  useEffect(() => {
    if (!container.current) return;
    const currentMarkers = markers.current;
    let map: L.Map | null = null;
    try {
      map = L.map(container.current, { scrollWheelZoom: true, minZoom: 10, maxZoom: 19,
        zoomAnimation: false, fadeAnimation: false, markerZoomAnimation: false });
      instance.current = map;
      const start = startView.current;
      map.setView(start ? [start.latitude, start.longitude] : [50.84, -0.14], start?.zoom ?? 12);
      const tiles = L.tileLayer(import.meta.env.VITE_MAP_TILE_URL
        ?? 'https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 19, keepBuffer: 1,
        attribution: import.meta.env.VITE_MAP_TILE_ATTRIBUTION
          ?? '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      });
      tileLayer.current = tiles;
      tiles.on('tileload', () => setBackground(current => current === 'failed' ? current : 'ready'));
      tiles.on('tileerror', () => {
        setBackground('failed');
        // Stop further background requests on failure, retaining all vector points.
        queueMicrotask(() => { if (instance.current === map && map?.hasLayer(tiles)) tiles.remove(); });
      });
      tiles.addTo(map);
      map.on('moveend', () => {
        if (!map) return;
        const centre = map.getCenter();
        onViewChange({ latitude: centre.lat, longitude: centre.lng, zoom: map.getZoom() });
      });
      const resize = new ResizeObserver(() => map?.invalidateSize({ pan: false }));
      resize.observe(container.current);
      return () => {
        resize.disconnect(); tiles.off(); map?.off(); map?.remove();
        instance.current = null; tileLayer.current = null; currentMarkers.clear();
        selectedMarker.current = null; fitted.current = false;
      };
    } catch {
      map?.remove(); instance.current = null;
      // Defer notification so construction failures cannot affect the list render.
      queueMicrotask(() => setUnavailable(true));
    }
  }, [onViewChange]);

  useEffect(() => {
    const map = instance.current;
    if (!map || !allowCentreSelection || !pickingCentre) return;
    const choose = (event: L.LeafletMouseEvent) => onPickCentre({ longitude: event.latlng.lng, latitude: event.latlng.lat });
    map.on('click', choose);
    return () => { map.off('click', choose); };
  }, [allowCentreSelection, pickingCentre, onPickCentre]);

  useEffect(() => {
    const map = instance.current;
    if (!map || !circle) return;
    const ring = L.circle([circle.latitude, circle.longitude], { radius: circle.radius,
      color: '#126f61', weight: 2, fillColor: '#168b79', fillOpacity: .08,
      interactive: false, className: 'search-circle' }).addTo(map);
    circleLayer.current = ring;
    ring.bringToBack();
    const element = ring.getElement();
    element?.setAttribute('data-radius', String(circle.radius));
    element?.setAttribute('data-longitude', String(circle.longitude));
    element?.setAttribute('data-latitude', String(circle.latitude));
    const label = document.createElement('span'); label.textContent = `Search centre: ${circle.label}`;
    const centre = L.circleMarker([circle.latitude, circle.longitude], { radius: 5, color: '#102c27',
      weight: 2, fillColor: '#fff', fillOpacity: 1, interactive: false, className: 'search-centre' })
      .bindTooltip(label, { permanent: false }).addTo(map);
    fitted.current = true;
    if (!startView.current || circle !== initialCircle.current) map.fitBounds(ring.getBounds(), { padding: [20, 20], maxZoom: 17, animate: false });
    return () => { ring.remove(); centre.remove(); circleLayer.current = null; };
  }, [circle]);

  useEffect(() => {
    const map = instance.current;
    if (!map) return;
    const currentMarkers = markers.current;
    const names = new Map(items.map(item => [item.establishment_id, item.display_name ?? 'Name unavailable']));
    for (const marker of currentMarkers.values()) marker.remove();
    currentMarkers.clear();
    for (const point of points) {
      const label = document.createElement('span');
      label.textContent = names.get(point.establishment_id)
        ?? `${categories.find(category => category.value === point.primary_category)?.label ?? 'Venue'} · select for details`;
      const marker = L.circleMarker([point.latitude, point.longitude], {
        radius: 6, color: '#fff', weight: 1.5, fillColor: categoryColours[point.primary_category], fillOpacity: .9,
      }).bindTooltip(label).on('click', (event: L.LeafletMouseEvent) => {
        L.DomEvent.stopPropagation(event.originalEvent);
        if (pickingCentre && allowCentreSelection) onPickCentre({ longitude: point.longitude, latitude: point.latitude });
        else onSelect(point.establishment_id);
      }).addTo(map);
      marker.getElement()?.setAttribute('data-venue-id', point.establishment_id);
      currentMarkers.set(point.establishment_id, marker);
    }
    if (!fitted.current && points.length && !circle) {
      fitted.current = true;
      if (!startView.current) map.fitBounds(L.latLngBounds(points.map(point => [point.latitude, point.longitude])),
        { padding: [24, 24], maxZoom: 16, animate: false });
    }
    return () => { for (const marker of currentMarkers.values()) marker.remove(); currentMarkers.clear(); };
  }, [points, items, onSelect, pickingCentre, allowCentreSelection, onPickCentre, circle]);

  useEffect(() => {
    const map = instance.current;
    if (!map) return;
    selectedMarker.current?.remove(); selectedMarker.current = null;
    for (const [id, marker] of markers.current) {
      marker.setStyle({ radius: id === selectedId ? 9 : 6,
        color: id === selectedId ? '#102c27' : '#fff', weight: id === selectedId ? 3 : 1.5 });
      marker.getElement()?.classList.toggle('selected-marker', id === selectedId);
    }
    if (selectedPoint && !markers.current.has(selectedPoint.establishment_id)) {
      selectedMarker.current = L.circleMarker([selectedPoint.latitude, selectedPoint.longitude], {
        radius: 9, color: '#102c27', weight: 3,
        fillColor: categoryColours[selectedPoint.primary_category], fillOpacity: 1,
      }).on('click', (event: L.LeafletMouseEvent) => {
        L.DomEvent.stopPropagation(event.originalEvent);
        if (pickingCentre && allowCentreSelection) onPickCentre({ longitude: selectedPoint.longitude, latitude: selectedPoint.latitude });
        else onSelect(selectedPoint.establishment_id);
      }).addTo(map);
      selectedMarker.current.getElement()?.setAttribute('data-venue-id', selectedPoint.establishment_id);
      selectedMarker.current.getElement()?.classList.add('selected-marker');
    }
    const location = selectedPoint ?? points.find(point => point.establishment_id === selectedId);
    if (location) {
      const marker = markers.current.get(location.establishment_id) ?? selectedMarker.current;
      marker?.bringToFront();
      if (!map.getBounds().contains([location.latitude, location.longitude])) {
        map.panTo([location.latitude, location.longitude], { animate: false });
      }
    }
  }, [selectedId, selectedPoint, points, items, onSelect, pickingCentre, allowCentreSelection, onPickCentre]);

  return <section className={`venue-map-panel${pickingCentre ? ' choosing-centre' : ''}`} aria-label="Competition map" onKeyDown={event => {
    if (event.key === 'Escape' && pickingCentre) onPickingChange(false);
  }}>
    <div className="map-caption"><strong>Venue map</strong><span>Moving the map does not change the result filters.</span>
      {circle ? <button className="text-button" onClick={() => {
        const bounds = circleLayer.current?.getBounds();
        if (bounds) instance.current?.fitBounds(bounds, { padding: [20, 20], maxZoom: 17, animate: false });
      }}>Show search circle</button> : <button className="text-button" disabled={!points.length}
        onClick={() => instance.current?.fitBounds(L.latLngBounds(points.map(point => [point.latitude, point.longitude])),
          { padding: [24, 24], maxZoom: 16, animate: false })}>Show overview points</button>}</div>
    {allowCentreSelection && <div className="map-centre-controls">
      <button className="button secondary" aria-pressed={pickingCentre}
        onClick={() => onPickingChange(!pickingCentre)}>{pickingCentre ? 'Cancel map selection' : 'Choose centre on map'}</button>
      <button className="button secondary" onClick={() => {
        const point = instance.current?.getCenter();
        if (point) onPickCentre({ longitude: point.lng, latitude: point.lat });
      }}>Use map view as centre</button>
      {pickingCentre && <p role="status">Click a point on the map. The current search changes only after the point is checked. Escape cancels selection.</p>}
      {awaitingCentre && !pickingCentre && <p>No radius search is applied yet. Choose a centre here or use a full postcode.</p>}
    </div>}
    <div ref={container} className="venue-map" role="region"
      aria-label="Venue map. Use the venue list for keyboard selection." />
    {unavailable ? <p role="status" className="map-message">The map could not start. All results remain available in the venue list.</p>
      : background === 'failed' ? <div className="map-message" role="status"><p>{awaitingCentre
        ? 'Map background unavailable. You can still use a postcode or choose a centre.'
        : 'Map background unavailable. Venue points, the list and all counts still work.'}</p>
        <button className="text-button" onClick={() => {
          setBackground('loading');
          if (instance.current && tileLayer.current) tileLayer.current.addTo(instance.current).redraw();
        }}>Retry map background</button></div>
        : background === 'loading' && <p className="map-message" role="status">Loading map background…</p>}
    <p className="map-access">© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap contributors</a> · <a href="/data-reuse/index.html">Data reuse</a>. <a href="#venue-list">Use the venue list</a> for keyboard access. <a
      href="https://www.openstreetmap.org/fixthemap" target="_blank" rel="noreferrer">Report a map issue</a></p>
  </section>;
}
