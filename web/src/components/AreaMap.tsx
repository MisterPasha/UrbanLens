import { useEffect, useRef, useState } from 'react';
import * as L from 'leaflet';
import type { GeoJsonObject } from 'geojson';
import 'leaflet/dist/leaflet.css';
import type { Area } from '../api/contracts';

export function AreaMap({ areas, selected, onSelect }: {
  areas: Area[]; selected: (string | null)[]; onSelect: (code: string) => void;
}) {
  const container = useRef<HTMLDivElement>(null);
  const instance = useRef<L.Map | null>(null);
  const layers = useRef(new Map<string, L.GeoJSON>());
  const select = useRef(onSelect);
  const tiles = useRef<L.TileLayer | null>(null);
  const [failed, setFailed] = useState(false);
  const [missing, setMissing] = useState(false);
  useEffect(() => { select.current = onSelect; }, [onSelect]);
  useEffect(() => {
    if (!container.current) return;
    const map = L.map(container.current, { scrollWheelZoom: true, minZoom: 10, maxZoom: 19, zoomSnap: .25,
      zoomAnimation: false, fadeAnimation: false }).setView([50.84, -.14], 12);
    instance.current = map;
    const background = L.tileLayer(import.meta.env.VITE_MAP_TILE_URL ?? 'https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
      maxZoom: 19, keepBuffer: 1, attribution: import.meta.env.VITE_MAP_TILE_ATTRIBUTION
        ?? '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    }).addTo(map);
    tiles.current = background;
    background.on('tileerror', () => {
      setFailed(true);
      queueMicrotask(() => { if (instance.current === map && map.hasLayer(background)) background.remove(); });
    });
    const resize = new ResizeObserver(() => map.invalidateSize({ pan: false }));
    resize.observe(container.current);
    return () => { resize.disconnect(); background.off(); map.remove(); instance.current = null; };
  }, []);
  useEffect(() => {
    const map = instance.current;
    if (!map) return;
    const current = layers.current;
    let invalid = false;
    for (const area of areas) {
      try {
        if (!['Polygon', 'MultiPolygon'].includes(String(area.display_geometry.type))) { invalid = true; continue; }
        const label = document.createElement('span'); label.textContent = `${area.area_name} · ${area.area_code}`;
        const layer = L.geoJSON(area.display_geometry as unknown as GeoJsonObject, { onEachFeature: (_, polygon) => {
          polygon.on('click', () => select.current(area.area_code));
        } }).bindTooltip(label).addTo(map);
        layer.eachLayer(polygon => {
          if (polygon instanceof L.Path) polygon.getElement()?.setAttribute('data-area-code', area.area_code);
        });
        current.set(area.area_code, layer);
      } catch { invalid = true; }
    }
    queueMicrotask(() => setMissing(invalid || current.size === 0));
    const bounds = L.featureGroup([...current.values()]).getBounds();
    if (bounds.isValid()) map.fitBounds(bounds, { padding: [12, 12], animate: false });
    return () => { for (const layer of current.values()) layer.remove(); current.clear(); };
  }, [areas]);
  useEffect(() => {
    for (const [code, layer] of layers.current) {
      const slot = selected.indexOf(code);
      layer.setStyle({ color: slot === 0 ? '#126f61' : slot === 1 ? '#7850a0' : '#547c6d',
        fillColor: slot === 0 ? '#168b79' : slot === 1 ? '#9460b8' : '#8bafa0',
        weight: slot >= 0 ? 3 : 1, fillOpacity: slot >= 0 ? .35 : .07 });
      layer.eachLayer(polygon => { if (polygon instanceof L.Path) {
        polygon.getElement()?.setAttribute('data-selected-slot', slot < 0 ? '' : String(slot + 1));
      } });
      if (slot >= 0) layer.bringToFront();
    }
  }, [selected, areas]);
  return <section className="venue-map-panel" aria-label="Neighbourhood boundary map">
    <div className="map-caption"><strong>Find an area on the map</strong><span>First area: green · Second area: purple</span>
      <button className="text-button" onClick={() => {
        const chosen = selected.flatMap(code => code && layers.current.has(code) ? [layers.current.get(code)!] : []);
        const bounds = L.featureGroup(chosen.length ? chosen : [...layers.current.values()]).getBounds();
        if (bounds.isValid()) instance.current?.fitBounds(bounds, { padding: [20, 20], maxZoom: 16, animate: false });
      }}>Show selected areas</button></div>
    <div ref={container} className="venue-map area-map" role="region" aria-label="Click a boundary to choose an area. Search and selectors provide keyboard access." />
    {failed && <div className="map-message" role="status">Map background unavailable. Boundaries, area selection and data still work.
      <button className="text-button" onClick={() => {
        setFailed(false); if (instance.current && tiles.current) tiles.current.addTo(instance.current).redraw();
      }}>Retry map background</button></div>}
    {missing && <p className="map-message" role="status">Some area boundaries are unavailable. Use the searchable selectors for all published areas.</p>}
    <p className="map-access">© <a href="https://www.openstreetmap.org/copyright">OpenStreetMap contributors</a> · ONS boundaries, OGL v3 · <a href="/data-reuse/index.html">Data reuse</a>.
      Street names and landmarks help you locate an area. Outlines are simplified 2021 statistical boundaries;
      they do not necessarily follow familiar neighbourhoods and are not hexagons. Use the selectors for keyboard selection.</p>
  </section>;
}
