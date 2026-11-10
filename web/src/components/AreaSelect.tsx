import type { Area } from '../api/contracts';

export function AreaSelect({ label, value, areas, onChange, excluded }: {
  label: string; value: string | null; areas: Area[];
  onChange: (value: string | null) => void; excluded?: string | null;
}) {
  return <label className="field">{label}<select value={value ?? ''}
    onChange={event => onChange(event.target.value || null)}>
    <option value="">Choose a neighbourhood</option>
    {areas.map(area => <option key={area.area_code} value={area.area_code}
      disabled={area.area_code === excluded}>{area.area_name} · {area.area_code}</option>)}
  </select></label>;
}

