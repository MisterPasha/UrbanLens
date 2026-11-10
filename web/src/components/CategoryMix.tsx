import { categories, categoryColours } from '../state/session';
import type { Category } from '../state/session';

export function CategoryMix({ mix, total, selected, showCaption = true }: {
  mix: Record<string, number>; total: number; selected: Category[]; showCaption?: boolean;
}) {
  return <div className="category-mix"><table aria-label={showCaption ? undefined : 'Venue category breakdown'}>
    {showCaption && <caption>Category counts and shares of matching venues</caption>}
    <thead><tr><th scope="col">Category</th><th scope="col">Venues</th><th scope="col">Share</th></tr></thead>
    <tbody>{categories.filter(category => selected.includes(category.value)).map(category => {
      const count = mix[category.value] ?? 0;
      const share = total > 0 ? count / total * 100 : null;
      return <tr key={category.value}><th scope="row"><span className="mix-label">{category.label}</span>
        <span className="mix-track" aria-hidden="true"><span style={{ width: `${share ?? 0}%`, backgroundColor: categoryColours[category.value] }} /></span></th>
        <td>{count.toLocaleString('en-GB')}</td><td>{share === null ? 'Unavailable' : `${share.toFixed(1)}%`}</td></tr>;
    })}</tbody></table>
    {total === 0 && <p className="muted">Shares are unavailable because there are no matching venues.</p>}
    <p className="muted">One primary category per venue. Shares use your selected categories; rounding may affect the total.
      Other food service is a broader category where the sources do not support a finer classification.</p>
  </div>;
}
