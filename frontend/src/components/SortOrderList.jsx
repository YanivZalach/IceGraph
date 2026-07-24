import { UI_BODY_MUTED_ITALIC_CLASS, UI_BODY_MUTED_CLASS } from '../uiTypography'

export function SortFieldRow({ field }) {
  return (
    <div className="grid grid-cols-[1fr_120px_120px] py-2 border-b border-edge last:border-0 items-center">
      <span className="text-sm font-mono text-accent">
        {typeof field.transform === 'object' ? JSON.stringify(field.transform) : field.transform}
      </span>
      <span className="text-sm text-ink">{field.direction}</span>
      <span className={UI_BODY_MUTED_CLASS}>{field['null-order']}</span>
    </div>
  )
}

export default function SortOrderList({ order }) {
  if (!order?.fields?.length) {
    return <p className={UI_BODY_MUTED_ITALIC_CLASS}>Unsorted.</p>
  }
  return (
    <div>
      <div className="grid grid-cols-[1fr_120px_120px] pb-1 mb-1 border-b border-edge">
        <span className="text-xs font-bold text-slate-500 uppercase">Transform</span>
        <span className="text-xs font-bold text-slate-500 uppercase">Direction</span>
        <span className="text-xs font-bold text-slate-500 uppercase">Nulls</span>
      </div>
      {order.fields.map((f, i) => <SortFieldRow key={f['source-id'] ?? i} field={f} />)}
    </div>
  )
}
