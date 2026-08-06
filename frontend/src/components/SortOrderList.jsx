import { UI_BODY_MUTED_ITALIC_CLASS, UI_BODY_MUTED_CLASS } from '../uiTypography'

function TypeText({ type }) {
  return typeof type === 'string' ? type : JSON.stringify(type)
}

function SortFieldHeader() {
  return (
    <div className="grid grid-cols-[90px_1fr_120px_120px] pb-1 mb-1 border-b border-edge">
      <span className="text-xs font-bold text-slate-500 uppercase">Source ID</span>
      <span className="text-xs font-bold text-slate-500 uppercase">Transform</span>
      <span className="text-xs font-bold text-slate-500 uppercase">Direction</span>
      <span className="text-xs font-bold text-slate-500 uppercase">Nulls</span>
    </div>
  )
}

export function SortFieldRow({ field, status, prevField }) {
  const dim = status === 'unchanged' ? 'opacity-50' : ''
  const bg =
    status === 'added' ? 'bg-green-900/20' :
    status === 'removed' ? 'bg-red-900/20' :
    status === 'changed' ? 'bg-amber-900/20' : ''
  const transformColor =
    status === 'added' ? 'text-green-400' :
    status === 'removed' ? 'text-red-400 line-through' :
    status === 'changed' ? 'text-amber-300' : 'text-accent'
  const directionColor =
    status === 'added' ? 'text-green-300' :
    status === 'removed' ? 'text-red-300' :
    status === 'changed' ? 'text-amber-300' : 'text-ink'
  const nullOrderColor =
    status === 'added' ? 'text-green-400' :
    status === 'removed' ? 'text-red-400' :
    status === 'changed' ? 'text-amber-300' : ''

  return (
    <div className={`grid grid-cols-[90px_1fr_120px_120px] py-2 border-b border-edge last:border-0 items-center ${bg} ${dim}`}>
      <span className="text-xs font-mono text-slate-500 tabular-nums">
        {field['source-id'] ?? '—'}
      </span>
      <span className={`text-sm font-mono ${transformColor}`}>
        <TypeText type={field.transform} />
      </span>
      <span className={`text-sm ${directionColor}`}>
        {field.direction}
        {status === 'changed' && prevField.direction !== field.direction && (
          <span className="text-amber-600 line-through ml-2">{prevField.direction}</span>
        )}
      </span>
      <span className={nullOrderColor ? `text-sm ${nullOrderColor}` : UI_BODY_MUTED_CLASS}>
        {field['null-order']}
        {status === 'changed' && prevField['null-order'] !== field['null-order'] && (
          <span className="text-amber-600 line-through ml-2">{prevField['null-order']}</span>
        )}
      </span>
    </div>
  )
}

export default function SortOrderList({ order }) {
  if (!order?.fields?.length) {
    return <p className={UI_BODY_MUTED_ITALIC_CLASS}>Unsorted.</p>
  }
  return (
    <div>
      <SortFieldHeader />
      {order.fields.map((f, i) => <SortFieldRow key={f['source-id'] ?? i} field={f} />)}
    </div>
  )
}

export function SortDiffView({ diff }) {
  return (
    <div>
      <SortFieldHeader />
      {diff.map((entry, i) => (
        <SortFieldRow
          key={i}
          field={entry.status === 'changed' ? entry.curr : entry.field}
          prevField={entry.status === 'changed' ? entry.prev : undefined}
          status={entry.status}
        />
      ))}
    </div>
  )
}
