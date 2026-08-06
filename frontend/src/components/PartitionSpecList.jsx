import { UI_BODY_MUTED_ITALIC_CLASS } from '../uiTypography'

function PartitionFieldHeader() {
  return (
    <div className="flex items-center gap-x-4 pb-1 mb-1 border-b border-edge">
      <span className="text-xs font-bold text-slate-500 uppercase w-10 text-right shrink-0">ID</span>
      <span className="text-xs font-bold text-slate-500 uppercase w-20 text-right shrink-0">Source ID</span>
      <span className="text-xs font-bold text-slate-500 uppercase min-w-30 shrink-0">Name</span>
      <span className="text-xs font-bold text-slate-500 uppercase shrink-0">Transform</span>
    </div>
  )
}

export function PartitionFieldRow({ field, status, prevField }) {
  const dim = status === 'unchanged' ? 'opacity-50' : ''
  const bg =
    status === 'added' ? 'bg-green-900/20' :
    status === 'removed' ? 'bg-red-900/20' :
    status === 'changed' ? 'bg-amber-900/20' : ''
  const idColor =
    status === 'added' ? 'text-green-400' :
    status === 'removed' ? 'text-red-400' :
    status === 'changed' ? 'text-amber-400' : 'text-slate-500'
  const sourceIdColor =
    status === 'added' ? 'text-green-400' :
    status === 'removed' ? 'text-red-400' : 'text-slate-500'
  const nameColor =
    status === 'added' ? 'text-green-300' :
    status === 'removed' ? 'text-red-300 line-through' :
    status === 'changed' ? 'text-amber-300' : 'text-ink'
  const marker = status === 'added' ? '+' : status === 'removed' ? '−' : status === 'changed' ? '~' : null
  const transformChanged = status === 'changed' && prevField.transform !== field.transform

  return (
    <div className={`flex items-center gap-x-4 py-2 border-b border-edge last:border-0 ${bg} ${dim}`}>
      <span className={`text-base font-mono w-10 text-right shrink-0 tabular-nums ${idColor}`}>
        {marker ?? (field['field-id'] ?? '—')}
      </span>
      <span className={`text-xs font-mono w-20 text-right shrink-0 tabular-nums ${sourceIdColor}`}>
        {field['source-id'] ?? '—'}
      </span>
      <span className={`text-sm font-semibold min-w-30 shrink-0 ${nameColor}`}>
        {field.name}
        {status === 'changed' && prevField.name !== field.name && (
          <span className="text-amber-600 line-through ml-2 font-normal">{prevField.name}</span>
        )}
      </span>
      {transformChanged ? (
        <span className="text-xs font-mono flex items-center gap-2 shrink-0">
          <span className="text-red-400 line-through">{prevField.transform}</span>
          <span className="text-slate-500">→</span>
          <span className="text-green-400">{field.transform}</span>
        </span>
      ) : status === 'added' || status === 'removed' ? (
        <span className={`text-xs font-mono shrink-0 ${status === 'added' ? 'text-green-400' : 'text-red-400'}`}>
          {field.transform}
        </span>
      ) : status === 'changed' ? (
        <span className="text-xs font-mono shrink-0 text-amber-300">{field.transform}</span>
      ) : (
        <span className="text-xs font-mono text-accent bg-accent-muted px-2 py-0.5 rounded shrink-0">
          {field.transform}
        </span>
      )}
    </div>
  )
}

export default function PartitionSpecList({ spec }) {
  if (!spec?.fields?.length) {
    return <p className={UI_BODY_MUTED_ITALIC_CLASS}>Unpartitioned.</p>
  }
  return (
    <div>
      <PartitionFieldHeader />
      {spec.fields.map((f, i) => <PartitionFieldRow key={f['field-id'] ?? i} field={f} />)}
    </div>
  )
}

export function PartitionDiffView({ diff }) {
  return (
    <div>
      <PartitionFieldHeader />
      {diff.map((entry, i) => (
        <PartitionFieldRow
          key={i}
          field={entry.status === 'changed' ? entry.curr : entry.field}
          prevField={entry.status === 'changed' ? entry.prev : undefined}
          status={entry.status}
        />
      ))}
    </div>
  )
}
