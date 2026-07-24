import { UI_BODY_MUTED_ITALIC_CLASS } from '../uiTypography'

export function PartitionFieldRow({ field }) {
  return (
    <div className="flex items-center gap-x-4 py-2 border-b border-edge last:border-0">
      <span className="text-base font-mono text-slate-500 w-10 text-right shrink-0 tabular-nums">
        {field['field-id'] ?? '—'}
      </span>
      <span className="text-xs font-mono text-slate-500 w-14 text-right shrink-0 tabular-nums">
        {field['source-id'] ?? '—'}
      </span>
      <span className="text-sm font-semibold text-ink min-w-30 shrink-0">{field.name}</span>
      <span className="text-xs font-mono text-accent bg-accent-muted px-2 py-0.5 rounded shrink-0">
        {field.transform}
      </span>
    </div>
  )
}

export default function PartitionSpecList({ spec }) {
  if (!spec?.fields?.length) {
    return <p className={UI_BODY_MUTED_ITALIC_CLASS}>Unpartitioned.</p>
  }
  return (
    <div>
      <div className="flex items-center gap-x-4 pb-1 mb-1 border-b border-edge">
        <span className="text-xs font-bold text-slate-500 uppercase w-10 text-right shrink-0">ID</span>
        <span className="text-xs font-bold text-slate-500 uppercase w-14 text-right shrink-0">Source ID</span>
        <span className="text-xs font-bold text-slate-500 uppercase min-w-30 shrink-0">Name</span>
        <span className="text-xs font-bold text-slate-500 uppercase shrink-0">Transform</span>
      </div>
      {spec.fields.map((f, i) => <PartitionFieldRow key={f['field-id'] ?? i} field={f} />)}
    </div>
  )
}
