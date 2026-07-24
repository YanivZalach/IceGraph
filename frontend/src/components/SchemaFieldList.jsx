export function TypeDisplay({ type }) {
  if (typeof type === 'string') {
    return (
      <span className="text-xs font-mono text-accent bg-accent-muted px-2 py-0.5 rounded">
        {type}
      </span>
    )
  }

  if (type.type === 'struct') {
    return (
      <div className="flex flex-col gap-2">
        <span className="text-xs font-mono text-violet-400 bg-violet-900/30 px-2 py-0.5 rounded w-fit">
          struct
        </span>
        <div className="ml-3 border-l-2 border-edge pl-4 flex flex-col gap-3 py-1">
          {type.fields.map(f => (
            <div key={f.id || f['field-id'] || f.name} className="flex flex-col gap-1.5">
              <div className="flex items-center gap-2">
                <span className="text-base font-mono text-slate-500 w-5 text-right shrink-0">
                  {f.id || f['field-id'] || '—'}
                </span>
                <span className="text-sm font-semibold text-ink">{f.name}</span>
                {f.required === false && (
                  <span className="text-xs font-bold text-slate-600 uppercase">optional</span>
                )}
              </div>
              <TypeDisplay type={f.type} />
            </div>
          ))}
        </div>
      </div>
    )
  }

  if (type.type === 'list') {
    return (
      <div className="flex flex-col gap-2">
        <span className="text-xs font-mono text-amber-400 bg-amber-900/40 px-2 py-0.5 rounded w-fit">
          list
        </span>
        <div className="ml-3 border-l-2 border-edge pl-4 py-1">
          <TypeDisplay type={type.element} />
        </div>
      </div>
    )
  }

  if (type.type === 'map') {
    return (
      <div className="flex flex-col gap-2">
        <span className="text-xs font-mono text-emerald-400 bg-emerald-900/40 px-2 py-0.5 rounded w-fit">
          map
        </span>
        <div className="ml-3 border-l-2 border-edge pl-4 flex flex-col gap-3 py-1">
          <div className="flex items-start gap-2">
            <span className="text-xs font-bold text-slate-500 uppercase mt-1 shrink-0 w-10 text-right">Key</span>
            <TypeDisplay type={type.key} />
          </div>
          <div className="flex items-start gap-2">
            <span className="text-xs font-bold text-slate-500 uppercase mt-1 shrink-0 w-10 text-right">Value</span>
            <TypeDisplay type={type.value} />
          </div>
        </div>
      </div>
    )
  }

  return (
    <pre className="text-detail font-mono text-slate-300 bg-canvas border border-edge rounded p-2 overflow-x-auto">
      {JSON.stringify(type, null, 2)}
    </pre>
  )
}

export function FieldRow({ field }) {
  return (
    <div className="py-4 border-b border-edge last:border-0">
      <div className="grid grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-x-4 gap-y-1 mb-3">
        <span className="text-base font-mono text-slate-500 w-10 text-right shrink-0 tabular-nums">
          {field['field-id'] ?? field.id ?? '—'}
        </span>
        <span className="text-sm font-bold text-ink-bright min-w-0">{field.name}</span>
        {field.required === false ? (
          <span className="text-xs font-bold text-slate-400 uppercase shrink-0">optional</span>
        ) : (
          <span />
        )}
      </div>
      <div className="ml-14">
        <TypeDisplay type={field.type} />
      </div>
    </div>
  )
}

export default function SchemaFieldList({ schema }) {
  if (!schema?.fields?.length) {
    return <p className="text-sm text-slate-400 italic">No fields defined.</p>
  }
  return (
    <div>
      <div className="flex items-center gap-3 pb-1 mb-1 border-b border-edge">
        <span className="text-xs font-bold text-slate-500 uppercase w-6 text-right shrink-0">ID</span>
        <span className="text-xs font-bold text-slate-500 uppercase min-w-30">Name</span>
        <span className="text-xs font-bold text-slate-500 uppercase">Type</span>
      </div>
      {schema.fields.map(f => <FieldRow key={f['field-id'] ?? f.name} field={f} />)}
    </div>
  )
}
