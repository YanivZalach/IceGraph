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

function SchemaFieldHeader() {
  return (
    <div className="flex items-center gap-3 pb-1 mb-1 border-b border-edge">
      <span className="text-xs font-bold text-slate-500 uppercase w-6 text-right shrink-0">ID</span>
      <span className="text-xs font-bold text-slate-500 uppercase min-w-30">Name</span>
      <span className="text-xs font-bold text-slate-500 uppercase">Type</span>
    </div>
  )
}

export function FieldRow({ field, status, prevField }) {
  const dim = status === 'unchanged' ? 'opacity-50' : ''
  const bg =
    status === 'added' ? 'bg-green-900/20' :
    status === 'removed' ? 'bg-red-900/20' :
    status === 'changed' ? 'bg-amber-900/20' : ''
  const idColor =
    status === 'added' ? 'text-green-400' :
    status === 'removed' ? 'text-red-400' :
    status === 'changed' ? 'text-amber-400' : 'text-slate-500'
  const nameColor =
    status === 'added' ? 'text-green-300' :
    status === 'removed' ? 'text-red-300 line-through' :
    status === 'changed' ? 'text-amber-300' : 'text-ink-bright'
  const marker = status === 'added' ? '+' : status === 'removed' ? '−' : status === 'changed' ? '~' : null
  const typeChanged = status === 'changed' && JSON.stringify(prevField.type) !== JSON.stringify(field.type)

  return (
    <div className={`py-4 border-b border-edge last:border-0 ${bg} ${dim}`}>
      <div className="grid grid-cols-[auto_minmax(0,1fr)_auto] items-center gap-x-4 gap-y-1 mb-3">
        <span className={`text-base font-mono w-10 text-right shrink-0 tabular-nums ${idColor}`}>
          {marker ?? (field['field-id'] ?? field.id ?? '—')}
        </span>
        <span className={`text-sm font-bold min-w-0 ${nameColor}`}>
          {field.name}
          {status === 'changed' && prevField.name !== field.name && (
            <span className="text-amber-600 line-through ml-2 font-normal">{prevField.name}</span>
          )}
        </span>
        {field.required === false ? (
          <span className="text-xs font-bold text-slate-400 uppercase shrink-0">optional</span>
        ) : (
          <span />
        )}
      </div>
      <div className="ml-14">
        {typeChanged ? (
          <div className="flex items-center gap-3 text-xs font-mono">
            <TypeDisplay type={prevField.type} />
            <span className="text-slate-500">→</span>
            <TypeDisplay type={field.type} />
          </div>
        ) : (
          <TypeDisplay type={field.type} />
        )}
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
      <SchemaFieldHeader />
      {schema.fields.map(f => <FieldRow key={f['field-id'] ?? f.name} field={f} />)}
    </div>
  )
}

export function SchemaDiffView({ diff }) {
  return (
    <div>
      <SchemaFieldHeader />
      {diff.map((entry, i) => (
        <FieldRow
          key={i}
          field={entry.status === 'changed' ? entry.curr : entry.field}
          prevField={entry.status === 'changed' ? entry.prev : undefined}
          status={entry.status}
        />
      ))}
    </div>
  )
}
