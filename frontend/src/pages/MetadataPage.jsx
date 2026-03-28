import { useOutletContext } from 'react-router-dom'

function Section({ title, children }) {
  return (
    <div className="bg-white rounded-xl border border-slate-100 shadow-sm overflow-hidden">
      <div className="px-5 py-3 border-b border-slate-100 bg-slate-50">
        <h2 className="text-xs font-bold text-slate-500 uppercase tracking-wider">{title}</h2>
      </div>
      <div className="px-5 py-4">{children}</div>
    </div>
  )
}

function KV({ label, value, mono = false }) {
  return (
    <div className="flex flex-col gap-0.5 py-2 border-b border-slate-50 last:border-0">
      <span className="text-[0.65rem] font-bold text-slate-400 uppercase tracking-wider">{label}</span>
      <span className={`text-sm text-[#1e293b] break-all ${mono ? 'font-mono' : ''}`}>
        {value ?? <span className="text-slate-300 italic">—</span>}
      </span>
    </div>
  )
}

function FieldRow({ field }) {
  const isComplex = typeof field.type === 'object' && field.type !== null
  const simpleType = isComplex ? null : field.type

  return (
    <div className="py-2 border-b border-slate-50 last:border-0">
      <div className="flex items-center gap-3">
        <span className="text-[0.65rem] font-mono text-slate-400 w-6 text-right shrink-0">
          {field['field-id'] ?? field.id ?? '—'}
        </span>
        <span className="text-sm font-semibold text-[#1e293b] min-w-[120px]">{field.name}</span>
        {simpleType && (
          <span className="text-xs font-mono text-[#2E86C1] bg-blue-50 px-2 py-0.5 rounded">
            {simpleType}
          </span>
        )}
        {isComplex && (
          <span className="text-xs font-mono text-violet-600 bg-violet-50 px-2 py-0.5 rounded">
            {field.type.type ?? 'complex'}
          </span>
        )}
        {field.required === false && (
          <span className="text-[0.6rem] font-bold text-slate-400 uppercase ml-auto">optional</span>
        )}
      </div>
      {isComplex && (
        <pre className="mt-2 ml-9 text-xs font-mono text-slate-600 bg-slate-50 border border-slate-100 rounded-lg p-3 overflow-x-auto whitespace-pre break-normal">
          {JSON.stringify(field.type, null, 2)}
        </pre>
      )}
    </div>
  )
}

export default function MetadataPage() {
  const { metadata } = useOutletContext()

  if (!metadata) return null

  const currentSchema = metadata.schemas?.find(s => s['schema-id'] === metadata['current-schema-id'])
  const defaultSpec = metadata['partition-specs']?.find(s => s['spec-id'] === metadata['default-spec-id'])
  const defaultOrder = metadata['sort-orders']?.find(s => s['order-id'] === metadata['default-sort-order-id'])
  const properties = metadata.properties ? Object.entries(metadata.properties) : []
  const refs = metadata.refs ? Object.entries(metadata.refs) : []

  const lastUpdated = metadata['last-updated-ms']
    ? new Date(metadata['last-updated-ms']).toLocaleString()
    : null

  return (
    <div className="flex-1 overflow-y-auto bg-[#f8fafc]">
      <div className="max-w-4xl mx-auto px-8 py-8 flex flex-col gap-6">

        {/* Overview */}
        <Section title="Overview">
          <div className="grid grid-cols-2 gap-x-8">
            <KV label="Table Name" value={metadata['table-name']} mono />
            <KV label="Table UUID" value={metadata['table-uuid']} mono />
            <KV label="Location" value={metadata.location} mono />
            <KV label="Format Version" value={metadata['format-version']} />
            <KV label="Last Updated" value={lastUpdated} />
            <KV label="Current Snapshot" value={metadata['current-snapshot-id']} mono />
          </div>
        </Section>

        {/* Current Schema */}
        {currentSchema && (
          <Section title={`Current Schema — ID ${currentSchema['schema-id']}`}>
            {currentSchema.fields?.length > 0 ? (
              <div>
                <div className="flex items-center gap-3 pb-1 mb-1 border-b border-slate-100">
                  <span className="text-[0.6rem] font-bold text-slate-400 uppercase w-6 text-right shrink-0">#</span>
                  <span className="text-[0.6rem] font-bold text-slate-400 uppercase min-w-[120px]">Name</span>
                  <span className="text-[0.6rem] font-bold text-slate-400 uppercase">Type</span>
                </div>
                {currentSchema.fields.map(f => <FieldRow key={f['field-id'] ?? f.name} field={f} />)}
              </div>
            ) : (
              <p className="text-sm text-slate-400 italic">No fields defined.</p>
            )}
          </Section>
        )}

        {/* Partition Spec */}
        {defaultSpec && (
          <Section title={`Partition Spec — ID ${defaultSpec['spec-id']}`}>
            {defaultSpec.fields?.length > 0 ? (
              <div>
                <div className="flex items-center gap-3 pb-1 mb-1 border-b border-slate-100">
                  <span className="text-[0.6rem] font-bold text-slate-400 uppercase w-6 text-right shrink-0">#</span>
                  <span className="text-[0.6rem] font-bold text-slate-400 uppercase min-w-[120px]">Name</span>
                  <span className="text-[0.6rem] font-bold text-slate-400 uppercase">Transform</span>
                </div>
                {defaultSpec.fields.map((f, i) => (
                  <div key={i} className="flex items-center gap-3 py-2 border-b border-slate-50 last:border-0">
                    <span className="text-[0.65rem] font-mono text-slate-400 w-6 text-right shrink-0">{f['field-id'] ?? i}</span>
                    <span className="text-sm font-semibold text-[#1e293b] min-w-[120px]">{f.name}</span>
                    <span className="text-xs font-mono text-[#2E86C1] bg-blue-50 px-2 py-0.5 rounded">{f.transform}</span>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-sm text-slate-400 italic">Unpartitioned.</p>
            )}
          </Section>
        )}

        {/* Sort Order */}
        {defaultOrder && (
          <Section title={`Sort Order — ID ${defaultOrder['order-id']}`}>
            {defaultOrder.fields?.length > 0 ? (
              <div>
                <div className="grid grid-cols-[1fr_120px_120px] pb-1 mb-1 border-b border-slate-100">
                  <span className="text-[0.6rem] font-bold text-slate-400 uppercase">Transform</span>
                  <span className="text-[0.6rem] font-bold text-slate-400 uppercase">Direction</span>
                  <span className="text-[0.6rem] font-bold text-slate-400 uppercase">Nulls</span>
                </div>
                {defaultOrder.fields.map((f, i) => (
                  <div key={i} className="grid grid-cols-[1fr_120px_120px] py-2 border-b border-slate-50 last:border-0 items-center">
                    <span className="text-sm font-mono text-[#2E86C1]">{typeof f.transform === 'object' ? JSON.stringify(f.transform) : f.transform}</span>
                    <span className="text-sm text-[#1e293b]">{f.direction}</span>
                    <span className="text-sm text-slate-400">{f['null-order']}</span>
                  </div>
                ))}
              </div>
            ) : (
              <p className="text-sm text-slate-400 italic">Unsorted.</p>
            )}
          </Section>
        )}

        {/* Refs */}
        {refs.length > 0 && (
          <Section title="Refs">
            <div className="flex flex-col">
              {refs.map(([name, ref]) => (
                <div key={name} className="flex items-center gap-3 py-2 border-b border-slate-50 last:border-0">
                  <span className="text-sm font-semibold text-[#1e293b] min-w-[100px]">{name}</span>
                  <span className="text-[0.6rem] font-bold uppercase px-2 py-0.5 rounded bg-slate-100 text-slate-500">{ref.type}</span>
                  <span className="text-xs font-mono text-slate-500 ml-auto">{ref['snapshot-id']}</span>
                </div>
              ))}
            </div>
          </Section>
        )}

        {/* Properties */}
        {properties.length > 0 && (
          <Section title="Properties">
            <div className="flex flex-col">
              {properties.map(([k, v]) => (
                <div key={k} className="flex items-start gap-4 py-2 border-b border-slate-50 last:border-0">
                  <span className="text-sm font-mono text-[#2E86C1] min-w-[180px] shrink-0">{k}</span>
                  <span className="text-sm text-slate-600 break-all">{String(v)}</span>
                </div>
              ))}
            </div>
          </Section>
        )}

      </div>
    </div>
  )
}
