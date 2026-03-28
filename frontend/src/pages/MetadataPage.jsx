import { useState } from 'react'
import { useOutletContext } from 'react-router-dom'

function Section({ title, children }) {
  return (
    <div className="bg-[#1a202c] rounded-xl border border-[#2d3748] shadow-sm overflow-hidden">
      <div className="px-5 py-3 border-b border-[#2d3748] bg-[#151c2c]">
        <h2 className="text-xs font-bold text-slate-400 uppercase tracking-wider">{title}</h2>
      </div>
      <div className="px-5 py-4">{children}</div>
    </div>
  )
}

function KV({ label, value, mono = false }) {
  return (
    <div className="flex flex-col gap-0.5 py-2 border-b border-[#2d3748] last:border-0 [&:nth-last-child(2)]:border-0">
      <span className="text-[0.65rem] font-bold text-slate-500 uppercase tracking-wider">{label}</span>
      <span className={`text-sm text-[#e2e8f0] break-all ${mono ? 'font-mono' : ''}`}>
        {value ?? <span className="text-slate-600 italic">—</span>}
      </span>
    </div>
  )
}

function FieldRow({ field }) {
  const isComplex = typeof field.type === 'object' && field.type !== null
  const simpleType = isComplex ? null : field.type

  return (
    <div className="py-2 border-b border-[#2d3748] last:border-0">
      <div className="flex items-center gap-3">
        <span className="text-[0.65rem] font-mono text-slate-500 w-6 text-right shrink-0">
          {field['field-id'] ?? field.id ?? '—'}
        </span>
        <span className="text-sm font-semibold text-[#e2e8f0] min-w-[120px]">{field.name}</span>
        {simpleType && (
          <span className="text-xs font-mono text-[#2E86C1] bg-[#1e3a5f] px-2 py-0.5 rounded">
            {simpleType}
          </span>
        )}
        {isComplex && (
          <span className="text-xs font-mono text-violet-400 bg-violet-900/30 px-2 py-0.5 rounded">
            {field.type.type ?? 'complex'}
          </span>
        )}
        {field.required === false && (
          <span className="text-[0.6rem] font-bold text-slate-500 uppercase ml-auto">optional</span>
        )}
      </div>
      {isComplex && (
        <pre className="mt-2 ml-9 text-xs font-mono text-slate-300 bg-[#0d1117] border border-[#2d3748] rounded-lg p-3 overflow-x-auto whitespace-pre break-normal">
          {JSON.stringify(field.type, null, 2)}
        </pre>
      )}
    </div>
  )
}

export default function MetadataPage() {
  const { metadata } = useOutletContext()
  const [copied, setCopied] = useState(false)

  if (!metadata) return null

  const handleCopy = () => {
    navigator.clipboard.writeText(JSON.stringify(metadata, null, 2))
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  const currentSchema = metadata.schemas?.find(s => s['schema-id'] === metadata['current-schema-id'])
  const defaultSpec = metadata['partition-specs']?.find(s => s['spec-id'] === metadata['default-spec-id'])
  const defaultOrder = metadata['sort-orders']?.find(s => s['order-id'] === metadata['default-sort-order-id'])
  const properties = metadata.properties ? Object.entries(metadata.properties) : []
  const refs = metadata.refs ? Object.entries(metadata.refs) : []

  const lastUpdated = metadata['last-updated-ms']
    ? new Date(metadata['last-updated-ms']).toLocaleString()
    : null

  return (
    <div className="flex-1 overflow-y-auto bg-[#0d1117]">
      <div className="max-w-4xl mx-auto px-8 py-8 flex flex-col gap-6">

        {/* Copy button */}
        <div className="flex items-center gap-2">
          <button
            onClick={handleCopy}
            className="text-sm font-medium px-4 py-2 rounded-lg border border-[#2d3748] bg-[#1a202c] text-[#e2e8f0] hover:border-[#2E86C1] hover:text-[#2E86C1] transition shadow-sm"
          >
            {copied ? '✓ Copied!' : 'Copy Metadata JSON'}
          </button>
          <div className="group relative">
            <div className="w-4 h-4 rounded-full bg-[#2E86C1] text-white text-[10px] font-black flex items-center justify-center cursor-help hover:bg-[#2471a3] transition select-none">
              i
            </div>
            <div className="absolute left-1/2 -translate-x-1/2 top-full mt-2 w-72 bg-[#1a202c] text-slate-300 text-[0.7rem] p-3 rounded-lg shadow-xl opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-50 leading-relaxed">
              <strong className="text-[#2E86C1] block mb-1 uppercase tracking-wide text-[0.65rem]">Partial Metadata</strong>
              The following fields are stripped by the backend due to size:
              <ul className="mt-1.5 flex flex-col gap-0.5">
                {['metadata-log', 'snapshot-log', 'snapshots', 'statistics'].map(f => (
                  <li key={f} className="font-mono text-[#2E86C1]">· {f}</li>
                ))}
              </ul>
              <div className="absolute bottom-full left-1/2 -translate-x-1/2 border-8 border-transparent border-b-[#1a202c]" />
            </div>
          </div>
        </div>

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
                <div className="flex items-center gap-3 pb-1 mb-1 border-b border-[#2d3748]">
                  <span className="text-[0.6rem] font-bold text-slate-500 uppercase w-6 text-right shrink-0">#</span>
                  <span className="text-[0.6rem] font-bold text-slate-500 uppercase min-w-[120px]">Name</span>
                  <span className="text-[0.6rem] font-bold text-slate-500 uppercase">Type</span>
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
                <div className="flex items-center gap-3 pb-1 mb-1 border-b border-[#2d3748]">
                  <span className="text-[0.6rem] font-bold text-slate-500 uppercase w-6 text-right shrink-0">#</span>
                  <span className="text-[0.6rem] font-bold text-slate-500 uppercase min-w-[120px]">Name</span>
                  <span className="text-[0.6rem] font-bold text-slate-500 uppercase">Transform</span>
                </div>
                {defaultSpec.fields.map((f, i) => (
                  <div key={i} className="flex items-center gap-3 py-2 border-b border-[#2d3748] last:border-0">
                    <span className="text-[0.65rem] font-mono text-slate-500 w-6 text-right shrink-0">{f['field-id'] ?? i}</span>
                    <span className="text-sm font-semibold text-[#e2e8f0] min-w-[120px]">{f.name}</span>
                    <span className="text-xs font-mono text-[#2E86C1] bg-[#1e3a5f] px-2 py-0.5 rounded">{f.transform}</span>
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
                <div className="grid grid-cols-[1fr_120px_120px] pb-1 mb-1 border-b border-[#2d3748]">
                  <span className="text-[0.6rem] font-bold text-slate-500 uppercase">Transform</span>
                  <span className="text-[0.6rem] font-bold text-slate-500 uppercase">Direction</span>
                  <span className="text-[0.6rem] font-bold text-slate-500 uppercase">Nulls</span>
                </div>
                {defaultOrder.fields.map((f, i) => (
                  <div key={i} className="grid grid-cols-[1fr_120px_120px] py-2 border-b border-[#2d3748] last:border-0 items-center">
                    <span className="text-sm font-mono text-[#2E86C1]">{typeof f.transform === 'object' ? JSON.stringify(f.transform) : f.transform}</span>
                    <span className="text-sm text-[#e2e8f0]">{f.direction}</span>
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
                <div key={name} className="flex items-center gap-3 py-2 border-b border-[#2d3748] last:border-0">
                  <span className="text-sm font-semibold text-[#e2e8f0] min-w-[100px]">{name}</span>
                  <span className="text-[0.6rem] font-bold uppercase px-2 py-0.5 rounded bg-[#2d3748] text-slate-400">{ref.type}</span>
                  <span className="text-xs font-mono text-slate-400 ml-auto">{ref['snapshot-id']}</span>
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
                <div key={k} className="flex items-start gap-4 py-2 border-b border-[#2d3748] last:border-0">
                  <span className="text-sm font-mono text-[#2E86C1] min-w-[180px] shrink-0">{k}</span>
                  <span className="text-sm text-slate-400 break-all">{String(v)}</span>
                </div>
              ))}
            </div>
          </Section>
        )}

      </div>
    </div>
  )
}
