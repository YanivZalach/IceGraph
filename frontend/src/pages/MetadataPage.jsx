import { useEffect, useRef, useState } from 'react'
import { useOutletContext } from 'react-router-dom'
import JSONbig from 'json-bigint'
import CopyableValue from '../components/CopyableValue'
import { DEFAULT_COLLAPSE_LINES, PANEL_COLLAPSE_TOGGLE_CLASS } from '../components/PanelContent'
import PartitionSpecList from '../components/PartitionSpecList'
import SchemaFieldList from '../components/SchemaFieldList'
import SortOrderList from '../components/SortOrderList'
import { FileType } from '../graphConstants'
import {
  UI_BODY_MUTED_CLASS,
  UI_FIELD_LABEL_CLASS,
  UI_METADATA_SECTION_TITLE_CLASS,
  UI_MONO_MUTED_CLASS,
} from '../uiTypography'
import { formatLocaleDateTime, parseUtcDate } from '../utils/dateUtils'
import { highlightJson } from '../utils/jsonHighlight'
import { bindMouseScrollHandoff } from '../utils/smoothScroll'

function Section({ title, children }) {
  return (
    <div className="bg-surface rounded-xl border border-edge shadow-sm overflow-hidden">
      <div className="px-5 py-3 border-b border-edge bg-surface-deep">
        <h2 className={UI_METADATA_SECTION_TITLE_CLASS}>{title}</h2>
      </div>
      <div className="px-5 py-4">{children}</div>
    </div>
  )
}

const JSONbigString = JSONbig({ storeAsString: true })

function parseJsonMaybe(value) {
  if (typeof value !== 'string') return null
  const trimmed = value.trim()
  if (!trimmed.startsWith('{') && !trimmed.startsWith('[')) return null
  try {
    const parsed = JSONbigString.parse(trimmed)
    return parsed && typeof parsed === 'object' ? parsed : null
  } catch {
    return null
  }
}

function JsonPropertyValue({ data }) {
  const [isCollapsed, setIsCollapsed] = useState(true)
  const fullText = JSON.stringify(data, null, 2)
  const lines = fullText.split('\n')
  const isCollapsible = lines.length > DEFAULT_COLLAPSE_LINES
  const displayText = isCollapsible && isCollapsed
    ? lines.slice(0, DEFAULT_COLLAPSE_LINES).join('\n')
    : fullText

  return (
    <div className="flex-1 min-w-0 flex flex-col items-start gap-1">
      {isCollapsible && (
        <button
          type="button"
          onClick={() => setIsCollapsed(p => !p)}
          className={PANEL_COLLAPSE_TOGGLE_CLASS}
        >
          {isCollapsed ? `▼ (${lines.length} lines)` : '▲'}
        </button>
      )}
      <pre className={`${UI_BODY_MUTED_CLASS} break-all whitespace-pre-wrap`}>
        {highlightJson(displayText)}
        {isCollapsible && isCollapsed && '\n…'}
      </pre>
    </div>
  )
}

function KV({ label, value, mono = false }) {
  return (
    <div className="flex flex-col gap-0.5 py-2 border-b border-edge last:border-0 [&:nth-last-child(2)]:border-0">
      <span className={UI_FIELD_LABEL_CLASS}>{label}</span>
      <CopyableValue value={value} mono={mono} />
    </div>
  )
}

export default function MetadataPage() {
  const { metadata, nodes } = useOutletContext()
  const [copied, setCopied] = useState(false)
  const scrollTargetRef = useRef(0)
  const rafRef = useRef(null)

  useEffect(() => {
    scrollTargetRef.current = window.scrollY
    const animate = () => {
      const diff = scrollTargetRef.current - window.scrollY
      if (Math.abs(diff) < 0.5) { window.scrollTo(0, scrollTargetRef.current); rafRef.current = null; return }
      window.scrollBy(0, diff * 0.14)
      rafRef.current = requestAnimationFrame(animate)
    }
    const scroll = (delta) => {
      const max = document.documentElement.scrollHeight - window.innerHeight
      scrollTargetRef.current = Math.max(0, Math.min(scrollTargetRef.current + delta, max))
      if (!rafRef.current) rafRef.current = requestAnimationFrame(animate)
    }
    const handleKey = (e) => {
      if (['INPUT', 'TEXTAREA', 'SELECT'].includes(e.target.tagName) || e.target.isContentEditable) return
      if (e.key === 'j') { e.preventDefault(); scroll(80) }
      else if (e.key === 'k') { e.preventDefault(); scroll(-80) }
    }
    window.addEventListener('keydown', handleKey)
    const unbindHandoff = bindMouseScrollHandoff(() => window, scrollTargetRef, rafRef)
    return () => { window.removeEventListener('keydown', handleKey); unbindHandoff(); if (rafRef.current) cancelAnimationFrame(rafRef.current) }
  }, [])

  if (!metadata) return null

  const mainMetadataPath = (() => {
    const node = (nodes || []).find(n => n.type === FileType.MAIN_METADATA);
    if (!node?.details) return null;

    return node.details.file_path ?? null;
  })();

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
    ? formatLocaleDateTime((new Date(metadata['last-updated-ms'])))
    : null

  const currentSnapshotNode = (nodes || [])
    .find(n => n.type === FileType.SNAPSHOT && n.details &&
      String(n.details.snapshot_id) === String(metadata['current-snapshot-id']))

  const getStat = (key) => currentSnapshotNode?.details?.summary?.[key] ?? null

  return (
    <div className="flex-1 overflow-y-auto bg-canvas">
      <div className="max-w-4xl mx-auto px-8 py-8 flex flex-col gap-6">

        <div className="flex items-center gap-3 flex-wrap">
          <button
            onClick={handleCopy}
            className="text-sm font-medium px-4 py-2 rounded-lg border border-edge bg-surface text-ink hover:border-accent hover:text-accent transition shadow-sm"
          >
            {copied ? '✓ Copied!' : 'Copy Metadata JSON'}
          </button>
          <div className="group relative">
            <div className="w-4 h-4 rounded-full bg-accent text-white text-tiny font-black flex items-center justify-center cursor-help hover:bg-accent-dark transition select-none">
              i
            </div>
            <div className="absolute left-1/2 -translate-x-1/2 top-full mt-2 w-72 bg-surface text-slate-300 text-detail p-3 rounded-lg shadow-xl opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-50 leading-relaxed">
              <strong className="text-accent block mb-1 uppercase tracking-wide text-base">Partial Metadata</strong>
              The following fields are stripped/altered by the backend due to size:
              <ul className="mt-1.5 flex flex-col gap-0.5">
                {['metadata-log', 'snapshot-log', 'snapshots', 'statistics'].map(f => (
                  <li key={f} className="font-mono text-accent">· {f}</li>
                ))}
              </ul>
              <div className="absolute bottom-full left-1/2 -translate-x-1/2 border-8 border-transparent border-b-surface" />
            </div>
          </div>
          {mainMetadataPath && (
            <div className="flex items-center gap-2 px-3 py-2 rounded-lg bg-surface border border-edge">
              <span className={`${UI_FIELD_LABEL_CLASS} shrink-0`}>Path</span>
              <span className="text-xs font-mono text-slate-300 break-all">{mainMetadataPath}</span>
            </div>
          )}
        </div>

        <Section title="Overview">
          <div className="grid grid-cols-2 gap-x-8">
            <KV label="Table Name" value={metadata['table-name']} mono />
            <KV label="Table UUID" value={metadata['table-uuid']} mono />
            <KV label="Location" value={metadata.location} mono />
            <KV label="Format Version" value={metadata['format-version']} />
            <KV label="Last Updated" value={lastUpdated} />
            <KV label="Current Snapshot" value={metadata['current-snapshot-id']} mono />
            {currentSnapshotNode && <>
              <KV label="Snapshot Timestamp" value={formatLocaleDateTime(parseUtcDate(currentSnapshotNode.details.timestamp))} />
              <KV label="Total Records" value={getStat('total-records')} />
              <KV label="Data Files" value={getStat('total-data-files')} />
              <KV label="Table Size" value={getStat('total-files-size')} />
              <KV label="Delete Files" value={getStat('total-delete-files')} />
              <KV label="Position Deletes" value={getStat('total-position-deletes')} />
              <KV label="Equality Deletes" value={getStat('total-equality-deletes')} />
            </>}
          </div>
        </Section>

        {defaultSpec && (
          <Section title={`Partition Spec — ID ${defaultSpec['spec-id']}`}>
            <PartitionSpecList spec={defaultSpec} />
          </Section>
        )}

        {defaultOrder && (
          <Section title={`Sort Order — ID ${defaultOrder['order-id']}`}>
            <SortOrderList order={defaultOrder} />
          </Section>
        )}

        {refs.length > 0 && (
          <Section title="Refs">
            <div className="flex flex-col">
              {refs.map(([name, ref]) => (
                <div key={name} className="flex items-center gap-3 py-2 border-b border-edge last:border-0">
                  <span className="text-sm font-semibold text-ink min-w-25">{name}</span>
                  <span className="text-xs font-bold uppercase px-2 py-0.5 rounded bg-edge text-slate-400">{ref.type}</span>
                  <span className={`${UI_MONO_MUTED_CLASS} ml-auto`}>{ref['snapshot-id']}</span>
                </div>
              ))}
            </div>
          </Section>
        )}

        {properties.length > 0 && (
          <Section title="Properties">
            <div className="flex flex-col">
              {properties.map(([k, v]) => {
                const parsedJson = parseJsonMaybe(v)
                return (
                  <div key={k} className="flex items-start gap-4 py-2 border-b border-edge last:border-0">
                    <span className="text-sm font-mono text-accent min-w-45 shrink-0">{k}</span>
                    {parsedJson ? (
                      <JsonPropertyValue data={parsedJson} />
                    ) : (
                      <span className={`${UI_BODY_MUTED_CLASS} break-all`}>{String(v)}</span>
                    )}
                  </div>
                )
              })}
            </div>
          </Section>
        )}

        {currentSchema && (
          <Section title={`Current Schema — ID ${currentSchema['schema-id']}`}>
            <SchemaFieldList schema={currentSchema} />
          </Section>
        )}

      </div>
    </div>
  )
}
