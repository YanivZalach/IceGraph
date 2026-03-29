import { useEffect, useMemo, useRef, useState } from 'react'
import { useOutletContext } from 'react-router-dom'
import { FileType, UI_NEWLINE, UI_SECTION_NEWLINE } from '../graphConstants'

const COLOR_A = '#1964B9'
const COLOR_B = '#6437D2'
const COLOR_INIT = '#4a5568'

const DIFF_KEYS = [
  'current_schema_id',
  'partition_spec_id',
  'sort_order_id',
  'table_format_version',
  'refs',
  'properties',
]

function parseDetails(details) {
  if (!details) return {}
  const sections = details.split(UI_SECTION_NEWLINE)
  const result = {}
  for (let i = 1; i < sections.length; i++) {
    const raw = sections[i].trim()
    const idx = raw.indexOf(':')
    if (idx === -1) continue
    const key = raw.substring(0, idx).trim()
    const val = raw.substring(idx + 1).trim().replace(new RegExp(UI_NEWLINE, 'g'), '\n')
    result[key] = val === 'None' || val === 'null' || val === '' ? null : val
  }
  return result
}

function parseSummary(summary) {
  if (!summary) return []
  return summary
    .split('\n')
    .map(line => {
      const idx = line.indexOf(':')
      if (idx === -1) return null
      return { key: line.substring(0, idx).trim(), value: line.substring(idx + 1).trim() }
    })
    .filter(Boolean)
}

function formatTs(tsStr) {
  if (!tsStr) return null
  try {
    const d = new Date(tsStr)
    if (isNaN(d.getTime())) return null
    const ms = String(d.getMilliseconds()).padStart(3, '0')
    const time = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) + `.${ms}`
    return {
      date: d.toLocaleDateString(),
      time,
      full: `${d.toLocaleDateString()} ${time}`,
    }
  } catch {
    return null
  }
}

function parseProperties(propertiesStr) {
  if (!propertiesStr) return []
  return propertiesStr
    .split('\n')
    .map(line => {
      const match = line.match(/^"([^"]+)":\s*"([^"]*)"$/)
      if (!match) return null
      return { key: match[1], value: match[2] }
    })
    .filter(Boolean)
}

function shortFileName(filePath) {
  if (!filePath) return '—'
  const base = filePath.split('/').pop() || filePath
  return base
}

function formatDuration(tsA, tsB) {
  const diff = Math.abs(new Date(tsA) - new Date(tsB))
  if (diff >= 86400000) return `${Math.round(diff / 86400000)}d`
  if (diff >= 3600000) return `${Math.round(diff / 3600000)}h`
  if (diff >= 60000) return `${Math.round(diff / 60000)}m`
  return `${Math.round(diff / 1000)}s`
}

function colorFor(type) {
  if (type === 'A') return COLOR_A
  if (type === 'B') return COLOR_B
  return COLOR_INIT
}

function labelFor(type) {
  if (type === 'A') return 'Write'
  if (type === 'B') return 'Metadata Op'
  return 'Init'
}

function DiffRow({ label, before, after }) {
  return (
    <div className="flex flex-col gap-1.5">
      <span className="text-[0.65rem] font-bold text-slate-500 uppercase tracking-wider">
        {label.replace(/_/g, ' ')}
      </span>
      <div className="grid grid-cols-2 gap-2">
        <div>
          <div className="text-[0.55rem] text-red-400 font-bold uppercase mb-0.5">Before</div>
          <pre className="text-xs bg-red-950/30 border border-red-900/40 text-red-300 rounded p-2 overflow-x-auto whitespace-pre-wrap break-all min-h-[32px]">
            {before ?? '—'}
          </pre>
        </div>
        <div>
          <div className="text-[0.55rem] text-green-400 font-bold uppercase mb-0.5">After</div>
          <pre className="text-xs bg-green-950/30 border border-green-900/40 text-green-300 rounded p-2 overflow-x-auto whitespace-pre-wrap break-all min-h-[32px]">
            {after ?? '—'}
          </pre>
        </div>
      </div>
    </div>
  )
}

export default function TimelinePage() {
  const { nodes } = useOutletContext()
  const [selected, setSelected] = useState(null)

  useEffect(() => {
    const handleKey = (e) => { if (e.key === 'Escape') setSelected(null) }
    window.addEventListener('keydown', handleKey)
    return () => window.removeEventListener('keydown', handleKey)
  }, [])

  const { events, snapshotMap } = useMemo(() => {
    const allNodes = nodes.get()

    const metaNodes = allNodes
      .filter(n => n.type === FileType.METADATA || n.type === FileType.MAIN_METADATA)
      .map(n => ({ details: parseDetails(n.details) }))
      .filter(n => n.details.timestamp)
      .sort((a, b) => new Date(b.details.timestamp) - new Date(a.details.timestamp))

    const snapMap = {}
    allNodes
      .filter(n => n.type === FileType.SNAPSHOT)
      .forEach(n => {
        const d = parseDetails(n.details)
        if (d.snapshot_id) snapMap[d.snapshot_id] = d
      })

    const timeline = metaNodes.map(({ details }, i) => {
      const prev = i < metaNodes.length - 1 ? metaNodes[i + 1].details : null
      const type = !prev
        ? 'init'
        : details.snapshot_id !== prev.snapshot_id
          ? 'A'
          : 'B'

      const diff =
        type === 'B' && prev
          ? DIFF_KEYS
            .filter(k => details[k] !== prev[k])
            .map(k => ({ key: k, before: prev[k], after: details[k] }))
          : []

      return { details, type, diff, snapshotId: details.snapshot_id }
    })

    return { events: timeline, snapshotMap: snapMap }
  }, [nodes])

  if (events.length === 0) {
    return (
      <div className="flex-1 flex items-center justify-center bg-[#0d1117]">
        <p className="text-slate-500 text-sm italic">No metadata history available.</p>
      </div>
    )
  }

  const selectedSnap = selected ? snapshotMap[selected.snapshotId] : null

  const scrollRef = useRef(null)
  const targetScrollRef = useRef(0)
  const rafRef = useRef(null)
  useEffect(() => {
    const el = scrollRef.current
    if (!el) return
    targetScrollRef.current = el.scrollLeft
    const onWheel = (e) => {
      if (e.deltaY === 0) return
      e.preventDefault()
      targetScrollRef.current = Math.max(
        0,
        Math.min(targetScrollRef.current + e.deltaY, el.scrollWidth - el.clientWidth)
      )
      if (rafRef.current) return
      const animate = () => {
        const diff = targetScrollRef.current - el.scrollLeft
        if (Math.abs(diff) < 0.5) {
          el.scrollLeft = targetScrollRef.current
          rafRef.current = null
          return
        }
        el.scrollLeft += diff * 0.12
        rafRef.current = requestAnimationFrame(animate)
      }
      rafRef.current = requestAnimationFrame(animate)
    }
    el.addEventListener('wheel', onWheel, { passive: false })
    return () => {
      el.removeEventListener('wheel', onWheel)
      if (rafRef.current) cancelAnimationFrame(rafRef.current)
    }
  }, [])

  return (
    <div className="flex-1 flex flex-col bg-[#0d1117] overflow-hidden">

      <div className="shrink-0 px-8 pt-5 flex items-center gap-5">
        {[['A', 'Write'], ['B', 'Metadata Op'], ['init', 'Initial State']].map(([type, lbl]) => (
          <div key={type} className="flex items-center gap-1.5">
            <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: colorFor(type) }} />
            <span className="text-xs text-slate-400">{lbl}</span>
          </div>
        ))}
      </div>

      <div ref={scrollRef} className="flex-1 flex items-center overflow-x-auto">
        <div className="flex items-start min-w-max px-16 py-12">
          {events.map((event, i) => {
            const ts = formatTs(event.details.timestamp)
            const fileName = shortFileName(event.details.file_path)
            return (
              <div key={i} className="flex items-center">
                {i > 0 && (
                  <div className="flex flex-col items-center shrink-0">
                    <div className="text-[0.6rem] text-slate-500 mb-1">
                      {formatDuration(events[i - 1].details.timestamp, events[i].details.timestamp)}
                    </div>
                    <div className="flex items-center">
                      <div style={{
                        width: 0, height: 0,
                        borderTop: '4px solid transparent',
                        borderBottom: '4px solid transparent',
                        borderRight: '7px solid #2d3748',
                      }} />
                      <div className="w-14 h-px bg-[#2d3748]" />
                    </div>
                  </div>
                )}
                <div
                  className="flex flex-col items-center gap-2 cursor-pointer group select-none"
                  onClick={() => setSelected(event)}
                >
                  <div className="text-center max-w-[160px]">
                    <div
                      className="text-xs font-mono font-bold leading-tight break-all"
                      style={{ color: colorFor(event.type) }}
                      title={event.details.file_path ?? ''}
                    >
                      {fileName}
                    </div>
                  </div>
                  <div
                    className="w-11 h-11 rounded-full border-2 shadow-lg transition-transform group-hover:scale-110 shrink-0"
                    style={{ backgroundColor: colorFor(event.type), borderColor: colorFor(event.type) }}
                  />
                  <div className="text-center max-w-[160px]">
                    {ts && (
                      <>
                        <div className="text-[0.7rem] text-slate-500 leading-tight">{ts.date}</div>
                        <div className="text-[0.7rem] text-slate-600 leading-tight">{ts.time}</div>
                      </>
                    )}
                  </div>
                </div>
              </div>
            )
          })}
        </div>
      </div>

      {selected && (
        <div
          className="fixed inset-0 z-[9999] bg-black/60 flex items-center justify-center font-sans"
          onClick={() => setSelected(null)}
        >
          <div
            className="w-[50vw] min-w-[360px] max-w-[680px] bg-[#1a202c] rounded-xl shadow-2xl border border-[#2d3748] max-h-[80vh] flex flex-col overflow-hidden"
            style={{ borderLeft: `4px solid ${colorFor(selected.type)}` }}
            onClick={e => e.stopPropagation()}
          >
            <div className="flex items-center justify-between px-6 py-4 border-b border-[#2d3748] shrink-0">
              <div className="min-w-0 pr-4">
                <div className="font-bold text-sm" style={{ color: colorFor(selected.type) }}>
                  {labelFor(selected.type)}
                </div>
                <div className="text-xs font-mono text-[#e2e8f0] mt-0.5 break-all">
                  {shortFileName(selected.details.file_path)}
                </div>
                {formatTs(selected.details.timestamp) && (
                  <div className="text-xs text-slate-500 mt-0.5">
                    {formatTs(selected.details.timestamp).full}
                  </div>
                )}
              </div>
              <button
                className="w-7 h-7 rounded-full bg-[#2d3748] text-slate-400 flex items-center justify-center text-sm cursor-pointer hover:bg-[#3d4a5c] hover:text-slate-200 transition"
                onClick={() => setSelected(null)}
              >
                ✕
              </button>
            </div>

            <div className="overflow-y-auto px-6 py-5 flex flex-col gap-5">

              {selected.type === 'A' && (
                <>
                  <div className="flex justify-between items-center py-1.5 border-b border-[#2d3748]">
                    <span className="text-xs text-slate-400">Snapshot ID</span>
                    <span className="text-xs font-mono text-[#e2e8f0]">{selected.snapshotId ?? '—'}</span>
                  </div>
                  {selectedSnap && (
                    <>
                      <div>
                        <div className="text-[0.65rem] font-bold text-slate-500 uppercase tracking-wider mb-1.5">Operation</div>
                        <span className="text-xs font-mono text-[#2E86C1] bg-[#1e3a5f] px-2.5 py-1 rounded">
                          {selectedSnap.operation ?? '—'}
                        </span>
                      </div>
                      {parseSummary(selectedSnap.summary).length > 0 && (
                        <div>
                          <div className="text-[0.65rem] font-bold text-slate-500 uppercase tracking-wider mb-2">Summary</div>
                          <div className="flex flex-col">
                            {parseSummary(selectedSnap.summary).map(({ key, value }) => (
                              <div key={key} className="flex justify-between items-center py-1.5 border-b border-[#2d3748] last:border-0">
                                <span className="text-xs text-slate-400">{key}</span>
                                <span className="text-xs font-mono text-[#e2e8f0]">{value}</span>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </>
                  )}
                </>
              )}

              {selected.type === 'B' && (
                selected.diff.length > 0
                  ? selected.diff.map(({ key, before, after }) => (
                    <DiffRow key={key} label={key} before={before} after={after} />
                  ))
                  : <p className="text-sm text-slate-400 italic">No tracked field changes detected.</p>
              )}

              {selected.type === 'init' && (
                <>
                  <div className="flex flex-col">
                    {[
                      ['Snapshot ID', selected.details.snapshot_id],
                      ['Schema ID', selected.details.current_schema_id],
                      ['Spec ID', selected.details.partition_spec_id],
                      ['Sort Order ID', selected.details.sort_order_id],
                      ['Format Version', selected.details.table_format_version],
                    ].map(([label, value]) => (
                      <div key={label} className="flex justify-between items-center py-1.5 border-b border-[#2d3748] last:border-0">
                        <span className="text-xs text-slate-400">{label}</span>
                        <span className="text-xs font-mono text-[#e2e8f0]">{value ?? '—'}</span>
                      </div>
                    ))}
                  </div>

                  {selectedSnap && (
                    <>
                      <div>
                        <div className="text-[0.65rem] font-bold text-slate-500 uppercase tracking-wider mb-1.5">Operation</div>
                        <span className="text-xs font-mono text-[#2E86C1] bg-[#1e3a5f] px-2.5 py-1 rounded">
                          {selectedSnap.operation ?? '—'}
                        </span>
                      </div>
                      {parseSummary(selectedSnap.summary).length > 0 && (
                        <div>
                          <div className="text-[0.65rem] font-bold text-slate-500 uppercase tracking-wider mb-2">Summary</div>
                          <div className="flex flex-col">
                            {parseSummary(selectedSnap.summary).map(({ key, value }) => (
                              <div key={key} className="flex justify-between items-center py-1.5 border-b border-[#2d3748] last:border-0">
                                <span className="text-xs text-slate-400">{key}</span>
                                <span className="text-xs font-mono text-[#e2e8f0]">{value}</span>
                              </div>
                            ))}
                          </div>
                        </div>
                      )}
                    </>
                  )}

                  {parseProperties(selected.details.properties).length > 0 && (
                    <div>
                      <div className="text-[0.65rem] font-bold text-slate-500 uppercase tracking-wider mb-2">Properties</div>
                      <div className="flex flex-col">
                        {parseProperties(selected.details.properties).map(({ key, value }) => (
                          <div key={key} className="flex justify-between items-start gap-4 py-1.5 border-b border-[#2d3748] last:border-0">
                            <span className="text-xs font-mono text-[#2E86C1] shrink-0">{key}</span>
                            <span className="text-xs text-[#e2e8f0] text-right break-all">{value}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </>
              )}

            </div>
          </div>
        </div>
      )}
    </div>
  )
}
