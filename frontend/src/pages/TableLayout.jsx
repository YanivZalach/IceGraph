import JSONbig from 'json-bigint'
import { useEffect, useRef, useState } from 'react'
import { Outlet, useNavigate, useSearchParams } from 'react-router-dom'
import { formatLocaleDateTime, parseUtcDate } from '../utils/dateUtils'
import { IS_MOCK, MOCK_HOME_ROUTE } from '../appConstants'
import { UI_BODY_MUTED_CLASS, UI_DIALOG_TITLE_CLASS, UI_MONO_MUTED_CLASS } from '../uiTypography'

import MetadataStructured from '../components/MetadataStructured'
import { useTableSpecs } from '../context/TableSpecsContext'
import {
  BRANCH_CONNECTION_COLOR,
  DELETED_DATA_FILE_CONNECTION_COLOR,
  NODE_STYLE_MAP,
} from '../graphConstants'
import { getCachedData } from '../utils/cacheUtils'

const localizeNodeTimestamps = (details) => {
  if (!details) return {}

  const result = { ...details }

  for (const key of Object.keys(result)) {
    if (!key.includes('timestamp')) continue

    try {
      const dateObj = parseUtcDate(result[key])
      if (dateObj) {
        result[key] = formatLocaleDateTime(dateObj)
      }
    } catch (e) {
      console.error('Failed to parse timestamp key:', key, 'value:', result[key], 'error:', e)
    }
  }

  return result
}

export default function TableLayout() {
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const { detailsOpen, setDetailsOpen, selectionDetail, setSelectionDetail, setRawData, setErrors, setWarnings, issuesOpen, setIssuesOpen, errors, warnings } = useTableSpecs()
  const detailPanelRef = useRef(null)

  useEffect(() => {
    sessionStorage.removeItem('last_graph_selection');
  }, []);

  useEffect(() => {
    const handleKey = (e) => {
      if (e.key === 'Escape') {
        setDetailsOpen(false);
        setSelectionDetail(null);
        setIssuesOpen(false);
      }
    }
    window.addEventListener('keydown', handleKey)
    return () => window.removeEventListener('keydown', handleKey)
  }, [setDetailsOpen, setSelectionDetail, setIssuesOpen])

  useEffect(() => {
    const hasErrors = errors && Object.keys(errors).length > 0;
    const hasWarnings = warnings && Object.keys(warnings).length > 0;
    if (hasErrors || hasWarnings) {
      setIssuesOpen(true);
    }
  }, [errors, warnings, setIssuesOpen]);

  const tableName = searchParams.get('table') || ''
  const startSnapshot = searchParams.get('start_snapshot_id') || ''
  const endSnapshot = searchParams.get('end_snapshot_id') || ''
  const isDup = searchParams.get('dup') === '1'
  const cacheKey = isDup
    ? window.location.href
    : `graphData_${tableName}_${startSnapshot}_${endSnapshot}`

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [graphData, setGraphData] = useState(null)
  const [jobId, setJobId] = useState(null)

  const pollIntervalRef = useRef(null)

  const clearPolling = () => {
    if (pollIntervalRef.current) {
      clearInterval(pollIntervalRef.current)
      pollIntervalRef.current = null
    }
  }


  useEffect(() => {
    if (selectionDetail && detailPanelRef.current) {
      detailPanelRef.current.scrollIntoView({ behavior: 'smooth', block: 'nearest' })
    }
  }, [selectionDetail])

  const buildGraphData = (data) => {
    const styledNodes = data.nodes.map((node) => {
      const style = NODE_STYLE_MAP[node.type] || { rgb: [100, 100, 100], level: 0 }
      const [r, g, b] = style.rgb

      node.details = localizeNodeTimestamps(node.details)

      return { ...node, shape: 'box', color: `rgba(${r},${g},${b},${node.color_shift || 1})`, level: style.level }
    })
    const styledEdges = data.edges.map((edge) => {
      const newEdge = { ...edge }
      if (edge.is_deleted) {
        newEdge.color = DELETED_DATA_FILE_CONNECTION_COLOR
        newEdge.title = 'deleted'
      } else if (edge.branch_names) {
        newEdge.dashes = [15, 20, 5, 20]
        newEdge.color = BRANCH_CONNECTION_COLOR
        newEdge.title = edge.branch_names
      }
      return newEdge
    })
    return { nodes: styledNodes, edges: styledEdges, metadata: data.metadata, errors: data.errors || {} }
  }

  const submitGraphJob = async (table, start, end) => {
    try {
      const body = new URLSearchParams()
      body.append('table_name', table)
      if (start) body.append('start_snapshot_id', start)
      if (end) body.append('end_snapshot_id', end)

      const res = await fetch('/api/v1/graph-data', {
        method: 'POST',
        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
        body: body.toString(),
      })

      if (!res.ok) {
        throw new Error('Failed to submit job')
      }

      const result = await res.json()
      setJobId(result.key)

    } catch (err) {
      setError(err.message || 'Failed to submit job')
      setLoading(false)
    }
  }

  const pollJobStatus = async (jid) => {
    try {
      const res = await fetch(`/api/v1/graph-data/${jid}`)
      if (res.status === 200) {
        const text = await res.text()
        setRawData(text)
        const data = JSONbig({ storeAsString: true }).parse(text)
        console.log(JSONbig({ storeAsString: true }).parse(text))

        setGraphData(buildGraphData(data))
        setErrors(data.errors || {})
        setWarnings(data.warnings || {})
        setLoading(false)
        setJobId(null)

        clearPolling()
      } else if (res.status !== 202) {
        setError(res.error || 'Job failed')
        setLoading(false)
        setJobId(null)

        clearPolling()
      }

    } catch (err) {
      setError(err.message || 'Failed to check job status')
      setLoading(false)
      setJobId(null)

      clearPolling()
    }
  }

  useEffect(() => {
    if (!tableName) {
      setError('No table name provided.')
      setLoading(false)
      return
    }

    if (isDup) {
      (async () => {
        try {
          const cached = await getCachedData(cacheKey)
          if (cached) {
            setRawData(cached)
            const data = JSONbig({ storeAsString: true }).parse(cached)
            setGraphData(buildGraphData(data))
            setErrors(data.errors || {})
            setWarnings(data.warnings || {})
            setLoading(false)

            const cleanUrl = new URL(window.location.href)
            cleanUrl.searchParams.delete('dup')
            cleanUrl.searchParams.delete('cache_id')
            history.replaceState(history.state, '', cleanUrl.toString())
            return
          }
          else {
            throw new Error('No cached data found.')
          }
        } catch (err) {
          console.error('Failed to restore from cache:', err)
          setError('No cached data found.')
          setLoading(false)
        }
      })()
      return
    }

    setError(null)
    setErrors({})
    setWarnings({})
    submitGraphJob(tableName, startSnapshot, endSnapshot)

  }, [tableName, startSnapshot, endSnapshot])

  useEffect(() => {
    if (!jobId) return

    pollJobStatus(jobId)

    pollIntervalRef.current = setInterval(() => {
      pollJobStatus(jobId)
    }, 1000)

    return clearPolling
  }, [jobId])

  if (loading) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center bg-canvas">
        <div className="w-10 h-10 border-4 border-edge border-t-accent rounded-full animate-spin mb-4" />
        <p className={UI_BODY_MUTED_CLASS}>
          Loading data for <strong>{tableName}</strong>…
        </p>
      </div>
    )
  }

  if (error) {
    let errorDisplay
    try {
      const parsed = JSONbig({ storeAsString: true }).parse(error)
      errorDisplay = (
        <div className="text-left mt-4 text-xs font-mono space-y-1">
          {Object.entries(parsed).map(([key, val]) => (
            <div key={key} className="flex gap-2">
              <span className="text-red-300 font-bold">{key}:</span>
              <span className="text-slate-300 truncate">{String(val)}</span>
            </div>
          ))}
        </div>
      )
    } catch {
      errorDisplay = <p className="text-sm">{error}</p>
    }

    return (
      <div className="flex-1 flex flex-col items-center justify-center bg-canvas p-6">
        <div className="bg-red-950/50 border border-red-800 text-red-400 px-8 py-6 rounded-xl text-center max-w-lg w-full">
          <h2 className="font-bold mb-2">Request Failed</h2>
          {errorDisplay}
          <button
            className="mt-6 px-5 py-2.5 rounded-lg border-2 border-accent bg-accent text-white font-bold text-sm cursor-pointer hover:bg-accent-dark transition"
            onClick={() => navigate(IS_MOCK ? MOCK_HOME_ROUTE : '/')}
          >
            ← Back to Home
          </button>
        </div>
      </div>
    )
  }

  const metadata = graphData.metadata

  const showDetail = (type, id) => {
    if (!metadata) return
    let data = null
    let label = ''

    if (type === 'schema') {
      data = metadata.schemas?.find(s => s['schema-id'] === id)
      label = `Schema ID: ${id}`
    } else if (type === 'spec') {
      data = metadata['partition-specs']?.find(s => s['spec-id'] === id)
      label = `Partition ID: ${id}`
    } else if (type === 'order') {
      data = metadata['sort-orders']?.find(s => s['order-id'] === id)
      label = `Order ID: ${id}`
    }

    if (data) setSelectionDetail({ label, data })
  }

  return (
    <div className="flex-1 flex overflow-hidden relative">
      <Outlet context={graphData} />

      {detailsOpen && metadata && (
        <div
          className="fixed inset-0 z-[9999] bg-black/50 flex items-center justify-center font-sans"
          onClick={() => { setDetailsOpen(false); setSelectionDetail(null) }}
        >
          <div
            className="w-1/2 min-w-85 max-w-3xl bg-surface rounded-xl shadow-2xl border border-edge max-h-[80dvh] flex flex-col"
            onClick={e => e.stopPropagation()}
          >
            <div className="flex items-center justify-between px-6 py-4 border-b border-edge shrink-0">
              <div>
                <div className={UI_DIALOG_TITLE_CLASS}>Table Specification</div>
                <div className={`${UI_MONO_MUTED_CLASS} mt-0.5`}>{metadata?.['table-name']}</div>
              </div>
              <button
                className="w-7 h-7 rounded-full bg-edge text-slate-400 flex items-center justify-center text-base cursor-pointer hover:bg-edge-hover hover:text-slate-200 transition"
                onClick={() => { setDetailsOpen(false); setSelectionDetail(null) }}
              >
                ✕
              </button>
            </div>

            <div className="overflow-y-auto px-6 py-5 flex flex-col gap-4">
              <MetadataStructured
                metadata={metadata}
                onSelect={showDetail}
                selectedId={selectionDetail?.label}
              />

              {selectionDetail && (
                <div ref={detailPanelRef} className="rounded-lg border-2 border-accent">
                  <div className="flex items-center justify-between px-4 py-2 bg-accent">
                    <span className="text-sm font-bold text-white">{selectionDetail.label}</span>
                    <button
                      className="text-white/70 hover:text-white text-xl leading-none cursor-pointer transition"
                      onClick={() => setSelectionDetail(null)}
                    >
                      ×
                    </button>
                  </div>
                  <pre style={{ margin: 0, padding: '1rem', background: '#0d1117', color: '#e2e8f0', fontSize: '0.8rem', fontFamily: 'monospace', whiteSpace: 'pre', overflowX: 'auto', maxHeight: '300px', display: 'block' }}>
                    {JSON.stringify(selectionDetail.data, null, 2)}
                  </pre>
                </div>
              )}

            </div>
          </div>
        </div>
      )}
      {issuesOpen && (errors || warnings) && (
        <div
          className="fixed inset-0 z-[9999] bg-black/60 flex items-center justify-center font-sans"
          onClick={() => setIssuesOpen(false)}
        >
          <div
            className="w-1/2 min-w-100 max-w-4xl bg-surface rounded-xl shadow-2xl border border-slate-800 max-h-[80dvh] flex flex-col overflow-hidden"
            onClick={e => e.stopPropagation()}
          >
            <div className="flex items-center justify-between px-6 py-4 border-b border-slate-700/30 bg-slate-800/20 shrink-0">
              <div className="flex items-center gap-3">
                <span className="font-bold text-ink text-base tracking-tight">System Issues</span>
              </div>
              <button
                className="w-7 h-7 rounded-full bg-slate-800/30 text-slate-400 flex items-center justify-center text-base cursor-pointer hover:bg-slate-800/50 hover:text-slate-200 transition"
                onClick={() => setIssuesOpen(false)}
              >
                ✕
              </button>
            </div>

            <div className="overflow-y-auto px-6 py-6 flex flex-col gap-8">
              {errors && Object.keys(errors).length > 0 && (
                <div className="flex flex-col gap-4">
                  <div className="flex items-center gap-2 px-1">
                    <div className="w-2 h-2 rounded-full bg-red-500 animate-pulse" />
                    <h3 className="text-red-400 text-xs font-bold uppercase tracking-widest">Critical Errors</h3>
                  </div>
                  {Object.entries(errors).map(([op, err], i) => (
                    <div key={`err-${i}`} className="bg-red-950/10 rounded-xl border border-red-900/30 overflow-hidden flex flex-col">
                      <div className="px-5 py-3 border-b border-red-900/10 bg-red-900/5">
                        <span className="text-base font-bold text-red-500/70 uppercase tracking-tighter block mb-1">Source</span>
                        <div className="text-xs font-mono text-red-200 break-all">{op}</div>
                      </div>
                      <div className="px-5 py-4">
                        <span className="text-base font-bold text-red-500/70 uppercase tracking-tighter block mb-1">Message</span>
                        <div className="text-xs text-red-300 font-semibold whitespace-pre-wrap leading-relaxed overflow-y-auto tracking-wide">{err}</div>
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {warnings && Object.keys(warnings).length > 0 && (
                <div className="flex flex-col gap-4">
                  <div className="flex items-center gap-2 px-1">
                    <div className="w-2 h-2 rounded-full bg-amber-500" />
                    <h3 className="text-amber-400 text-xs font-bold uppercase tracking-widest">Processing Warnings</h3>
                  </div>
                  {Object.entries(warnings).map(([op, msg], i) => (
                    <div key={`warn-${i}`} className="bg-amber-950/10 rounded-xl border border-amber-900/30 overflow-hidden flex flex-col">
                      <div className="px-5 py-3 border-b border-amber-900/10 bg-amber-900/5">
                        <span className="text-base font-bold text-amber-500/70 uppercase tracking-tighter block mb-1">Context</span>
                        <div className="text-xs font-mono text-amber-200 break-all">{op}</div>
                      </div>
                      <div className="px-5 py-4">
                        <span className="text-base font-bold text-amber-500/70 uppercase tracking-tighter block mb-1">Notice</span>
                        <div className="text-xs text-amber-300 font-semibold whitespace-pre-wrap leading-relaxed overflow-y-auto tracking-wide">{msg}</div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  )
}