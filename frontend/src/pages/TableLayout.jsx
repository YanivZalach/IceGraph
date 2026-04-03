import JSONbig from 'json-bigint'
import { useEffect, useRef, useState } from 'react'
import { Outlet, useNavigate, useSearchParams } from 'react-router-dom'
import { DataSet } from 'vis-network/standalone'
import MetadataStructured from '../components/MetadataStructured'
import { useTableSpecs } from '../context/TableSpecsContext'
import {
  BRANCH_CONNECTION_COLOR,
  DELETED_DATA_FILE_CONNECTION_COLOR,
  NODE_STYLE_MAP,
} from '../graphConstants'

export default function TableLayout() {
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const { detailsOpen, setDetailsOpen, selectionDetail, setSelectionDetail } = useTableSpecs()
  const detailPanelRef = useRef(null)

  useEffect(() => {
    const handleKey = (e) => {
      if (e.key === 'Escape') { setDetailsOpen(false); setSelectionDetail(null) }
    }
    window.addEventListener('keydown', handleKey)
    return () => window.removeEventListener('keydown', handleKey)
  }, [setDetailsOpen, setSelectionDetail])

  const tableName = searchParams.get('table') || ''
  const date = searchParams.get('date') || ''
  const isDup = searchParams.get('dup') === '1'
  const cacheKey = `graphData_${tableName}_${date}`

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [graphData, setGraphData] = useState(null)

  useEffect(() => {
    if (selectionDetail && detailPanelRef.current) {
      detailPanelRef.current.scrollIntoView({ behavior: 'smooth', block: 'nearest' })
    }
  }, [selectionDetail])

  const buildGraphData = (data) => {
    const styledNodes = new DataSet(
      data.nodes.map((node) => {
        const style = NODE_STYLE_MAP[node.type] || { rgb: [100, 100, 100], level: 0 }
        const [r, g, b] = style.rgb
        return { ...node, shape: 'box', color: `rgba(${r},${g},${b},${node.color_shift || 1})`, level: style.level }
      })
    )
    const styledEdges = new DataSet(
      data.edges.map((edge) => {
        const newEdge = { ...edge }
        if (edge.is_deleted) { newEdge.color = DELETED_DATA_FILE_CONNECTION_COLOR; newEdge.title = 'deleted' }
        else if (edge.branch_names) { newEdge.dashes = [15, 20, 5, 20]; newEdge.color = BRANCH_CONNECTION_COLOR; newEdge.title = edge.branch_names }
        return newEdge
      })
    )
    return { nodes: styledNodes, edges: styledEdges, metadata: data.metadata, errors: data.errors || {} }
  }

  useEffect(() => {
    if (!tableName) {
      setError('No table name provided.')
      setLoading(false)
      return
    }

    if (isDup) {
      const cached = sessionStorage.getItem(cacheKey)
      if (cached) {
        setGraphData(buildGraphData(JSONbig({ storeAsString: true }).parse(cached)))
        setLoading(false)
        const cleanUrl = new URL(window.location.href)
        cleanUrl.searchParams.delete('dup')
        history.replaceState(history.state, '', cleanUrl.toString())
        return
      }
    }

    const body = new URLSearchParams({ table_name: tableName, date })
    const controller = new AbortController()

    fetch('/api/v1/graph-data', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: body.toString(),
      signal: controller.signal,
    })
      .then(async (res) => {
        const text = await res.text()
        const data = JSONbig({ storeAsString: true }).parse(text)

        console.log(data)

        if (!res.ok) throw new Error(data.error || 'Request failed')
        sessionStorage.setItem(cacheKey, text)
        return data
      })
      .then((data) => {
        setGraphData(buildGraphData(data))
        setLoading(false)
      })
      .catch((err) => {
        if (err.name === 'AbortError') return
        setError(err.message || 'An unexpected error occurred.')
        setLoading(false)
      })

    return () => controller.abort()
  }, [tableName, date])

  if (loading) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center bg-[#0d1117]">
        <div className="w-10 h-10 border-4 border-[#2d3748] border-t-[#2E86C1] rounded-full animate-spin mb-4" />
        <p className="text-slate-400 text-sm">
          Loading data for <strong>{tableName}</strong>…
        </p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center bg-[#0d1117]">
        <div className="bg-red-950/50 border border-red-800 text-red-400 px-8 py-6 rounded-xl text-center max-w-sm">
          <p>{error}</p>
          <button
            className="mt-3 px-5 py-2.5 rounded-lg border-2 border-[#2E86C1] bg-[#2E86C1] text-white font-bold text-sm cursor-pointer"
            onClick={() => navigate('/')}
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
      label = `Spec ID: ${id}`
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
            className="w-[50vw] min-w-[340px] max-w-[720px] bg-[#1a202c] rounded-xl shadow-2xl border border-[#2d3748] max-h-[80vh] flex flex-col"
            onClick={e => e.stopPropagation()}
          >
            <div className="flex items-center justify-between px-6 py-4 border-b border-[#2d3748] shrink-0">
              <div>
                <div className="font-bold text-[#e2e8f0] text-sm">Table Specification</div>
                <div className="text-xs text-slate-400 font-mono mt-0.5">{metadata?.['table-name']}</div>
              </div>
              <button
                className="w-7 h-7 rounded-full bg-[#2d3748] text-slate-400 flex items-center justify-center text-base cursor-pointer hover:bg-[#3d4a5c] hover:text-slate-200 transition"
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
                <div ref={detailPanelRef} className="rounded-lg border-2 border-[#2E86C1]">
                  <div className="flex items-center justify-between px-4 py-2 bg-[#2E86C1]">
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
    </div>
  )
}
