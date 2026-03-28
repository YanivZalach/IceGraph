import { useEffect, useState } from 'react'
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

  const tableName = searchParams.get('table') || ''
  const date = searchParams.get('date') || ''

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [graphData, setGraphData] = useState(null)

  useEffect(() => {
    if (!tableName) {
      setError('No table name provided.')
      setLoading(false)
      return
    }

    const body = new URLSearchParams({ table_name: tableName, date })

    fetch('/api/v1/graph-data', {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: body.toString(),
    })
      .then(async (res) => {
        const data = await res.json()
        if (!res.ok) throw new Error(data.error || 'Request failed')
        return data
      })
      .then((data) => {
        const styledNodes = new DataSet(
          data.nodes.map((node) => {
            const style = NODE_STYLE_MAP[node.type] || { rgb: [100, 100, 100], level: 0 }
            const [r, g, b] = style.rgb
            return {
              ...node,
              shape: 'box',
              color: `rgba(${r},${g},${b},${node.color_shift || 1})`,
              level: style.level,
            }
          })
        )

        const styledEdges = new DataSet(
          data.edges.map((edge) => {
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
        )

        setGraphData({
          nodes: styledNodes,
          edges: styledEdges,
          metadata: data.metadata,
          errors: data.errors || {},
        })
        setLoading(false)
      })
      .catch((err) => {
        setError(err.message || 'An unexpected error occurred.')
        setLoading(false)
      })
  }, [tableName, date])

  if (loading) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center bg-slate-50">
        <div className="w-10 h-10 border-4 border-slate-200 border-t-[#2E86C1] rounded-full animate-spin mb-4" />
        <p className="text-slate-500 text-sm">
          Loading data for <strong>{tableName}</strong>…
        </p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center bg-slate-50">
        <div className="bg-red-50 border border-red-200 text-red-700 px-8 py-6 rounded-xl text-center max-w-sm">
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
          className="absolute inset-0 z-[9999] bg-black/50 flex items-center justify-center font-sans"
          onClick={() => setDetailsOpen(false)}
        >
          <div
            className="w-[50vw] min-w-[340px] max-w-[720px] bg-white rounded-xl shadow-2xl border border-slate-200 p-5 max-h-[80vh] overflow-y-auto"
            onClick={e => e.stopPropagation()}
          >
            <div className="font-extrabold text-slate-600 text-[0.65rem] uppercase mb-1 tracking-wide">
              Table Specification
            </div>
            <MetadataStructured
              metadata={metadata}
              onSelect={showDetail}
              selectedId={selectionDetail?.label}
            />

            {selectionDetail && (
              <div className="mt-4 p-2.5 bg-slate-100 rounded-lg border border-slate-200">
                <div className="text-[0.7rem] font-extrabold text-slate-600 uppercase mb-2 flex justify-between items-center">
                  <span>{selectionDetail.label}</span>
                  <span
                    className="cursor-pointer text-slate-400 text-lg"
                    onClick={() => setSelectionDetail(null)}
                  >
                    ×
                  </span>
                </div>
                <pre className="font-mono text-sm bg-[#1e293b] text-slate-50 p-3 rounded-md overflow-auto whitespace-pre break-normal max-h-[400px]">
                  {JSON.stringify(selectionDetail.data, null, 2)}
                </pre>
              </div>
            )}

            <div className="font-extrabold text-slate-600 text-[0.65rem] uppercase mb-1 tracking-wide mt-4">
              Raw Metadata JSON
            </div>
            <pre className="font-mono text-sm text-[#2E86C1] m-0 whitespace-pre break-normal overflow-auto max-h-[400px] p-2.5">
              {JSON.stringify(metadata, null, 2)}
            </pre>
          </div>
        </div>
      )}
    </div>
  )
}
