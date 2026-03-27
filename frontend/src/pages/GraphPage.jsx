import { useCallback, useEffect, useRef, useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { DataSet, Network } from 'vis-network/standalone'
import MetadataStructured from '../components/MetadataStructured'
import {
  BRANCH_CONNECTION_COLOR,
  DELETED_DATA_FILE_CONNECTION_COLOR,
  NODE_STYLE_MAP,
  UI_NEWLINE,
  UI_SECTION_NEWLINE,
  VISUALIZATION_OPTIONS,
} from '../graphConstants'

export default function GraphPage() {
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()

  const tableName = searchParams.get('table') || ''
  const date = searchParams.get('date') || ''

  const networkContainerRef = useRef(null)
  const networkRef = useRef(null)
  const nodesRef = useRef(null)
  const edgesRef = useRef(null)

  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [metadata, setMetadata] = useState(null)
  const [errors, setErrors] = useState({})

  const [isInspectMode, setIsInspectMode] = useState(false)
  const [isFullView, setIsFullView] = useState(true)
  const [stickyNode, setStickyNode] = useState(null)
  const [detailsOpen, setDetailsOpen] = useState(false)
  const [selectionDetail, setSelectionDetail] = useState(null)

  const isInspectModeRef = useRef(isInspectMode)
  useEffect(() => {
    isInspectModeRef.current = isInspectMode
  }, [isInspectMode])
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
        const styledNodes = data.nodes.map((node) => {
          const style = NODE_STYLE_MAP[node.type] || { rgb: [100, 100, 100], level: 0 }
          const [r, g, b] = style.rgb
          return {
            ...node,
            shape: 'box',
            color: `rgba(${r},${g},${b},${node.color_shift || 1})`,
            level: style.level,
          }
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

        nodesRef.current = new DataSet(styledNodes)
        edgesRef.current = new DataSet(styledEdges)
        setMetadata(data.metadata)
        setErrors(data.errors || {})
        setLoading(false)
      })
      .catch((err) => {
        setError(err.message || 'An unexpected error occurred.')
        setLoading(false)
      })
  }, [tableName, date])

  useEffect(() => {
    if (loading || error || !nodesRef.current) return

    const network = new Network(
      networkContainerRef.current,
      { nodes: nodesRef.current, edges: edgesRef.current },
      VISUALIZATION_OPTIONS
    )
    networkRef.current = network

    network.once('afterDrawing', () => network.fit())

    network.on('zoom', () => setIsFullView(false))
    network.on('dragEnd', () => setIsFullView(false))

    network.on('click', (params) => {
      if (params.nodes.length === 0) return

      const selectedNodeId = params.nodes[0]

      const liveNodes = network.body.data.nodes
      const liveEdges = network.body.data.edges
      const nodeData = liveNodes.get(selectedNodeId)

      if (!isInspectModeRef.current) {
        liveNodes.update(liveNodes.get().map(n => ({ ...n, hidden: false })))
        liveEdges.update(liveEdges.get().map(e => ({ ...e, hidden: false })))

        const relatedNodes = new Set([String(selectedNodeId)])

        const traverse = (nodeId, direction) => {
          network.getConnectedNodes(nodeId, direction).forEach(id => {
            const idStr = String(id)
            if (!relatedNodes.has(idStr)) {
              relatedNodes.add(idStr)
              traverse(id, direction)
            }
          })
        }

        traverse(selectedNodeId, 'to')
        traverse(selectedNodeId, 'from')

        liveNodes.update(
          liveNodes.get().map((n) => ({
            ...n,
            hidden: !relatedNodes.has(String(n.id))
          }))
        )

        liveEdges.update(
          liveEdges.get().map((e) => ({
            ...e,
            hidden: !(relatedNodes.has(String(e.from)) && relatedNodes.has(String(e.to)))
          }))
        )

        requestAnimationFrame(() => network.fit());
        setIsFullView(false)
      }

      setStickyNode(nodeData)
    })

    return () => network.destroy()
  }, [loading, error])



  useEffect(() => {
    if (Object.keys(errors).length > 0) {
      const summary = Object.entries(errors)
        .map(([file, err]) => `• ${file.split('/').pop()}: ${err}`)
        .join('\n')
      alert(`⚠️ IceGraph: ${Object.keys(errors).length} Errors Detected\n\n${summary}`)
    }
  }, [errors])

  const resetView = useCallback(() => {
    const network = networkRef.current;
    if (!network) return;

    const liveNodes = network.body.data.nodes;
    const liveEdges = network.body.data.edges;

    const allNodes = liveNodes.get({ filter: () => true });
    const allEdges = liveEdges.get({ filter: () => true });

    liveNodes.update(allNodes.map(n => ({ ...n, hidden: false })));
    liveEdges.update(allEdges.map(e => ({ ...e, hidden: false })));

    setStickyNode(null);
    setIsFullView(true);

    requestAnimationFrame(() => {
      network.redraw();
      network.fit();
    });
  }, []);

  const parseStickyDetails = (details) => {
    if (!details) return { title: '', rows: [] }
    const splitToken = UI_SECTION_NEWLINE === '\n' ? /\\n|\n/ : UI_SECTION_NEWLINE
    const lines = details.split(splitToken).map((l) => l.replace(new RegExp(UI_NEWLINE, 'g'), '\n'))
    const title = lines[0] || ''
    const rows = []
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim()
      const idx = line.indexOf(':')
      if (idx === -1) continue
      rows.push({ label: line.substring(0, idx), value: line.substring(idx + 1).trim() })
    }
    return { title, rows }
  }

  const showDetail = (type, id) => {
    if (!metadata) return
    let data = null
    let label = ''
    if (type === 'schema') {
      data = metadata.schemas?.find((s) => s['schema-id'] === id)
      label = `Schema ID: ${id}`
    } else if (type === 'spec') {
      data = metadata['partition-specs']?.find((s) => s['spec-id'] === id)
      label = `Spec ID: ${id}`
    } else if (type === 'order') {
      data = metadata['sort-orders']?.find((s) => s['order-id'] === id)
      label = `Order ID: ${id}`
    }
    if (data) setSelectionDetail({ label, data })
  }

  if (loading) {
    return (
      <div className="flex flex-col items-center justify-center h-screen bg-slate-50 font-sans">
        <div className="w-10 h-10 border-4 border-slate-200 border-t-[#2E86C1] rounded-full animate-spin mb-4" />
        <p className="text-slate-500 text-sm">Generating graph for <strong>{tableName}</strong>…</p>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex flex-col items-center justify-center h-screen bg-slate-50 font-sans">
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

  const sticky = stickyNode ? parseStickyDetails(stickyNode.details) : null

  return (
    <div className="relative w-screen h-screen overflow-hidden bg-slate-50">
      <div ref={networkContainerRef} className="w-full h-screen" />

      <div className="fixed top-5 left-5 flex flex-col gap-2 z-[9999] font-sans">
        <div className="flex gap-2 w-[220px]">
          <button
            className="p-3 rounded-lg cursor-pointer text-xl shadow-lg bg-white text-[#2E86C1] border-2 border-[#2E86C1] flex items-center justify-center hover:bg-slate-50 transition"
            onClick={() => navigate('/')}
            title="Home"
          >
            🏠
          </button>
          <button
            className={`flex-1 px-2.5 py-3 rounded-lg cursor-pointer font-bold text-xs uppercase tracking-wide shadow-lg transition whitespace-nowrap
              ${isFullView
                ? 'bg-[#2E86C1] text-white border-none'
                : 'bg-white text-[#2E86C1] border-2 border-[#2E86C1]'
              }`}
            onClick={resetView}
          >
            Reset Full View
          </button>
        </div>

        <button
          className={`flex overflow-hidden w-[220px] rounded-lg cursor-pointer font-bold text-xs uppercase tracking-wide shadow-lg p-0 transition
            ${isInspectMode
              ? 'bg-[#2E86C1] text-white border-2 border-[#2E86C1]'
              : 'bg-white text-[#2E86C1] border-2 border-[#2E86C1]'
            }`}
          onClick={() => setIsInspectMode((p) => !p)}
        >
          <span className="w-[30%] flex items-center justify-center text-2xl bg-black/5">
            {isInspectMode ? '🔒' : '🔍'}
          </span>
          <span className="w-[70%] flex items-center justify-center p-3">
            {isInspectMode ? 'Mode: Inspect (Locked)' : 'Mode: Lineage Traversal'}
          </span>
        </button>

        <div className="flex flex-col gap-1.5">
          <button
            className={`truncate px-4.5 py-3 rounded-lg cursor-pointer font-bold text-xs uppercase tracking-wide shadow-md text-center w-[220px] transition
              ${detailsOpen
                ? 'bg-[#2E86C1] text-white border-none'
                : 'bg-white text-[#2E86C1] border-2 border-[#2E86C1]'
              }`}
            onClick={() => setDetailsOpen((p) => !p)}
          >
            {metadata?.['table-name'] || 'Table Specs'}
          </button>

          {detailsOpen && metadata && (
            <div className="w-[25vw] min-w-[280px] bg-white rounded-lg shadow-lg border border-slate-200 p-3 max-h-[67vh] overflow-y-auto">
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
          )}
        </div>
      </div>

      {sticky && (
        <div
          className="fixed top-5 right-5 w-[420px] max-h-[85vh] overflow-y-auto bg-white/[0.98] border-l-[12px] rounded-xl p-6 z-[1000] shadow-[-10px_10px_30px_rgba(0,0,0,0.1)] backdrop-blur-sm"
          style={{ borderLeftColor: stickyNode.color }}
        >
          <button
            className="absolute top-4 right-4 w-8 h-8 rounded-full bg-slate-100 text-slate-500 border-none flex items-center justify-center text-lg cursor-pointer hover:bg-slate-200 transition"
            onClick={() => setStickyNode(null)}
          >
            ✕
          </button>
          <div className="font-extrabold text-xl text-slate-900 mb-4 pb-3 border-b-2 border-slate-100">
            {sticky.title}
          </div>
          {isInspectMode && (
            <div className="inline-block bg-blue-100 text-blue-800 px-2 py-1 rounded text-[0.65rem] font-extrabold mb-4">
              LOCKED VIEW
            </div>
          )}
          {sticky.rows.map((r, i) => (
            <div key={i} className="mb-3.5">
              <span className="block font-bold text-slate-500 text-[0.7rem] uppercase mb-1">
                {r.label}
              </span>
              <span className="block font-mono bg-[#1e293b] text-white px-3.5 py-2.5 rounded-lg text-sm shadow-[inset_0_2px_4px_rgba(0,0,0,0.2)] whitespace-pre overflow-x-auto break-normal">
                {r.value}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

