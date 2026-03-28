import { useCallback, useEffect, useRef, useState } from 'react'
import { useOutletContext } from 'react-router-dom'
import { Network } from 'vis-network/standalone'
import {
  UI_NEWLINE,
  UI_SECTION_NEWLINE,
  VISUALIZATION_OPTIONS,
} from '../graphConstants'

export default function GraphPage() {
  const { nodes, edges, metadata, errors } = useOutletContext()

  const networkContainerRef = useRef(null)
  const networkRef = useRef(null)

  const [isInspectMode, setIsInspectMode] = useState(false)
  const [isFullView, setIsFullView] = useState(true)
  const [stickyNode, setStickyNode] = useState(null)

  const isInspectModeRef = useRef(isInspectMode)
  useEffect(() => {
    isInspectModeRef.current = isInspectMode
  }, [isInspectMode])

  useEffect(() => {
    if (Object.keys(errors).length > 0) {
      const summary = Object.entries(errors)
        .map(([file, err]) => `• ${file.split('/').pop()}: ${err}`)
        .join('\n')
      alert(`⚠️ IceGraph: ${Object.keys(errors).length} Errors Detected\n\n${summary}`)
    }
  }, [errors])

  useEffect(() => {
    const network = new Network(
      networkContainerRef.current,
      { nodes, edges },
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

        liveNodes.update(liveNodes.get().map(n => ({
          ...n,
          hidden: !relatedNodes.has(String(n.id)),
        })))
        liveEdges.update(liveEdges.get().map(e => ({
          ...e,
          hidden: !(relatedNodes.has(String(e.from)) && relatedNodes.has(String(e.to))),
        })))

        requestAnimationFrame(() => network.fit())
        setIsFullView(false)
      }

      setStickyNode(nodeData)
    })

    return () => network.destroy()
  }, [nodes, edges])

  const resetView = useCallback(() => {
    const network = networkRef.current
    if (!network) return

    const liveNodes = network.body.data.nodes
    const liveEdges = network.body.data.edges

    liveNodes.update(liveNodes.get().map(n => ({ ...n, hidden: false })))
    liveEdges.update(liveEdges.get().map(e => ({ ...e, hidden: false })))

    setStickyNode(null)
    setIsFullView(true)

    requestAnimationFrame(() => {
      network.redraw()
      network.fit()
    })
  }, [])

  const parseStickyDetails = (details) => {
    if (!details) return { title: '', rows: [] }
    const splitToken = UI_SECTION_NEWLINE === '\n' ? /\\n|\n/ : UI_SECTION_NEWLINE
    const lines = details.split(splitToken).map(l => l.replace(new RegExp(UI_NEWLINE, 'g'), '\n'))
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

  const sticky = stickyNode ? parseStickyDetails(stickyNode.details) : null

  return (
    <div className="relative w-full flex-1 overflow-hidden bg-slate-50">
      <div ref={networkContainerRef} className="absolute inset-0" />

      <div className="absolute top-5 left-5 flex flex-col gap-2 z-[9999] font-sans">
        <div className="flex gap-2 w-[220px]">
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
          onClick={() => setIsInspectMode(p => !p)}
        >
          <span className="w-[30%] flex items-center justify-center text-2xl bg-black/5">
            {isInspectMode ? '🔒' : '🔍'}
          </span>
          <span className="w-[70%] flex items-center justify-center p-3">
            {isInspectMode ? 'Mode: Inspect (Locked)' : 'Mode: Lineage Traversal'}
          </span>
        </button>

      </div>

      {sticky && (
        <div
          className="absolute top-5 right-5 w-[420px] max-h-[85vh] overflow-y-auto bg-white/[0.98] border-l-[12px] rounded-xl p-6 z-[1000] shadow-[-10px_10px_30px_rgba(0,0,0,0.1)] backdrop-blur-sm"
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
