import { useCallback, useEffect, useRef, useState } from 'react'
import { useLocation, useOutletContext } from 'react-router-dom'
import { Network } from 'vis-network/standalone'
import {
  UI_NEWLINE,
  UI_SECTION_NEWLINE,
  VISUALIZATION_OPTIONS,
} from '../graphConstants'

function applySelection(network, nodeId) {
  const liveNodes = network.body.data.nodes
  const liveEdges = network.body.data.edges

  liveNodes.update(liveNodes.get().map(n => ({ ...n, hidden: false })))
  liveEdges.update(liveEdges.get().map(e => ({ ...e, hidden: false })))

  const relatedNodes = new Set([String(nodeId)])
  const traverse = (id, direction) => {
    network.getConnectedNodes(id, direction).forEach(connId => {
      const s = String(connId)
      if (!relatedNodes.has(s)) { relatedNodes.add(s); traverse(connId, direction) }
    })
  }
  traverse(nodeId, 'to')
  traverse(nodeId, 'from')

  liveNodes.update(liveNodes.get().map(n => ({ ...n, hidden: !relatedNodes.has(String(n.id)) })))
  liveEdges.update(liveEdges.get().map(e => ({
    ...e,
    hidden: !(relatedNodes.has(String(e.from)) && relatedNodes.has(String(e.to))),
  })))

  requestAnimationFrame(() => network.fit())
}

export default function GraphPage() {
  const { nodes, edges, metadata, errors } = useOutletContext()

  const location = useLocation()
  const networkContainerRef = useRef(null)
  const networkRef = useRef(null)
  const initialSelectRef = useRef(location.state?.selectNodeId || null)
  const restoreSelectRef = useRef(
    !location.state?.selectNodeId ? (history.state?.graphSelection || null) : null
  )

  const [isInspectMode, setIsInspectMode] = useState(false)
  const [isFullView, setIsFullView] = useState(true)
  const [stickyNode, setStickyNode] = useState(null)

  const isInspectModeRef = useRef(isInspectMode)
  useEffect(() => {
    isInspectModeRef.current = isInspectMode
  }, [isInspectMode])

  useEffect(() => {
    const handleKey = (e) => { if (e.key === 'Escape') setStickyNode(null) }
    window.addEventListener('keydown', handleKey)
    return () => window.removeEventListener('keydown', handleKey)
  }, [])

  const resetView = useCallback(() => {
    const network = networkRef.current
    if (!network) return
    const liveNodes = network.body.data.nodes
    const liveEdges = network.body.data.edges
    liveNodes.update(liveNodes.get().map(n => ({ ...n, hidden: false })))
    liveEdges.update(liveEdges.get().map(e => ({ ...e, hidden: false })))
    setStickyNode(null)
    setIsFullView(true)
    history.replaceState({ graphSelection: null }, '')
    requestAnimationFrame(() => { network.redraw(); network.fit() })
  }, [])

  useEffect(() => {
    if (!history.state || !('graphSelection' in history.state)) {
      history.replaceState({ graphSelection: null }, '')
    }

    const handlePopState = (e) => {
      if (!e.state || !('graphSelection' in e.state)) return
      const nodeId = e.state.graphSelection
      const network = networkRef.current
      if (!network) return
      if (nodeId === null) {
        resetView()
        return
      }
      applySelection(network, nodeId)
      const nodeData = network.body.data.nodes.get(nodeId)
      if (nodeData) { setStickyNode(nodeData); setIsFullView(false) }
    }

    window.addEventListener('popstate', handlePopState)
    return () => window.removeEventListener('popstate', handlePopState)
  }, [resetView])

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

    network.once('afterDrawing', () => {
      const fromFileTree = initialSelectRef.current
      const fromHistory = restoreSelectRef.current
      initialSelectRef.current = null
      restoreSelectRef.current = null

      const nodeId = fromFileTree || fromHistory
      if (nodeId) {
        applySelection(network, nodeId)
        const nodeData = network.body.data.nodes.get(nodeId)
        if (nodeData) { setStickyNode(nodeData); setIsFullView(false) }
        if (fromFileTree) history.replaceState({ graphSelection: nodeId }, '')
      } else {
        network.fit()
      }
    })
    network.on('zoom', () => setIsFullView(false))
    network.on('dragEnd', () => setIsFullView(false))

    network.on('click', (params) => {
      if (params.nodes.length === 0) return

      const selectedNodeId = params.nodes[0]
      const nodeData = network.body.data.nodes.get(selectedNodeId)

      if (!isInspectModeRef.current) {
        applySelection(network, selectedNodeId)
        setIsFullView(false)
      }

      history.pushState({ graphSelection: selectedNodeId }, '')
      setStickyNode(nodeData)
    })

    return () => network.destroy()
  }, [nodes, edges])

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
    <div
      className="relative w-full flex-1 overflow-hidden"
      style={{
        backgroundColor: '#0d1117',
        backgroundImage: 'radial-gradient(circle, #2d3748 1px, transparent 1px)',
        backgroundSize: '24px 24px',
      }}
    >
      <div ref={networkContainerRef} className="absolute inset-0" />

      <div className="absolute top-4 left-4 flex flex-col gap-2 z-[9999] font-sans w-[200px]">
        <button
          className={`w-full py-2.5 rounded-lg cursor-pointer font-bold text-xs uppercase tracking-wide shadow-md transition
            ${isFullView
              ? 'bg-[#2E86C1] text-white hover:bg-[#2471a3]'
              : 'bg-[#1a202c] text-[#2E86C1] border border-[#2E86C1] hover:bg-[#2d3748]'
            }`}
          onClick={resetView}
        >
          Reset Full View
        </button>

        <button
          className={`w-full flex overflow-hidden rounded-lg cursor-pointer font-bold text-xs uppercase tracking-wide shadow-md transition
            ${isInspectMode
              ? 'bg-[#2E86C1] text-white border border-[#2E86C1] hover:bg-[#2471a3]'
              : 'bg-[#1a202c] text-[#2E86C1] border border-[#2E86C1] hover:bg-[#2d3748]'
            }`}
          onClick={() => setIsInspectMode(p => !p)}
        >
          <span className="w-9 flex items-center justify-center text-lg bg-black/5 shrink-0 py-2.5">
            {isInspectMode ? '🔒' : '🔍'}
          </span>
          <span className="flex-1 flex items-center justify-center py-2.5 px-2 leading-tight">
            {isInspectMode ? 'Inspect (Locked)' : 'Lineage Traversal'}
          </span>
        </button>

        <button
          className="w-full py-2.5 rounded-lg cursor-pointer font-bold text-xs uppercase tracking-wide shadow-md transition bg-[#1a202c] text-[#2E86C1] border border-[#2E86C1] hover:bg-[#2d3748]"
          onClick={() => networkRef.current?.fit()}
        >
          Center Graph
        </button>
      </div>

      {sticky && (
        <div
          className="absolute top-4 right-4 w-[400px] max-h-[88vh] overflow-y-auto bg-[#1a202c] border-l-4 rounded-xl z-[1000] shadow-xl"
          style={{ borderLeftColor: stickyNode.color }}
        >
          <div className="flex items-start justify-between px-5 pt-5 pb-4 border-b border-[#2d3748]">
            <div className="font-bold text-base text-[#e2e8f0] pr-6 leading-snug">{sticky.title}</div>
            <button
              className="w-7 h-7 rounded-full bg-[#2d3748] text-slate-400 flex items-center justify-center text-base cursor-pointer hover:bg-[#3d4a5c] hover:text-slate-200 transition shrink-0"
              onClick={() => setStickyNode(null)}
            >
              ✕
            </button>
          </div>
          <div className="px-5 py-4 flex flex-col gap-3">
            {isInspectMode && (
              <span className="inline-flex items-center gap-1.5 bg-[#2E86C1]/10 text-[#2E86C1] px-2.5 py-1 rounded-md text-[0.65rem] font-bold uppercase tracking-wide w-fit">
                🔒 Locked View
              </span>
            )}
            {sticky.rows.map((r, i) => {
              let displayValue = r.value
              const tryParseJson = (str) => {
                try { return JSON.parse(str) } catch { return undefined }
              }
              const asPythonToJson = (str) => str
                .replace(/'/g, '"')
                .replace(/\bTrue\b/g, 'true')
                .replace(/\bFalse\b/g, 'false')
                .replace(/\bNone\b/g, 'null')
              const parsed = tryParseJson(r.value) ?? tryParseJson(asPythonToJson(r.value))
              if (parsed !== undefined && typeof parsed === 'object' && parsed !== null) {
                displayValue = JSON.stringify(parsed, null, 2)
              }
              return (
                <div key={i}>
                  <span className="block font-bold text-slate-500 text-[0.65rem] uppercase tracking-wider mb-1">
                    {r.label}
                  </span>
                  <span className="block font-mono bg-[#0d1117] text-slate-200 px-3 py-2 rounded-lg text-xs whitespace-pre overflow-x-auto break-normal">
                    {displayValue}
                  </span>
                </div>
              )
            })}
          </div>
        </div>
      )}
    </div>
  )
}
