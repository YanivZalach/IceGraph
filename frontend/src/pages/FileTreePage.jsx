import { useEffect, useMemo, useRef, useState } from 'react'
import { useOutletContext } from 'react-router-dom'
import { FileType, UI_NEWLINE, UI_SECTION_NEWLINE } from '../graphConstants'

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

const FILE_TYPES = new Set([FileType.DATA, FileType.POSITION_DELETE, FileType.EQUALITY_DELETE])

export default function FileTreePage() {
  const { nodes, edges } = useOutletContext()
  const [search, setSearch] = useState('')
  const [selectedIdx, setSelectedIdx] = useState(null)
  const [collapsed, setCollapsed] = useState({})
  const [dropdownOpen, setDropdownOpen] = useState(false)
  const dropdownRef = useRef(null)

  useEffect(() => {
    if (!dropdownOpen) return
    const handleClick = (e) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target)) setDropdownOpen(false)
    }
    document.addEventListener('mousedown', handleClick)
    return () => document.removeEventListener('mousedown', handleClick)
  }, [dropdownOpen])

  const { snapshots, adjacency, nodeById } = useMemo(() => {
    const allNodes = nodes.get()
    const allEdges = edges.get()

    const byId = {}
    for (const n of allNodes) byId[n.id] = n

    const snaps = allNodes
      .filter(n => n.type === FileType.SNAPSHOT)
      .map(n => ({ ...n, parsedDetails: parseDetails(n.details) }))
      .sort((a, b) => {
        const ta = new Date(a.parsedDetails.timestamp || 0).getTime()
        const tb = new Date(b.parsedDetails.timestamp || 0).getTime()
        return ta - tb
      })

    const adj = {}
    for (const e of allEdges) {
      if (!adj[e.from]) adj[e.from] = []
      adj[e.from].push({ to: e.to, is_deleted: !!e.is_deleted })
    }

    return { snapshots: snaps, adjacency: adj, nodeById: byId }
  }, [nodes, edges])

  const effectiveIdx = selectedIdx !== null ? selectedIdx : snapshots.length - 1

  const partitionMap = useMemo(() => {
    if (snapshots.length === 0) return {}
    const snapshot = snapshots[effectiveIdx]
    if (!snapshot) return {}

    const visited = new Set()
    const queue = [snapshot.id]
    const dataFiles = []

    while (queue.length > 0) {
      const current = queue.shift()
      if (visited.has(current)) continue
      visited.add(current)

      for (const { to, is_deleted } of adjacency[current] || []) {
        const child = nodeById[to]
        if (!child) continue
        if (FILE_TYPES.has(child.type)) {
          if (!is_deleted) dataFiles.push(child)
        } else if (child.type === FileType.MANIFEST) {
          queue.push(to)
        }
      }
    }

    const partMap = {}
    for (const f of dataFiles) {
      const details = parseDetails(f.details)
      const partition = details.partition || '(unpartitioned)'
      if (!partMap[partition]) partMap[partition] = []
      partMap[partition].push(f.id)
    }
    return partMap
  }, [snapshots, effectiveIdx, adjacency, nodeById])

  const filteredPartitions = useMemo(() => {
    const q = search.trim().toLowerCase()
    const entries = Object.entries(partitionMap)
      .filter(([part]) => !q || part.toLowerCase().includes(q))
      .sort(([a], [b]) => b.localeCompare(a))
    return entries
  }, [partitionMap, search])

  const totalPartitions = filteredPartitions.length
  const totalFiles = filteredPartitions.reduce((sum, [, f]) => sum + f.length, 0)

  const toggleCollapse = (partition) =>
    setCollapsed(prev => ({ ...prev, [partition]: !prev[partition] }))

  if (snapshots.length === 0) {
    return (
      <div className="flex-1 flex items-center justify-center bg-[#0d1117]">
        <p className="text-slate-500 text-sm italic">No snapshots available.</p>
      </div>
    )
  }

  return (
    <div className="flex-1 flex flex-col bg-[#0d1117] overflow-hidden">
      <div className="shrink-0 px-8 pt-5 pb-3 flex items-center gap-4 border-b border-[#2d3748]">
        <div className="flex items-center gap-2">
          <div ref={dropdownRef} className="relative">
            <button
              onClick={() => setDropdownOpen(p => !p)}
              className={`flex items-center gap-2 text-sm px-3 py-1.5 rounded-lg border transition cursor-pointer select-none ${
                dropdownOpen
                  ? 'bg-[#1e2a3a] border-[#2E86C1] text-white'
                  : 'bg-[#1a202c] border-[#2d3748] text-[#e2e8f0] hover:border-[#3d4a5c]'
              }`}
            >
              <span className="font-medium">
                Snapshot {effectiveIdx + 1}
                {effectiveIdx === snapshots.length - 1 && (
                  <span className="ml-1.5 text-[0.6rem] font-bold uppercase tracking-wider text-[#2E86C1]">latest</span>
                )}
              </span>
              <svg
                className={`w-3.5 h-3.5 text-slate-400 transition-transform ${dropdownOpen ? 'rotate-180' : ''}`}
                viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2"
              >
                <path d="M4 6l4 4 4-4" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </button>

            {dropdownOpen && (
              <div className="absolute top-full left-0 mt-1 z-50 bg-[#1a202c] border border-[#2d3748] rounded-xl shadow-2xl overflow-hidden min-w-[160px] max-h-60 overflow-y-auto">
                {snapshots.map((snap, i) => (
                  <button
                    key={snap.id}
                    onClick={() => { setSelectedIdx(i); setCollapsed({}); setDropdownOpen(false) }}
                    className={`w-full flex items-center justify-between px-4 py-2 text-sm transition cursor-pointer ${
                      i === effectiveIdx
                        ? 'bg-[#1e3a5f] text-white'
                        : 'text-slate-300 hover:bg-[#252d3d] hover:text-white'
                    }`}
                  >
                    <span>Snapshot {i + 1}</span>
                    {i === snapshots.length - 1 && (
                      <span className="text-[0.6rem] font-bold uppercase tracking-wider text-[#2E86C1] ml-3">latest</span>
                    )}
                  </button>
                ))}
              </div>
            )}
          </div>

          <div className="group relative">
            <div className="w-4 h-4 rounded-full bg-[#2E86C1] text-white text-[10px] font-black flex items-center justify-center cursor-help hover:bg-[#2471a3] transition select-none">
              i
            </div>
            <div className="absolute left-1/2 -translate-x-1/2 top-full mt-2 w-56 bg-[#1a202c] text-slate-300 text-[0.7rem] p-3 rounded-lg shadow-xl opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-50 leading-relaxed border border-[#2d3748]">
              Snapshots are numbered in chronological order — Snapshot 1 is the oldest, the highest number is the latest.
              <div className="absolute bottom-full left-1/2 -translate-x-1/2 border-8 border-transparent border-b-[#1a202c]" />
            </div>
          </div>

          {snapshots[effectiveIdx] && (
            <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-[#1a202c] border border-[#2d3748]">
              <span className="text-[0.65rem] font-bold text-slate-500 uppercase tracking-wider shrink-0">Snapshot ID</span>
              <span className="text-xs font-mono text-slate-300">
                {snapshots[effectiveIdx].parsedDetails.snapshot_id ?? '—'}
              </span>
            </div>
          )}
        </div>

        <input
          type="text"
          placeholder="Search partitions…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          className="flex-1 max-w-xs text-sm bg-[#1a202c] border border-[#2d3748] text-[#e2e8f0] rounded-lg px-3 py-1.5 placeholder-slate-500 focus:outline-none focus:border-[#2E86C1]"
        />

        <div className="ml-auto flex gap-5 text-xs text-slate-400">
          <span>
            <span className="font-semibold text-slate-300">{totalPartitions}</span>
            {' '}partition{totalPartitions !== 1 ? 's' : ''}
          </span>
          <span>
            <span className="font-semibold text-slate-300">{totalFiles}</span>
            {' '}file{totalFiles !== 1 ? 's' : ''}
          </span>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto px-8 py-4 flex flex-col gap-2">
        {totalPartitions === 0 && (
          <p className="text-slate-500 text-sm italic mt-4">
            {search ? 'No partitions match the search.' : 'No data files found for this snapshot.'}
          </p>
        )}
        {filteredPartitions.map(([partition, files]) => (
          <div key={partition} className="bg-[#1a202c] rounded-lg border border-[#2d3748] overflow-hidden">
            <button
              className="w-full flex items-center justify-between px-4 py-2.5 hover:bg-[#252d3d] transition text-left"
              onClick={() => toggleCollapse(partition)}
            >
              <div className="flex items-center gap-3 min-w-0">
                <svg
                  className={`w-3.5 h-3.5 text-[#2E86C1] shrink-0 transition-transform ${collapsed[partition] ? '-rotate-90' : ''}`}
                  viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2.5"
                >
                  <path d="M4 6l4 4 4-4" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
                <span className="text-sm font-mono text-[#e2e8f0] truncate">{partition}</span>
              </div>
              <span className="ml-4 shrink-0 text-[0.65rem] font-bold bg-[#2d3748] text-slate-400 px-2 py-0.5 rounded-full">
                {files.length}
              </span>
            </button>
            {!collapsed[partition] && (
              <div className="border-t border-[#2d3748]">
                {files.map((filePath, i) => (
                  <div
                    key={filePath}
                    className={`px-8 py-1.5 text-xs font-mono text-slate-400 ${i % 2 === 0 ? 'bg-[#0d1117]' : 'bg-[#111820]'}`}
                  >
                    {filePath.split('/').pop()}
                  </div>
                ))}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}
