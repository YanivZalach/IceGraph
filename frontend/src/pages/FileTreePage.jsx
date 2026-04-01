import { useEffect, useMemo, useRef, useState } from 'react'
import { useLocation, useNavigate, useOutletContext } from 'react-router-dom'
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

function Dropdown({ triggerLabel, isOpen, onToggle, dropdownRef, children }) {
  return (
    <div ref={dropdownRef} className="relative">
      <button
        onClick={onToggle}
        className={`flex items-center gap-2 text-sm px-3 py-1.5 rounded-lg border transition cursor-pointer select-none ${isOpen
            ? 'bg-[#1e2a3a] border-[#2E86C1] text-white'
            : 'bg-[#1a202c] border-[#2d3748] text-[#e2e8f0] hover:border-[#3d4a5c]'
          }`}
      >
        <span className="font-medium">{triggerLabel}</span>
        <svg
          className={`w-3.5 h-3.5 text-slate-400 transition-transform ${isOpen ? 'rotate-180' : ''}`}
          viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2"
        >
          <path d="M4 6l4 4 4-4" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </button>
      {isOpen && (
        <div className="absolute top-full left-0 mt-1 z-50 bg-[#1a202c] border border-[#2d3748] rounded-xl shadow-2xl overflow-hidden min-w-[160px] max-h-60 overflow-y-auto">
          {children}
        </div>
      )}
    </div>
  )
}

function DropdownItem({ label, badge, active, onClick }) {
  return (
    <button
      onClick={onClick}
      className={`w-full flex items-center justify-between px-4 py-2 text-sm transition cursor-pointer ${active ? 'bg-[#1e3a5f] text-white' : 'text-slate-300 hover:bg-[#252d3d] hover:text-white'
        }`}
    >
      <span>{label}</span>
      {badge && <span className="text-[0.6rem] font-bold uppercase tracking-wider text-[#2E86C1] ml-3">{badge}</span>}
    </button>
  )
}

export default function FileTreePage() {
  const { nodes, edges, metadata } = useOutletContext()
  const navigate = useNavigate()
  const { search: tabSearch } = useLocation()
  const [search, setSearch] = useState('')
  const [selectedBranch, setSelectedBranch] = useState(null) // null = all
  const [selectedIdx, setSelectedIdx] = useState(null)
  const [collapsed, setCollapsed] = useState({})
  const [checkedFiles, setCheckedFiles] = useState(new Set())
  const [copied, setCopied] = useState(false)
  const [branchDropdownOpen, setBranchDropdownOpen] = useState(false)
  const [snapshotDropdownOpen, setSnapshotDropdownOpen] = useState(false)
  const branchDropdownRef = useRef(null)
  const snapshotDropdownRef = useRef(null)

  useEffect(() => {
    if (!branchDropdownOpen) return
    const handler = (e) => { if (branchDropdownRef.current && !branchDropdownRef.current.contains(e.target)) setBranchDropdownOpen(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [branchDropdownOpen])

  useEffect(() => {
    if (!snapshotDropdownOpen) return
    const handler = (e) => { if (snapshotDropdownRef.current && !snapshotDropdownRef.current.contains(e.target)) setSnapshotDropdownOpen(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [snapshotDropdownOpen])

  const { snapshots, adjacency, nodeById, snapshotById } = useMemo(() => {
    const allNodes = nodes.get()
    const allEdges = edges.get()

    const byId = {}
    for (const n of allNodes) byId[n.id] = n

    const snaps = allNodes
      .filter(n => n.type === FileType.SNAPSHOT)
      .map(n => ({ ...n, parsedDetails: parseDetails(n.details) }))
      .sort((a, b) => new Date(a.parsedDetails.timestamp || 0) - new Date(b.parsedDetails.timestamp || 0))

    const snapById = {}
    for (const s of snaps) {
      if (s.parsedDetails.snapshot_id) snapById[s.parsedDetails.snapshot_id] = s
    }

    const adj = {}
    for (const e of allEdges) {
      if (!adj[e.from]) adj[e.from] = []
      adj[e.from].push({ to: e.to, is_deleted: !!e.is_deleted })
    }

    return { snapshots: snaps, adjacency: adj, nodeById: byId, snapshotById: snapById }
  }, [nodes, edges])

  const branches = useMemo(() => {
    if (!metadata?.refs) return []
    return Object.entries(metadata.refs)
      .filter(([, ref]) => ref.type === 'branch')
      .map(([name, ref]) => ({ name, headSnapshotId: String(ref['snapshot-id']) }))
      .sort((a, b) => a.name.localeCompare(b.name))
  }, [metadata])

  const displayedSnapshots = useMemo(() => {
    if (!selectedBranch) return snapshots
    const branch = branches.find(b => b.name === selectedBranch)
    if (!branch) return snapshots

    // Trace parent_id chain from branch HEAD to collect all ancestor snapshots
    const result = []
    const visited = new Set()
    let currentId = branch.headSnapshotId
    while (currentId && !visited.has(currentId)) {
      visited.add(currentId)
      const node = snapshotById[currentId]
      if (!node) break
      result.push(node)
      currentId = node.parsedDetails.parent_id
    }
    return result.reverse()
  }, [selectedBranch, branches, snapshots, snapshotById])

  const effectiveIdx = selectedIdx !== null ? selectedIdx : displayedSnapshots.length - 1

  const partitionMap = useMemo(() => {
    if (displayedSnapshots.length === 0) return {}
    const snapshot = displayedSnapshots[effectiveIdx]
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
  }, [displayedSnapshots, effectiveIdx, adjacency, nodeById])

  const filteredPartitions = useMemo(() => {
    const q = search.trim().toLowerCase()
    return Object.entries(partitionMap)
      .filter(([part]) => !q || part.toLowerCase().includes(q))
      .sort(([a], [b]) => b.localeCompare(a))
  }, [partitionMap, search])

  const totalPartitions = filteredPartitions.length
  const totalFiles = filteredPartitions.reduce((sum, [, f]) => sum + f.length, 0)

  const resetSelection = () => { setSelectedIdx(null); setCollapsed({}); setCheckedFiles(new Set()) }

  const toggleCollapse = (partition) =>
    setCollapsed(prev => ({ ...prev, [partition]: !prev[partition] }))

  const toggleFile = (path) =>
    setCheckedFiles(prev => {
      const next = new Set(prev)
      next.has(path) ? next.delete(path) : next.add(path)
      return next
    })

  const togglePartition = (files) => {
    const allChecked = files.every(f => checkedFiles.has(f))
    setCheckedFiles(prev => {
      const next = new Set(prev)
      files.forEach(f => allChecked ? next.delete(f) : next.add(f))
      return next
    })
  }

  const copyPaths = () => {
    navigator.clipboard.writeText([...checkedFiles].join('\n'))
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  if (snapshots.length === 0) {
    return (
      <div className="flex-1 flex items-center justify-center bg-[#0d1117]">
        <p className="text-slate-500 text-sm italic">No snapshots available.</p>
      </div>
    )
  }

  const currentSnapshot = displayedSnapshots[effectiveIdx]

  return (
    <div className="flex-1 flex flex-col bg-[#0d1117] overflow-hidden">
      <div className="shrink-0 px-4 sm:px-8 pt-5 pb-3 flex items-center flex-wrap gap-x-4 gap-y-2 border-b border-[#2d3748]">
        <div className="flex items-center flex-wrap gap-2">

          {branches.length > 0 && (
            <>
              <Dropdown
                dropdownRef={branchDropdownRef}
                isOpen={branchDropdownOpen}
                onToggle={() => setBranchDropdownOpen(p => !p)}
                triggerLabel={
                  selectedBranch
                    ? <>{selectedBranch}</>
                    : <span className="text-slate-400">All branches</span>
                }
              >
                <DropdownItem
                  label="All branches"
                  active={selectedBranch === null}
                  onClick={() => { setSelectedBranch(null); resetSelection(); setBranchDropdownOpen(false) }}
                />
                <div className="h-px bg-[#2d3748] mx-2" />
                {branches.map(b => (
                  <DropdownItem
                    key={b.name}
                    label={b.name}
                    active={selectedBranch === b.name}
                    onClick={() => { setSelectedBranch(b.name); resetSelection(); setBranchDropdownOpen(false) }}
                  />
                ))}
              </Dropdown>

              <div className="w-px h-4 bg-slate-700" />
            </>
          )}

          <Dropdown
            dropdownRef={snapshotDropdownRef}
            isOpen={snapshotDropdownOpen}
            onToggle={() => setSnapshotDropdownOpen(p => !p)}
            triggerLabel={
              <>
                Snapshot {effectiveIdx + 1}
                {effectiveIdx === displayedSnapshots.length - 1 && (
                  <span className="ml-1.5 text-[0.6rem] font-bold uppercase tracking-wider text-[#2E86C1]">latest</span>
                )}
              </>
            }
          >
            {displayedSnapshots.map((snap, i) => (
              <DropdownItem
                key={snap.id}
                label={`Snapshot ${i + 1}`}
                badge={i === displayedSnapshots.length - 1 ? 'latest' : null}
                active={i === effectiveIdx}
                onClick={() => { setSelectedIdx(i); setCollapsed({}); setCheckedFiles(new Set()); setSnapshotDropdownOpen(false) }}
              />
            ))}
          </Dropdown>

          <div className="group relative">
            <div className="w-4 h-4 rounded-full bg-[#2E86C1] text-white text-[10px] font-black flex items-center justify-center cursor-help hover:bg-[#2471a3] transition select-none">
              i
            </div>
            <div className="absolute left-1/2 -translate-x-1/2 top-full mt-2 w-56 bg-[#1a202c] text-slate-300 text-[0.7rem] p-3 rounded-lg shadow-xl opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-50 leading-relaxed border border-[#2d3748]">
              Snapshots are numbered in chronological order — Snapshot 1 is the oldest, the highest number is the latest.
              <div className="absolute bottom-full left-1/2 -translate-x-1/2 border-8 border-transparent border-b-[#1a202c]" />
            </div>
          </div>

          {currentSnapshot && (
            <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-[#1a202c] border border-[#2d3748]">
              <span className="text-[0.65rem] font-bold text-slate-500 uppercase tracking-wider shrink-0">Snapshot ID</span>
              <span className="text-xs font-mono text-slate-300">
                {currentSnapshot.parsedDetails.snapshot_id ?? '—'}
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

        <div className="ml-auto flex items-center gap-3">
          <div className="flex gap-5 text-xs text-slate-400">
            <span><span className="font-semibold text-slate-300">{totalPartitions}</span> partition{totalPartitions !== 1 ? 's' : ''}</span>
            <span><span className="font-semibold text-slate-300">{totalFiles}</span> file{totalFiles !== 1 ? 's' : ''}</span>
          </div>

          <div className="w-px h-4 bg-slate-700" />

          <button
            onClick={() => setCheckedFiles(new Set(Object.values(partitionMap).flat()))}
            disabled={totalFiles === 0}
            className="text-sm px-3 py-1.5 rounded-lg border border-[#2d3748] text-slate-400 hover:border-slate-500 hover:text-slate-200 transition disabled:opacity-30 disabled:cursor-not-allowed cursor-pointer"
          >
            Select all
          </button>
          <button
            onClick={() => setCheckedFiles(new Set())}
            disabled={checkedFiles.size === 0}
            className="text-sm px-3 py-1.5 rounded-lg border border-[#2d3748] text-slate-400 hover:border-slate-500 hover:text-slate-200 transition disabled:opacity-30 disabled:cursor-not-allowed cursor-pointer"
          >
            Clear
          </button>

          <div className="w-px h-4 bg-slate-700" />

          <button
            onClick={copyPaths}
            disabled={checkedFiles.size === 0}
            className={`flex items-center gap-2 text-sm px-3 py-1.5 rounded-lg border transition ${checkedFiles.size === 0
                ? 'border-[#2d3748] text-slate-600 cursor-not-allowed'
                : copied
                  ? 'border-green-600 bg-green-900/30 text-green-400'
                  : 'border-[#2E86C1] text-[#2E86C1] hover:bg-[#1e3a5f] cursor-pointer'
              }`}
          >
            {copied ? (
              <>
                <svg className="w-3.5 h-3.5" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2">
                  <path d="M3 8l3.5 3.5L13 4" strokeLinecap="round" strokeLinejoin="round" />
                </svg>
                Copied!
              </>
            ) : (
              <>
                <svg className="w-3.5 h-3.5" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.8">
                  <rect x="5" y="5" width="8" height="9" rx="1.5" />
                  <path d="M11 5V4a1 1 0 0 0-1-1H4a1 1 0 0 0-1 1v8a1 1 0 0 0 1 1h1" strokeLinecap="round" />
                </svg>
                Copy paths {checkedFiles.size > 0 && <span className="text-xs font-bold">({checkedFiles.size})</span>}
              </>
            )}
          </button>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto px-4 sm:px-8 py-4 flex flex-col gap-2">
        {totalPartitions === 0 && (
          <p className="text-slate-500 text-sm italic mt-4">
            {search ? 'No partitions match the search.' : 'No data files found for this snapshot.'}
          </p>
        )}
        {filteredPartitions.map(([partition, files]) => {
          const allChecked = files.every(f => checkedFiles.has(f))
          const someChecked = !allChecked && files.some(f => checkedFiles.has(f))
          return (
            <div key={partition} className="bg-[#1a202c] rounded-lg border border-[#2d3748] overflow-hidden">
              <div className="flex items-center px-4 py-2.5 hover:bg-[#252d3d] transition">
                <button
                  className="flex items-center gap-3 min-w-0 flex-1 text-left"
                  onClick={() => toggleCollapse(partition)}
                >
                  <svg
                    className={`w-3.5 h-3.5 text-[#2E86C1] shrink-0 transition-transform ${collapsed[partition] ? '-rotate-90' : ''}`}
                    viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="2.5"
                  >
                    <path d="M4 6l4 4 4-4" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                  <span className="text-sm font-mono text-[#e2e8f0] truncate">{partition}</span>
                </button>
                <div className="flex items-center gap-3 ml-4 shrink-0">
                  <span className="text-[0.65rem] font-bold bg-[#2d3748] text-slate-400 px-2 py-0.5 rounded-full">
                    {files.length}
                  </span>
                  <input
                    type="checkbox"
                    checked={allChecked}
                    ref={el => { if (el) el.indeterminate = someChecked }}
                    onChange={() => togglePartition(files)}
                    className="w-3.5 h-3.5 rounded accent-[#2E86C1] cursor-pointer"
                    title="Select all in partition"
                  />
                </div>
              </div>
              {!collapsed[partition] && (
                <div className="border-t border-[#2d3748] px-4 py-2 flex flex-col gap-1">
                  {files.map((filePath) => (
                    <div
                      key={filePath}
                      onClick={() => toggleFile(filePath)}
                      className={`flex items-center gap-2.5 px-3 py-2 rounded-md border transition cursor-pointer group ${checkedFiles.has(filePath)
                          ? 'bg-[#1e3a5f] border-[#2E86C1]/40'
                          : 'bg-[#0d1117] border-transparent hover:bg-[#131c2b] hover:border-[#2d3748]'
                        }`}
                    >
                      <input
                        type="checkbox"
                        checked={checkedFiles.has(filePath)}
                        onChange={() => toggleFile(filePath)}
                        onClick={e => e.stopPropagation()}
                        className="w-3.5 h-3.5 rounded accent-[#2E86C1] cursor-pointer shrink-0"
                      />
                      <span
                        className={`text-xs font-mono transition-colors overflow-hidden whitespace-nowrap flex-1 ${checkedFiles.has(filePath) ? 'text-slate-200' : 'text-slate-400 group-hover:text-slate-200'
                          }`}
                        style={{ direction: 'rtl', textOverflow: 'ellipsis' }}
                        title={filePath}
                      >
                        {filePath}
                      </span>
                      <button
                        onClick={e => { e.stopPropagation(); navigate(`/table/graph${tabSearch}`, { state: { selectNodeId: filePath } }) }}
                        title="View in graph"
                        className="shrink-0 ml-2 p-1 rounded text-slate-500 hover:text-[#2E86C1] hover:bg-[#1e3a5f] transition-colors"
                      >
                        <svg className="w-3.5 h-3.5" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.8">
                          <circle cx="4" cy="8" r="2" />
                          <circle cx="12" cy="4" r="2" />
                          <circle cx="12" cy="12" r="2" />
                          <path d="M6 7.2L10 4.8M6 8.8L10 11.2" strokeLinecap="round" />
                        </svg>
                      </button>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}
