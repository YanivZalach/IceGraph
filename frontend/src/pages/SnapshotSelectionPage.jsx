import { useEffect, useRef, useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { UI_BODY_MUTED_CLASS, UI_FORM_LABEL_MB_CLASS, UI_HELPER_TEXT_CLASS, UI_PAGE_TITLE_CLASS } from '../uiTypography'
import { parseUtcDate, formatLocaleDateTime } from '../utils/dateUtils'

function SnapshotItem({ ts, id, operation, selectedId, onClick }) {
    const isSelected = selectedId === id
    return (
        <div key={id} data-id={id} onClick={() => onClick(id)}
            className={`p-3 rounded-lg cursor-pointer border ${isSelected ? 'bg-accent border-accent' : 'bg-surface border-edge'}`}>
            <div className="flex justify-between items-start">
                <div className="text-xs text-slate-300">{ts}</div>
                {operation && (
                    <span className={`text-xs uppercase font-bold px-1.5 py-0.5 rounded ${operation === 'overwrite' ? 'bg-blue-950/80 text-blue-400 border border-blue-800' :
                        operation === 'append' ? 'bg-emerald-950/80 text-emerald-400 border border-emerald-800' :
                            operation === 'replace' ? 'bg-amber-950/80 text-amber-400 border border-amber-800' :
                                operation === 'delete' ? 'bg-rose-950/80 text-rose-400 border border-rose-800' :
                                    'bg-slate-800 text-slate-400 border border-slate-700'
                        }`}>
                        {operation}
                    </span>
                )}
            </div>
            <div className={`text-sm ${isSelected ? 'text-blue-200' : 'text-slate-500'} font-mono mt-1 opacity-75`}>ID: {id}</div>
        </div>
    )
}

export default function SnapshotSelectionPage() {
    const [searchParams] = useSearchParams()
    const tableName = searchParams.get('table')

    const startListRef = useRef(null)
    const endListRef = useRef(null)
    const submitBtnRef = useRef(null)

    useEffect(() => {
        const handleKey = (e) => {
            if (e.key === 'Enter' && !['INPUT', 'TEXTAREA', 'SELECT'].includes(e.target.tagName))
                submitBtnRef.current?.click()
        }
        window.addEventListener('keydown', handleKey)
        return () => window.removeEventListener('keydown', handleKey)
    }, [])

    const [snapshots, setSnapshots] = useState({})
    const [startSnapshot, setStartSnapshot] = useState('')
    const [endSnapshot, setEndSnapshot] = useState('')
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState(null)

    const navigate = useNavigate()

    const scrollToActive = (containerRef, activeId) => {
        if (!containerRef.current) return
        const activeElement = containerRef.current.querySelector(`[data-id="${activeId}"]`)
        if (activeElement) {
            activeElement.scrollIntoView({ block: 'nearest', behavior: 'smooth' })
        }
    }

    useEffect(() => {
        if (!tableName) {
            setError('Missing table name')
            setLoading(false)
            return
        }

        fetch(`/api/v1/snapshot-map/${tableName}`)
            .then(async (res) => {
                const data = await res.json()
                if (!res.ok || data.error) throw new Error(data.error || 'Failed to fetch snapshots')
                return data
            })
            .then((data) => {
                if (Object.keys(data).length === 0) {
                    setSnapshots({});
                    setLoading(false);
                    return;
                }
                setSnapshots(data)
                const sorted = Object.entries(data).sort((a, b) => b[0].localeCompare(a[0]))
                setStartSnapshot(sorted[Math.min(sorted.length - 1, 4)][1].snapshot_id)
                setEndSnapshot('')
                setLoading(false)
            })
            .catch((e) => {
                setError(e.message)
                setLoading(false)
            })
    }, [tableName])

    useEffect(() => {
        if (!loading) {
            setTimeout(() => {
                scrollToActive(startListRef, startSnapshot)
                scrollToActive(endListRef, endSnapshot)
            }, 100)
        }
    }, [loading, startSnapshot, endSnapshot])

    function handleSubmit(e) {
        e.preventDefault()
        if (startSnapshot && endSnapshot) {
            const startTs = Object.keys(snapshots).find(ts => snapshots[ts]?.snapshot_id === startSnapshot)
            const endTs = Object.keys(snapshots).find(ts => snapshots[ts]?.snapshot_id === endSnapshot)
            if (startTs && endTs && startTs > endTs) {
                alert('Start snapshot must be before End snapshot')
                return
            }
        }

        const params = new URLSearchParams({ table: tableName })
        if (startSnapshot) params.set('start_snapshot_id', startSnapshot)
        if (endSnapshot) params.set('end_snapshot_id', endSnapshot)
        navigate(`/table/timeline?${params.toString()}`)
    }

    function handleJumpToLatest(latestSnapshotId) {
        const params = new URLSearchParams({ table: tableName, start_snapshot_id: latestSnapshotId })
        navigate(`/table/metadata?${params.toString()}`)
    }

    if (error) {
        return (
            <div className="flex-1 flex items-center justify-center p-8 text-red-400">
                <div className="bg-red-950/50 border border-red-800 p-8 rounded-xl text-center">
                    <h2 className="font-bold mb-2">Failed to Load Snapshots</h2>
                    <p className="text-sm mb-4">{error}</p>
                    <button onClick={() => navigate('/')} className="text-slate-400 hover:text-white text-sm">Go Back</button>
                </div>
            </div>
        )
    }

    if (loading) {
        return (
            <div className={`flex-1 flex flex-col items-center justify-center ${UI_BODY_MUTED_CLASS}`}>
                <div className="w-10 h-10 border-4 border-slate-700 border-t-accent rounded-full animate-spin mb-4" />
                <p>Loading snapshots for {tableName}…</p>
            </div>
        )
    }

    const entries = Object.entries(snapshots)
        .sort((a, b) => b[0].localeCompare(a[0]))
        .map(([ts, val]) => {
            const dateObj = parseUtcDate(ts);
            const readableTs = dateObj ? formatLocaleDateTime(dateObj) : ts;
            return [readableTs, val.snapshot_id, val.operation];
        });
    const latestSnapshotId = entries[0]?.[1]

    return (
        <div className="flex-1 flex items-center justify-center p-8">
            <div className="bg-surface rounded-2xl shadow-xl p-10 w-full max-w-5xl border border-edge">
                <h2 className={`${UI_PAGE_TITLE_CLASS} mb-4`}>
                    Select Snapshots
                </h2>
                <p className={`${UI_BODY_MUTED_CLASS} mb-6`}>
                    Choose a range of snapshots to view <strong>{tableName}</strong> (Inclusive)
                </p>
                <form onSubmit={handleSubmit} className="flex flex-col gap-6">
                    <div className="flex gap-6">
                        {latestSnapshotId && (
                            <div className="w-44 shrink-0 flex flex-col justify-center items-center gap-3 border-r border-dashed border-edge pr-6">
                                <button
                                    type="button"
                                    onClick={() => handleJumpToLatest(latestSnapshotId)}
                                    className="w-full rounded-xl border-2 border-accent text-accent font-bold text-sm px-3 py-3 hover:bg-accent/10 active:bg-accent/20 transition-colors"
                                >
                                    Latest Metadata Only
                                </button>
                                <p className={`${UI_HELPER_TEXT_CLASS} text-center`}>
                                    Skip range selection and jump straight to the table's current state
                                </p>
                            </div>
                        )}
                        <div className="flex-1">
                            <label className={UI_FORM_LABEL_MB_CLASS}>Start Snapshot</label>
                            <div ref={startListRef} className="h-72 overflow-y-auto bg-edge rounded-xl p-2 space-y-2 scroll-py-4">
                                {entries.map(([ts, id, operation]) => (
                                    <SnapshotItem
                                        key={id}
                                        ts={ts}
                                        id={id}
                                        operation={operation}
                                        selectedId={startSnapshot}
                                        onClick={setStartSnapshot}
                                    />
                                ))}
                                <div data-id="" onClick={() => setStartSnapshot('')}
                                    className={`p-3 rounded-lg cursor-pointer border ${startSnapshot === '' ? 'bg-accent border-accent' : 'bg-surface border-edge'}`}>
                                    <div className="text-xs font-bold text-white">-- Full History --</div>
                                </div>
                            </div>
                        </div>

                        <div className="flex-1">
                            <label className={UI_FORM_LABEL_MB_CLASS}>End Snapshot</label>
                            <div ref={endListRef} className="h-72 overflow-y-auto bg-edge rounded-xl p-2 space-y-2 scroll-py-4">
                                <div data-id="" onClick={() => setEndSnapshot('')}
                                    className={`p-3 rounded-lg cursor-pointer border ${endSnapshot === '' ? 'bg-accent border-accent' : 'bg-surface border-edge'}`}>
                                    <div className="text-xs font-bold text-white">-- Latest --</div>
                                </div>
                                {entries.map(([ts, id, operation]) => (
                                    <SnapshotItem
                                        key={id}
                                        ts={ts}
                                        id={id}
                                        operation={operation}
                                        selectedId={endSnapshot}
                                        onClick={setEndSnapshot}
                                    />
                                ))}
                            </div>
                        </div>
                    </div>
                    <button ref={submitBtnRef} type="submit" className="w-full bg-accent hover:bg-accent-dark py-3 rounded-lg text-white font-bold transition-colors">
                        Generate Graph
                    </button>
                </form>
            </div>
        </div>
    )
}