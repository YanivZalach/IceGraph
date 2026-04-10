import { useEffect, useRef, useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'

export default function SnapshotSelectionPage() {
    const [searchParams] = useSearchParams()
    const tableName = searchParams.get('table')

    const startListRef = useRef(null)
    const endListRef = useRef(null)

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
                if (Object.keys(data).length === 0) throw new Error('No snapshots found')
                setSnapshots(data)
                const sorted = Object.entries(data).sort((a, b) => b[0].localeCompare(a[0]))
                setStartSnapshot(sorted[Math.min(sorted.length - 1, 4)][1])
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
            const startTs = Object.keys(snapshots).find(ts => snapshots[ts] === startSnapshot)
            const endTs = Object.keys(snapshots).find(ts => snapshots[ts] === endSnapshot)
            if (startTs && endTs && startTs > endTs) {
                alert('Start snapshot must be before End snapshot')
                return
            }
        }

        const params = new URLSearchParams({ table: tableName })
        if (startSnapshot) params.set('start_snapshot_id', startSnapshot)
        if (endSnapshot) params.set('end_snapshot_id', endSnapshot)
        navigate(`/table/graph?${params.toString()}`)
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
            <div className="flex-1 flex flex-col items-center justify-center text-slate-400">
                <div className="w-10 h-10 border-4 border-slate-700 border-t-[#2E86C1] rounded-full animate-spin mb-4" />
                <p className="text-sm">Loading snapshots for {tableName}…</p>
            </div>
        )
    }

    const entries = Object.entries(snapshots).sort((a, b) => b[0].localeCompare(a[0]))

    return (
        <div className="flex-1 flex items-center justify-center p-8">
            <div className="bg-[#1a202c] rounded-2xl shadow-xl p-10 w-full max-w-4xl border border-[#2d3748]">
                <h2 className="text-xl font-bold text-[#e2e8f0] mb-4">
                    Select Snapshots
                </h2>
                <p className="text-slate-400 text-sm mb-6">
                    Choose a range of snapshots to view <strong>{tableName}</strong> (Inclusive)
                </p>
                <form onSubmit={handleSubmit} className="flex flex-col gap-6">
                    <div className="flex gap-6">
                        <div className="flex-1">
                            <label className="block text-xs font-bold text-slate-400 uppercase mb-2">Start Snapshot</label>
                            <div ref={startListRef} className="h-72 overflow-y-auto bg-[#2d3748] rounded-xl p-2 space-y-2 scroll-py-4">
                                {entries.map(([ts, id]) => (
                                    <div key={id} data-id={id} onClick={() => setStartSnapshot(id)}
                                        className={`p-3 rounded-lg cursor-pointer border ${startSnapshot === id ? 'bg-[#2E86C1] border-[#2E86C1]' : 'bg-[#1a202c] border-[#2d3748]'}`}>
                                        <div className="text-xs text-slate-300">{ts}</div>
                                        <div className={`text-[13px] ${startSnapshot === id ? 'text-blue-200' : 'text-slate-500'} font-mono mt-1 opacity-75`}>ID: {id}</div>
                                    </div>
                                ))}
                                <div data-id="" onClick={() => setStartSnapshot('')}
                                    className={`p-3 rounded-lg cursor-pointer border ${startSnapshot === '' ? 'bg-[#2E86C1] border-[#2E86C1]' : 'bg-[#1a202c] border-[#2d3748]'}`}>
                                    <div className="text-xs font-bold text-white">-- Full History --</div>
                                </div>
                            </div>
                        </div>

                        <div className="flex-1">
                            <label className="block text-xs font-bold text-slate-400 uppercase mb-2">End Snapshot</label>
                            <div ref={endListRef} className="h-72 overflow-y-auto bg-[#2d3748] rounded-xl p-2 space-y-2 scroll-py-4">
                                <div data-id="" onClick={() => setEndSnapshot('')}
                                    className={`p-3 rounded-lg cursor-pointer border ${endSnapshot === '' ? 'bg-[#2E86C1] border-[#2E86C1]' : 'bg-[#1a202c] border-[#2d3748]'}`}>
                                    <div className="text-xs font-bold text-white">-- Latest --</div>
                                </div>
                                {entries.map(([ts, id]) => (
                                    <div key={id} data-id={id} onClick={() => setEndSnapshot(id)}
                                        className={`p-3 rounded-lg cursor-pointer border ${endSnapshot === id ? 'bg-[#2E86C1] border-[#2E86C1]' : 'bg-[#1a202c] border-[#2d3748]'}`}>
                                        <div className="text-xs text-slate-300">{ts}</div>
                                        <div className={`text-[13px] ${endSnapshot === id ? 'text-blue-200' : 'text-slate-500'} font-mono mt-1 opacity-75`}>ID: {id}</div>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>
                    <button type="submit" className="w-full bg-[#2E86C1] hover:bg-[#2471a3] py-3 rounded-lg text-white font-bold transition-colors">
                        Generate Graph
                    </button>
                </form>
            </div>
        </div>
    )
}