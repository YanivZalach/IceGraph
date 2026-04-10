import { useEffect, useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'

export default function SnapshotSelectionPage() {
    const [searchParams] = useSearchParams()
    const tableName = searchParams.get('table')

    const [snapshots, setSnapshots] = useState({})
    const [startSnapshot, setStartSnapshot] = useState('')
    const [endSnapshot, setEndSnapshot] = useState('')
    const [loading, setLoading] = useState(true)
    const [error, setError] = useState(null)

    const navigate = useNavigate()

    useEffect(() => {
        async function fetchSnapshots() {
            try {
                setLoading(true)
                setError(null)

                const res = await fetch(`/api/v1/snapshot-map/${tableName}`)

                if (!res.ok) {
                    const err = await res.json()
                    throw new Error(err.error || 'Failed to fetch snapshots')
                }

                const data = await res.json()

                if (!data || Object.keys(data).length === 0) {
                    throw new Error('No snapshots found for this table')
                }

                setSnapshots(data)
            } catch (e) {
                console.error('Failed to fetch snapshots', e)
                setError(e.message)
            } finally {
                setLoading(false)
            }
        }

        if (tableName) {
            fetchSnapshots()
        } else {
            setError('Missing table name')
            setLoading(false)
        }
    }, [tableName])

    function handleSubmit(e) {
        e.preventDefault()

        if (startSnapshot && endSnapshot && startSnapshot > endSnapshot) {
            alert('Start snapshot must be before end snapshot')
            return
        }

        const params = new URLSearchParams({ table: tableName })

        if (startSnapshot) params.set('start_snapshot_id', startSnapshot)
        if (endSnapshot) params.set('end_snapshot_id', endSnapshot)

        navigate(`/table/graph?${params.toString()}`)
    }

    // 🔴 ERROR STATE
    if (error) {
        return (
            <div className="flex-1 flex items-center justify-center p-8">
                <div className="bg-red-950/50 border border-red-800 text-red-400 px-8 py-6 rounded-xl text-center max-w-sm">

                    <h2 className="text-red-400 text-lg font-bold mb-2">
                        Failed to Load Snapshots
                    </h2>

                    <p className="text-slate-400 text-xs mb-2">
                        Table: {tableName || 'N/A'}
                    </p>

                    <p className="text-slate-300 text-sm mb-4">
                        {error}
                    </p>

                    <div className="flex justify-center gap-3">
                        <button
                            onClick={() => window.location.reload()}
                            className="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-lg text-sm"
                        >
                            Retry
                        </button>

                        <button
                            onClick={() => navigate('/')}
                            className="text-slate-400 hover:text-white text-sm"
                        >
                            Go Back
                        </button>
                    </div>

                </div>
            </div>
        )
    }

    if (loading) {
        return (
            <div className="flex-1 flex flex-col items-center justify-center bg-[#0d1117]">
                <div className="w-10 h-10 border-4 border-[#2d3748] border-t-[#2E86C1] rounded-full animate-spin mb-4" />
                <p className="text-slate-400 text-sm">
                    Loading snapshots for <strong>{tableName}</strong>…
                </p>
            </div>
        )
    }

    const entries = Object.entries(snapshots).sort((a, b) =>
        b[0].localeCompare(a[0])
    )

    return (
        <div className="flex-1 flex items-center justify-center p-8">
            <div className="bg-[#1a202c] rounded-2xl shadow-xl p-10 w-full max-w-lg border border-[#2d3748]">

                <h2 className="text-xl font-bold text-[#e2e8f0] mb-4">
                    Select Snapshots
                </h2>

                <p className="text-slate-400 text-sm mb-6">
                    Choose a range of snapshots for <strong>{tableName}</strong>
                </p>

                <form onSubmit={handleSubmit} className="flex flex-col gap-5">

                    <div>
                        <label className="block text-xs font-bold text-slate-400 uppercase mb-1.5">
                            Start Snapshot
                        </label>
                        <select
                            value={startSnapshot}
                            onChange={e => setStartSnapshot(e.target.value)}
                            className="w-full bg-[#2d3748] text-white rounded-lg px-3 py-2"
                        >
                            <option value="">-- None (Full History) --</option>
                            {entries.map(([ts, id]) => (
                                <option key={id} value={id}>
                                    {ts} ({id})
                                </option>
                            ))}
                        </select>
                    </div>

                    <div>
                        <label className="block text-xs font-bold text-slate-400 uppercase mb-1.5">
                            End Snapshot
                        </label>
                        <select
                            value={endSnapshot}
                            onChange={e => setEndSnapshot(e.target.value)}
                            className="w-full bg-[#2d3748] text-white rounded-lg px-3 py-2"
                        >
                            <option value="">-- Latest --</option>
                            {entries.map(([ts, id]) => (
                                <option key={id} value={id}>
                                    {ts} ({id})
                                </option>
                            ))}
                        </select>
                    </div>

                    <button
                        type="submit"
                        className="bg-[#2E86C1] hover:bg-[#2471a3] text-white font-bold py-2.5 rounded-lg"
                    >
                        Continue to Graph
                    </button>

                </form>
            </div>
        </div>
    )
}