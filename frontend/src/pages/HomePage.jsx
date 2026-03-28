import { useState } from 'react'
import { useNavigate } from 'react-router-dom'

export default function HomePage() {
  const [tableName, setTableName] = useState('')
  const [date, setDate] = useState(() => {
    const now = new Date()
    const offsetDate = new Date(now.getTime() - 2 * 60 * 60 * 1000)
    return new Date(offsetDate.getTime() - offsetDate.getTimezoneOffset() * 60000)
      .toISOString()
      .slice(0, 16)
  })
  const navigate = useNavigate()

  function handleSubmit(e) {
    e.preventDefault()
    const params = new URLSearchParams({ table: tableName })
    if (date) params.set('date', date)
    navigate(`/table/graph?${params.toString()}`)
  }

  return (
    <div className="flex-1 flex flex-col">
      {/* Card */}
      <main className="flex-1 flex items-center justify-center p-8">
        <div className="bg-white rounded-2xl shadow-xl p-10 w-full max-w-lg border border-slate-100">
          <h2 className="text-xl font-bold text-[#1e293b] mb-1">Visualize a Table</h2>
          <p className="text-slate-500 text-sm mb-6">Enter an Iceberg table name to explore its metadata graph.</p>

          <form onSubmit={handleSubmit} className="flex flex-col gap-5">
            <div>
              <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-1.5">
                Table Name
              </label>
              <input
                type="text"
                required
                value={tableName}
                onChange={e => setTableName(e.target.value)}
                placeholder="default.my_table"
                className="w-full border border-slate-200 rounded-lg px-4 py-2.5 text-sm text-[#1e293b] focus:outline-none focus:ring-2 focus:ring-[#2E86C1] focus:border-transparent transition"
              />
            </div>
            <div>
              <div className="flex items-center gap-1.5 mb-1.5">
                <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider">
                  As-Of Date
                </label>
                <div className="group relative ml-1">
                  <div className="w-4 h-4 rounded-full bg-[#2E86C1] text-white text-[10px] font-black flex items-center justify-center cursor-help transition-all hover:bg-[#EBF5FB] hover:text-[#2E86C1] shadow-sm border border-[#2E86C1]/20">
                    i
                  </div>
                  <div className="absolute left-1/2 -translate-x-1/2 bottom-full mb-2 w-64 bg-slate-900 text-white text-[0.7rem] p-3 rounded-lg shadow-xl opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-50 leading-relaxed translate-y-2 group-hover:translate-y-0 duration-200">
                    <strong className="text-[#3498db] block mb-1 uppercase tracking-tighter">Time Filter Context</strong>
                    Filters by the timestamp of the <strong>snapshot files</strong>. IceGraph prunes metadata logs, manifests, and data files added after the selected time. If no snapshot exists for a metadata file, the filter applies to the metadata timestamp.
                    <div className="absolute top-full left-1/2 -translate-x-1/2 border-8 border-transparent border-t-slate-900" />
                  </div>
                </div>
              </div>
              <input
                type="datetime-local"
                value={date}
                onChange={e => setDate(e.target.value)}
                className="w-full border border-slate-200 rounded-lg px-4 py-2.5 text-sm text-[#1e293b] focus:outline-none focus:ring-2 focus:ring-[#2E86C1] focus:border-transparent transition"
              />
            </div>
            <button
              type="submit"
              className="bg-[#2E86C1] hover:bg-[#2471a3] text-white font-bold py-3 rounded-lg transition text-sm uppercase tracking-wider"
            >
              Generate Graph
            </button>
          </form>
        </div>
      </main>

      <footer className="text-center text-xs text-slate-400 py-4">
        IceGraph — Apache Iceberg Metadata Visualizer
      </footer>
    </div>
  )
}
