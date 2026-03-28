import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import logo from '../assets/icegraph.png'

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
      <main className="flex-1 flex items-center justify-center p-8">
        <div className="bg-white rounded-2xl shadow-xl p-10 w-full max-w-lg border border-slate-100">
          <div className="flex flex-col items-center mb-6">
            <img src={logo} alt="IceGraph" className="h-20 w-20 object-contain mb-3" />
            <h1 className="text-2xl font-bold text-[#1e293b]">IceGraph</h1>
          </div>

          <h2 className="text-xl font-bold text-[#1e293b] mb-1">Visualize a Table</h2>
          <p className="text-slate-500 text-sm mb-7">
            Enter an Iceberg table name to explore its metadata graph.
          </p>

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
                className="w-full border border-slate-200 rounded-lg px-4 py-2.5 text-sm text-[#1e293b] placeholder-slate-300 focus:outline-none focus:ring-2 focus:ring-[#2E86C1]/40 focus:border-[#2E86C1] transition"
              />
            </div>

            <div>
              <div className="flex items-center gap-1.5 mb-1.5">
                <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider">
                  As-Of Date
                </label>
                <div className="group relative ml-1">
                  <div className="w-4 h-4 rounded-full bg-[#2E86C1] text-white text-[10px] font-black flex items-center justify-center cursor-help transition hover:bg-[#2471a3] select-none">
                    i
                  </div>
                  <div className="absolute left-1/2 -translate-x-1/2 bottom-full mb-2 w-64 bg-[#1a202c] text-slate-300 text-[0.7rem] p-3 rounded-lg shadow-xl opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none z-50 leading-relaxed">
                    <strong className="text-[#2E86C1] block mb-1 uppercase tracking-wide text-[0.65rem]">
                      Time Filter
                    </strong>
                    Filters by the timestamp of <strong className="text-white">snapshot files</strong>.
                    IceGraph prunes metadata, manifests, and data files added after the selected time.
                    <div className="absolute top-full left-1/2 -translate-x-1/2 border-8 border-transparent border-t-[#1a202c]" />
                  </div>
                </div>
              </div>
              <input
                type="datetime-local"
                value={date}
                onChange={e => setDate(e.target.value)}
                className="w-full border border-slate-200 rounded-lg px-4 py-2.5 text-sm text-[#1e293b] focus:outline-none focus:ring-2 focus:ring-[#2E86C1]/40 focus:border-[#2E86C1] transition"
              />
            </div>

            <button
              type="submit"
              className="bg-[#2E86C1] hover:bg-[#2471a3] active:bg-[#1a5c8a] text-white font-bold py-2.5 rounded-lg transition text-sm tracking-wide mt-1"
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
