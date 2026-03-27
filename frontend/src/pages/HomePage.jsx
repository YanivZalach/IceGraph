import { useState } from 'react'
import { useNavigate } from 'react-router-dom'

export default function HomePage() {
  const [tableName, setTableName] = useState('')
  const [date, setDate] = useState('')
  const navigate = useNavigate()

  function handleSubmit(e) {
    e.preventDefault()
    const params = new URLSearchParams({ table: tableName })
    if (date) params.set('date', date)
    navigate(`/graph?${params.toString()}`)
  }

  return (
    <div className="min-h-screen bg-[#f8fafc] flex flex-col">
      {/* Header */}
      <header className="bg-[#1a202c] text-white py-4 px-8 shadow-lg">
        <h1 className="text-2xl font-bold tracking-tight">🧊 IceGraph</h1>
        <p className="text-slate-400 text-sm mt-1">Apache Iceberg Table Visualizer</p>
      </header>

      {/* Card */}
      <main className="flex-1 flex items-center justify-center p-8">
        <div className="bg-white rounded-2xl shadow-xl p-10 w-full max-w-lg border border-slate-100">
          <h2 className="text-xl font-bold text-[#1e293b] mb-1">Visualize a Table</h2>
          <p className="text-slate-500 text-sm mb-6">Enter your Iceberg table name to explore its metadata graph.</p>

          <form onSubmit={handleSubmit} className="flex flex-col gap-5">
            <div>
              <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-1.5">
                Table Name *
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
              <label className="block text-xs font-bold text-slate-500 uppercase tracking-wider mb-1.5">
                As-Of Date <span className="text-slate-400 font-normal">(optional)</span>
              </label>
              <input
                type="date"
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
