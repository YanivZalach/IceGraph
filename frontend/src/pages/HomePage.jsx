import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import logo from '../assets/icegraph.png'

export default function HomePage() {
  const [tableName, setTableName] = useState('')
  const navigate = useNavigate()

  function handleSubmit(e) {
    e.preventDefault()
    const params = new URLSearchParams({ table: tableName })
    navigate(`/snapshots-selection?${params.toString()}`)
  }

  return (
    <div className="flex-1 flex flex-col">
      <main className="flex-1 flex items-center justify-center p-8">
        <div className="bg-[#1a202c] rounded-2xl shadow-xl p-10 w-full max-w-lg border border-[#2d3748]">
          <div className="flex flex-col items-center mb-6">
            <img src={logo} alt="IceGraph" className="h-20 w-20 object-contain mb-3" />
            <h1 className="text-2xl font-bold text-[#e2e8f0]">IceGraph</h1>
          </div>

          <h2 className="text-xl font-bold text-[#e2e8f0] mb-1">Visualize a Table</h2>
          <p className="text-slate-400 text-sm mb-7">
            Enter an Iceberg table name to explore its metadata graph.
          </p>

          <form onSubmit={handleSubmit} className="flex flex-col gap-5">
            <div>
              <label className="block text-xs font-bold text-slate-400 uppercase tracking-wider mb-1.5">
                Table Name
              </label>
              <input
                type="text"
                required
                value={tableName}
                onChange={e => setTableName(e.target.value)}
                placeholder="default.my_table"
                className="w-full border border-[#2d3748] bg-[#2d3748] rounded-lg px-4 py-2.5 text-sm text-[#e2e8f0] placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-[#2E86C1]/40 focus:border-[#2E86C1] transition"
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

      <footer className="text-center text-xs text-slate-500 py-4">
        IceGraph — Apache Iceberg Metadata Visualizer
      </footer>
    </div>
  )
}
