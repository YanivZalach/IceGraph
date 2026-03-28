import { NavLink, useLocation, useMatch, useNavigate, useSearchParams } from 'react-router-dom'
import { useTableSpecs } from '../context/TableSpecsContext'

export default function NavBar() {
  const location = useLocation()
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const isTablePage = useMatch('/table/*')
  const tableName = searchParams.get('table')
  const { detailsOpen, setDetailsOpen } = useTableSpecs()

  const tabSearch = location.search

  const tabClass = ({ isActive }) =>
    `text-sm font-medium px-1 py-0.5 border-b-2 transition ${
      isActive
        ? 'border-[#2E86C1] text-white'
        : 'border-transparent text-slate-400 hover:text-white hover:border-slate-500'
    }`

  return (
    <nav className="bg-[#1a202c] text-white px-8 py-3 flex items-center gap-6 shadow-lg shrink-0">
      <span className="text-lg font-bold tracking-tight select-none">🧊 IceGraph</span>

      {!isTablePage && (
        <NavLink to="/" end className={tabClass}>
          Home
        </NavLink>
      )}

      {isTablePage && (
        <>
          {tableName && (
            <button
              className={`text-sm font-mono px-3 py-1 rounded-md border transition ${
                detailsOpen
                  ? 'bg-[#2E86C1] border-[#2E86C1] text-white'
                  : 'bg-transparent border-slate-600 text-slate-300 hover:border-slate-400 hover:text-white'
              }`}
              onClick={() => setDetailsOpen(p => !p)}
            >
              {tableName}
            </button>
          )}

          <div className="w-px h-4 bg-slate-700" />

          <NavLink to={`/table/graph${tabSearch}`} className={tabClass}>
            Graph
          </NavLink>
          <NavLink to={`/table/timeline${tabSearch}`} className={tabClass}>
            Timeline
          </NavLink>

          <button
            className="ml-auto text-sm font-medium text-slate-400 hover:text-white border border-slate-600 hover:border-slate-400 px-3 py-1 rounded-md transition"
            onClick={() => navigate('/')}
          >
            ← Home
          </button>
        </>
      )}
    </nav>
  )
}
