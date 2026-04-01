import { useEffect, useRef, useState } from 'react'
import { NavLink, useLocation, useMatch, useNavigate, useSearchParams } from 'react-router-dom'
import { useTableSpecs } from '../context/TableSpecsContext'
import logo from '../assets/icegraph.png'

export default function NavBar() {
  const location = useLocation()
  const [searchParams] = useSearchParams()
  const navigate = useNavigate()
  const isTablePage = useMatch('/table/*')
  const tableName = searchParams.get('table')
  const { detailsOpen, setDetailsOpen } = useTableSpecs()
  const [aboutOpen, setAboutOpen] = useState(false)
  const [menuOpen, setMenuOpen] = useState(false)
  const navRef = useRef(null)

  useEffect(() => {
    if (!aboutOpen) return
    const handleKey = (e) => { if (e.key === 'Escape') setAboutOpen(false) }
    window.addEventListener('keydown', handleKey)
    return () => window.removeEventListener('keydown', handleKey)
  }, [aboutOpen])

  useEffect(() => {
    if (!menuOpen) return
    const handler = (e) => { if (navRef.current && !navRef.current.contains(e.target)) setMenuOpen(false) }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [menuOpen])

  useEffect(() => { setMenuOpen(false) }, [location.pathname, location.search])

  const tabSearch = location.search

  const tabClass = ({ isActive }) =>
    `text-sm font-medium px-1 py-0.5 border-b-2 transition ${isActive
      ? 'border-[#2E86C1] text-white'
      : 'border-transparent text-slate-400 hover:text-white hover:border-slate-500'
    }`

  const mobileTabClass = ({ isActive }) =>
    `text-sm font-medium px-3 py-2 rounded-md transition text-left ${isActive
      ? 'bg-[#1e3a5f] text-white'
      : 'text-slate-400 hover:text-white hover:bg-[#252d3d]'
    }`

  return (
    <>
      <nav ref={navRef} className="bg-[#1a202c] text-white shadow-lg shrink-0 sticky top-0 z-50">

        <div className="px-4 sm:px-6 py-3 flex items-center gap-4">

          <div className="flex items-center gap-2 select-none shrink-0">
            <img src={logo} alt="IceGraph" className="h-10 w-10 object-contain" />
            <span className="text-lg font-bold tracking-tight">IceGraph</span>
          </div>

          {!isTablePage && (
            <NavLink to="/" end className={tabClass}>
              Home
            </NavLink>
          )}

          {isTablePage && (
            <div className="hidden md:flex items-center gap-4 flex-1">
              {tableName && (
                <button
                  className="text-sm font-mono px-3 py-1 rounded-md border border-slate-600 text-slate-300 hover:border-slate-400 hover:text-white bg-transparent transition"
                  onClick={() => setAboutOpen(true)}
                >
                  {tableName}
                </button>
              )}

              <div className="w-px h-4 bg-slate-700" />

              <NavLink to={`/table/graph${tabSearch}`} className={tabClass}>Graph</NavLink>
              <NavLink to={`/table/metadata${tabSearch}`} className={tabClass}>Metadata</NavLink>
              <NavLink to={`/table/timeline${tabSearch}`} className={tabClass}>Timeline</NavLink>
              <NavLink to={`/table/filetree${tabSearch}`} className={tabClass}>FileTree</NavLink>

              <button
                className={`text-sm font-medium px-3 py-1 rounded-md border transition ${detailsOpen
                  ? 'bg-[#2E86C1] border-[#2E86C1] text-white'
                  : 'border-slate-600 text-slate-400 hover:border-slate-400 hover:text-white'
                  }`}
                onClick={() => setDetailsOpen(p => !p)}
              >
                Specs
              </button>

              <div className="ml-auto flex items-center gap-3">
                <button
                  className="text-sm font-medium text-slate-400 hover:text-white border border-slate-600 hover:border-slate-400 px-3 py-1 rounded-md transition"
                  title="Opens this view in a new tab using cached data, no backend request is made"
                  onClick={() => {
                    const url = new URL(window.location.href)
                    url.searchParams.set('dup', '1')
                    window.open(url.toString(), '_blank')
                  }}
                >
                  Duplicate tab
                </button>

                <div className="w-px h-4 bg-slate-700" />

                <button
                  className="text-sm font-medium text-slate-400 hover:text-white border border-slate-600 hover:border-slate-400 px-3 py-1 rounded-md transition"
                  onClick={() => navigate('/')}
                >
                  ← Home
                </button>
              </div>
            </div>
          )}

          {isTablePage && (
            <button
              className="md:hidden ml-auto flex flex-col justify-center items-center w-8 h-8 gap-1.5 rounded transition hover:bg-[#252d3d] cursor-pointer"
              onClick={() => setMenuOpen(p => !p)}
              aria-label="Toggle menu"
            >
              <span className={`block w-5 h-0.5 bg-slate-400 transition-all origin-center ${menuOpen ? 'rotate-45 translate-y-2' : ''}`} />
              <span className={`block w-5 h-0.5 bg-slate-400 transition-all ${menuOpen ? 'opacity-0' : ''}`} />
              <span className={`block w-5 h-0.5 bg-slate-400 transition-all origin-center ${menuOpen ? '-rotate-45 -translate-y-2' : ''}`} />
            </button>
          )}
        </div>

        {isTablePage && menuOpen && (
          <div className="md:hidden border-t border-[#2d3748] px-4 py-3 flex flex-col gap-1">
            {tableName && (
              <button
                className="text-sm font-mono px-3 py-2 rounded-md border border-slate-600 text-slate-300 hover:border-slate-400 hover:text-white bg-transparent transition text-left"
                onClick={() => { setAboutOpen(true); setMenuOpen(false) }}
              >
                {tableName}
              </button>
            )}

            <div className="h-px bg-[#2d3748] my-1" />

            <NavLink to={`/table/graph${tabSearch}`} className={mobileTabClass}>Graph</NavLink>
            <NavLink to={`/table/metadata${tabSearch}`} className={mobileTabClass}>Metadata</NavLink>
            <NavLink to={`/table/timeline${tabSearch}`} className={mobileTabClass}>Timeline</NavLink>
            <NavLink to={`/table/filetree${tabSearch}`} className={mobileTabClass}>FileTree</NavLink>

            <div className="h-px bg-[#2d3748] my-1" />

            <button
              className={`text-sm font-medium px-3 py-2 rounded-md border transition text-left ${detailsOpen
                ? 'bg-[#2E86C1] border-[#2E86C1] text-white'
                : 'border-slate-600 text-slate-400 hover:border-slate-400 hover:text-white'
                }`}
              onClick={() => { setDetailsOpen(p => !p); setMenuOpen(false) }}
            >
              Specs
            </button>

            <button
              className="text-sm font-medium text-slate-400 hover:text-white border border-slate-600 hover:border-slate-400 px-3 py-2 rounded-md transition text-left"
              title="Opens this view in a new tab using cached data, no backend request is made"
              onClick={() => {
                const url = new URL(window.location.href)
                url.searchParams.set('dup', '1')
                window.open(url.toString(), '_blank')
                setMenuOpen(false)
              }}
            >
              Duplicate tab
            </button>

            <button
              className="text-sm font-medium text-slate-400 hover:text-white border border-slate-600 hover:border-slate-400 px-3 py-2 rounded-md transition text-left"
              onClick={() => { navigate('/'); setMenuOpen(false) }}
            >
              ← Home
            </button>
          </div>
        )}
      </nav>

      {aboutOpen && (
        <div
          className="fixed inset-0 z-[9999] bg-black/50 flex items-center justify-center font-sans"
          onClick={() => setAboutOpen(false)}
        >
          <div
            className="w-[480px] min-w-[320px] bg-[#1a202c] rounded-xl shadow-2xl border border-[#2d3748] flex flex-col"
            onClick={e => e.stopPropagation()}
          >
            <div className="flex items-center justify-between px-6 py-4 border-b border-[#2d3748]">
              <div className="flex items-center gap-3">
                <img src={logo} alt="IceGraph" className="h-8 w-8 object-contain" />
                <span className="font-bold text-[#e2e8f0] text-base">IceGraph</span>
              </div>
              <button
                className="w-7 h-7 rounded-full bg-[#2d3748] text-slate-400 flex items-center justify-center text-base cursor-pointer hover:bg-[#3d4a5c] hover:text-slate-200 transition"
                onClick={() => setAboutOpen(false)}
              >
                ✕
              </button>
            </div>
            <div className="px-6 py-5 flex flex-col gap-4 text-sm text-slate-300">
              <p className="leading-relaxed">
                <span className="font-semibold text-white">IceGraph</span> is an open-source tool for visualizing and exploring Apache Iceberg table metadata through an interactive graph interface.
              </p>
              <div className="border-t border-[#2d3748] pt-4 flex flex-col gap-2 text-xs">
                <div className="flex items-center justify-between text-slate-400">
                  <span className="text-slate-500 uppercase tracking-wider text-[10px] font-semibold">Source</span>
                  <a
                    href="https://github.com/YanivZalach/IceGraph"
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-[#2E86C1] hover:text-blue-400 transition font-mono"
                  >
                    github.com/YanivZalach/IceGraph
                  </a>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
    </>
  )
}
