import { Route, Routes } from 'react-router-dom'
import HomePage from './pages/HomePage'
import GraphPage from './pages/GraphPage'
import MetadataPage from './pages/MetadataPage'
import TableLayout from './pages/TableLayout'
import NavBar from './components/NavBar'
import { TableSpecsProvider } from './context/TableSpecsContext'

function Layout({ children }) {
  return (
    <div className="min-h-screen bg-[#f8fafc] flex flex-col">
      <NavBar />
      {children}
    </div>
  )
}

export default function App() {
  return (
    <TableSpecsProvider>
    <Routes>
      <Route path="/" element={<Layout><HomePage /></Layout>} />
      <Route
        path="/table"
        element={
          <Layout>
            <TableLayout />
          </Layout>
        }
      >
        <Route path="graph" element={<GraphPage />} />
        <Route path="metadata" element={<MetadataPage />} />
      </Route>
    </Routes>
    </TableSpecsProvider>
  )
}
