import { Route, Routes } from 'react-router-dom'
import HomePage from './pages/HomePage'
import GraphPage from './pages/GraphPage'

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<HomePage />} />
      <Route path="/graph" element={<GraphPage />} />
    </Routes>
  )
}
