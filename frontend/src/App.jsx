import { lazy, Suspense } from "react";
import { Navigate, Route, Routes } from "react-router-dom";
import { MOCK_HOME_ROUTE, IS_MOCK } from "./appConstants";
import NavBar from "./components/NavBar";
import { TableSpecsProvider } from "./context/TableSpecsContext";
import logo from "./assets/icegraph.png";

const HomePage = lazy(() => import("./pages/HomePage"));
const SnapshotSelectionPage = lazy(
  () => import("./pages/SnapshotSelectionPage"),
);
const DocsPage = lazy(() => import("./pages/DocsPage"));
const TableLayout = lazy(() => import("./pages/TableLayout"));
const GraphPage = lazy(() => import("./pages/GraphPage"));
const MetadataPage = lazy(() => import("./pages/MetadataPage"));
const TimelinePage = lazy(() => import("./pages/TimelinePage"));
const FileTreePage = lazy(() => import("./pages/FileTreePage"));

function PageLoader() {
  return (
    <div className="fixed inset-0 flex flex-col items-center justify-center gap-5 bg-canvas">
      <img src={logo} alt="IceGraph" className="h-28 w-28 object-contain" />
      <div className="flex items-center gap-1 text-lg font-medium tracking-wide text-slate-300">
        <span>Loading</span>
        <span className="animate-bounce [animation-delay:-0.3s]">.</span>
        <span className="animate-bounce [animation-delay:-0.15s]">.</span>
        <span className="animate-bounce">.</span>
      </div>
    </div>
  );
}

function Layout({ children }) {
  return (
    <div className="min-h-screen bg-canvas flex flex-col">
      <NavBar />
      {children}
    </div>
  );
}

export default function App() {
  return (
    <TableSpecsProvider>
      <Suspense fallback={<PageLoader />}>
        <Routes>
          <Route
            path="/"
            element={
              IS_MOCK ? (
                <Navigate to={MOCK_HOME_ROUTE} replace />
              ) : (
                <Layout>
                  <HomePage />
                </Layout>
              )
            }
          />
          <Route
            path="snapshots-selection"
            element={
              IS_MOCK ? (
                <Navigate to={MOCK_HOME_ROUTE} replace />
              ) : (
                <Layout>
                  <SnapshotSelectionPage />
                </Layout>
              )
            }
          />
          <Route
            path="/docs"
            element={
              <Layout>
                <DocsPage />
              </Layout>
            }
          />
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
            <Route path="timeline" element={<TimelinePage />} />
            <Route path="filetree" element={<FileTreePage />} />
          </Route>
        </Routes>
      </Suspense>
    </TableSpecsProvider>
  );
}
