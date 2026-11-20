import JSONbig from "json-bigint";
import { Suspense, useEffect } from "react";
import { useHotkey } from "@tanstack/react-hotkeys";
import { Outlet, useNavigate, useSearch } from "@tanstack/react-router";
import { TableGraphDataContext } from "../features/table/tableGraphData";
import PageLoader from "../components/PageLoader";
import GraphCollectionChecklist from "../components/GraphCollectionChecklist";
import GraphCacheIssueNotice from "../features/table/components/GraphCacheIssueNotice";
import { UI_BODY_MUTED_CLASS } from "../uiTypography";

import SpecDetailsOverlay from "../features/specs/components/SpecDetailsOverlay";
import Assistant from "../features/assistant/components/Assistant";
import NotIcebergTablePage from "../features/table/components/NotIcebergTablePage";
import UnreachableMetadataFilePage from "../features/table/components/UnreachableMetadataFilePage";
import {
  getUnreachableMetadataFile,
  isNotIcebergTableError,
} from "../features/table/api/tableDescriptionQueries";
import { useTableSpecs } from "../features/specs/tableSpecs";
import { buildGraphData } from "../utils/tableGraphData";

export default function TableLayout() {
  const search = useSearch({ strict: false });
  const navigate = useNavigate();
  const {
    graphQuery,
    collectionStages,
    graphCacheIssue,
    issuesOpen,
    setIssuesOpen,
    errors,
    warnings,
  } = useTableSpecs();

  useHotkey(
    "Escape",
    () => {
      setIssuesOpen(false);
    },
    {
      conflictBehavior: "allow",
      enabled: issuesOpen,
    },
  );

  useEffect(() => {
    const hasErrors = errors && Object.keys(errors).length > 0;
    const hasWarnings = warnings && Object.keys(warnings).length > 0;
    if (hasErrors || hasWarnings) {
      setIssuesOpen(true);
    }
  }, [errors, warnings, setIssuesOpen]);

  const tableName = search.table || "";
  const loading = tableName !== "" && graphQuery.isPending;
  const error =
    tableName === ""
      ? "No table name provided."
      : graphQuery.isError
        ? graphQuery.error?.message || "Failed to load graph"
        : null;
  const graphData = graphQuery.data ? buildGraphData(graphQuery.data) : null;

  if (loading) {
    return (
      <div className="flex-1 flex flex-col items-center justify-center bg-canvas">
        <div
          className="w-full max-w-md rounded-2xl border border-edge bg-surface/80 p-7 shadow-2xl shadow-black/20 backdrop-blur-sm"
          aria-busy="true"
        >
          <div className="mb-6">
            <p className="text-base font-semibold text-ink-bright">
              Preparing table graph
            </p>
            <p className={`${UI_BODY_MUTED_CLASS} mt-1 break-all`}>
              {tableName}
            </p>
          </div>
          <GraphCacheIssueNotice issue={graphCacheIssue} />
          <GraphCollectionChecklist stages={collectionStages} />
        </div>
      </div>
    );
  }

  const unreachableMetadataFile = getUnreachableMetadataFile(graphQuery.error);
  if (unreachableMetadataFile !== null) {
    return (
      <UnreachableMetadataFilePage metadataFile={unreachableMetadataFile} />
    );
  }

  if (isNotIcebergTableError(graphQuery.error)) {
    return (
      <NotIcebergTablePage
        tableName={tableName}
        errorMessages={[graphQuery.error.message]}
      />
    );
  }

  if (error) {
    let errorDisplay;
    try {
      const parsed = JSONbig({ storeAsString: true }).parse(error);
      errorDisplay = (
        <div className="text-left mt-4 text-xs font-mono space-y-1">
          {Object.entries(parsed).map(([key, val]) => (
            <div key={key} className="flex gap-2">
              <span className="text-red-300 font-bold">{key}:</span>
              <span className="text-slate-300 truncate">{String(val)}</span>
            </div>
          ))}
        </div>
      );
    } catch {
      errorDisplay = <p className="text-sm">{error}</p>;
    }

    return (
      <div className="flex-1 flex flex-col items-center justify-center bg-canvas p-6">
        <div className="bg-red-950/50 border border-red-800 text-red-400 px-8 py-6 rounded-xl text-center max-w-lg w-full">
          <h2 className="font-bold mb-2">Request Failed</h2>
          {collectionStages && (
            <div className="my-5 rounded-lg border border-red-900/50 bg-canvas/40 p-4 text-left">
              <GraphCollectionChecklist stages={collectionStages} />
            </div>
          )}
          {errorDisplay}
          <button
            className="mt-6 px-5 py-2.5 rounded-lg border-2 border-accent bg-accent text-white font-bold text-sm cursor-pointer hover:bg-accent-dark transition"
            onClick={() => navigate({ to: "/" })}
          >
            ← Back to Home
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 flex overflow-hidden relative">
      <TableGraphDataContext.Provider value={graphData}>
        {/* Boundary below TableLayout: a suspending tab chunk must not
            reach the root Suspense, which would unmount TableLayout and
            wipe the graph selection (sessionStorage cleared on mount). */}
        <Suspense fallback={<PageLoader />}>
          <Outlet />
        </Suspense>
      </TableGraphDataContext.Provider>

      <SpecDetailsOverlay />
      <Assistant />
      {issuesOpen && (errors || warnings) && (
        <div
          className="fixed inset-0 z-[9999] bg-black/60 flex items-center justify-center font-sans"
          onClick={() => setIssuesOpen(false)}
        >
          <div
            className="w-1/2 min-w-100 max-w-4xl bg-surface rounded-xl shadow-2xl border border-slate-800 max-h-[80dvh] flex flex-col overflow-hidden"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between px-6 py-4 border-b border-slate-700/30 bg-slate-800/20 shrink-0">
              <div className="flex items-center gap-3">
                <span className="font-bold text-ink text-base tracking-tight">
                  System Issues
                </span>
              </div>
              <button
                className="w-7 h-7 rounded-full bg-slate-800/30 text-slate-400 flex items-center justify-center text-base cursor-pointer hover:bg-slate-800/50 hover:text-slate-200 transition"
                onClick={() => setIssuesOpen(false)}
              >
                ✕
              </button>
            </div>

            <div className="overflow-y-auto px-6 py-6 flex flex-col gap-8">
              {errors && Object.keys(errors).length > 0 && (
                <div className="flex flex-col gap-4">
                  <div className="flex items-center gap-2 px-1">
                    <div className="w-2 h-2 rounded-full bg-red-500 animate-pulse" />
                    <h3 className="text-red-400 text-xs font-bold uppercase tracking-widest">
                      Critical Errors
                    </h3>
                  </div>
                  {Object.entries(errors).map(([op, messages], i) => (
                    <div
                      key={`err-${i}`}
                      className="bg-red-950/10 rounded-xl border border-red-900/30 overflow-hidden flex flex-col"
                    >
                      <div className="px-5 py-3 border-b border-red-900/10 bg-red-900/5">
                        <span className="text-base font-bold text-red-500/70 uppercase tracking-tighter block mb-1">
                          Source
                        </span>
                        <div className="text-xs font-mono text-red-200 break-all">
                          {op}
                        </div>
                      </div>
                      <div className="px-5 py-4">
                        <span className="text-base font-bold text-red-500/70 uppercase tracking-tighter block mb-1">
                          Message
                        </span>
                        <div className="divide-y divide-red-500/30 text-xs text-red-300 font-semibold whitespace-pre-wrap leading-relaxed overflow-y-auto tracking-wide">
                          {messages.map((message, j) => (
                            <div key={j} className="py-2 first:pt-0 last:pb-0">
                              {message}
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {warnings && Object.keys(warnings).length > 0 && (
                <div className="flex flex-col gap-4">
                  <div className="flex items-center gap-2 px-1">
                    <div className="w-2 h-2 rounded-full bg-amber-500" />
                    <h3 className="text-amber-400 text-xs font-bold uppercase tracking-widest">
                      Processing Warnings
                    </h3>
                  </div>
                  {Object.entries(warnings).map(([op, messages], i) => (
                    <div
                      key={`warn-${i}`}
                      className="bg-amber-950/10 rounded-xl border border-amber-900/30 overflow-hidden flex flex-col"
                    >
                      <div className="px-5 py-3 border-b border-amber-900/10 bg-amber-900/5">
                        <span className="text-base font-bold text-amber-500/70 uppercase tracking-tighter block mb-1">
                          Context
                        </span>
                        <div className="text-xs font-mono text-amber-200 break-all">
                          {op}
                        </div>
                      </div>
                      <div className="px-5 py-4">
                        <span className="text-base font-bold text-amber-500/70 uppercase tracking-tighter block mb-1">
                          Notice
                        </span>
                        <div className="divide-y divide-amber-500/30 text-xs text-amber-300 font-semibold whitespace-pre-wrap leading-relaxed overflow-y-auto tracking-wide">
                          {messages.map((message, j) => (
                            <div key={j} className="py-2 first:pt-0 last:pb-0">
                              {message}
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
