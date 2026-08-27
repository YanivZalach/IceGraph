import JSONbig from "json-bigint";
import { Suspense, useEffect, useEffectEvent, useRef, useState } from "react";
import { Outlet, useNavigate, useSearch } from "@tanstack/react-router";
import { TableGraphDataContext } from "../features/table/tableGraphData";
import PageLoader from "../components/PageLoader";
import GraphCollectionChecklist from "../components/GraphCollectionChecklist";
import { formatLocaleDateTime, parseUtcDate } from "../utils/dateUtils";
import { IS_MOCK, MOCK_TABLE } from "../appConstants";
import {
  UI_BODY_MUTED_CLASS,
  UI_DIALOG_TITLE_CLASS,
  UI_MONO_MUTED_CLASS,
} from "../uiTypography";

import MetadataStructured from "../components/MetadataStructured";
import PartitionSpecList, {
  PartitionDiffView,
} from "../components/PartitionSpecList";
import SchemaFieldList, { SchemaDiffView } from "../components/SchemaFieldList";
import SortOrderList, { SortDiffView } from "../components/SortOrderList";
import { useTableSpecs } from "../context/TableSpecsContext";
import {
  BRANCH_CONNECTION_COLOR,
  DELETED_DATA_FILE_CONNECTION_COLOR,
  ERROR_NODE_RGB,
  FileType,
  MAIN_BRANCH_NAME,
  NODE_STYLE_MAP,
} from "../graphConstants";
import { getCachedData } from "../utils/cacheUtils";
import { diffFieldLists } from "../utils/diffFieldLists";

const DETAIL_TYPE_CONFIG = {
  schema: {
    listKey: "schemas",
    idKey: "schema-id",
    fieldIdKey: (f) => f["field-id"] ?? f.id,
    noPrevLabel: "No previous schema",
  },
  spec: {
    listKey: "partition-specs",
    idKey: "spec-id",
    fieldIdKey: (f) => f["field-id"],
    noPrevLabel: "No previous partition spec",
  },
  order: {
    listKey: "sort-orders",
    idKey: "order-id",
    fieldIdKey: (f) => f["source-id"],
    noPrevLabel: "No previous sort order",
  },
};

const localizeNodeTimestamps = (details) => {
  if (!details) return {};

  const result = { ...details };

  for (const key of Object.keys(result)) {
    if (!key.includes("timestamp")) continue;

    try {
      const dateObj = parseUtcDate(result[key]);
      if (dateObj) {
        result[key] = formatLocaleDateTime(dateObj);
      }
    } catch (e) {
      console.error(
        "Failed to parse timestamp key:",
        key,
        "value:",
        result[key],
        "error:",
        e,
      );
    }
  }

  return result;
};

const METADATA_COLOR_SHIFT_SPREAD = 1.5;

const fileNameFromPath = (filePath) =>
  String(filePath ?? "")
    .split("/")
    .pop();

const buildMetadataColorShifts = (nodeDetails) => {
  const metadataDetails = nodeDetails
    .filter(
      (d) => d.type === FileType.MAIN_METADATA || d.type === FileType.METADATA,
    )
    .map((d) => ({
      filePath: d.file_path,
      time: parseUtcDate(d.timestamp)?.getTime() ?? 0,
    }))
    .sort((a, b) => b.time - a.time);

  return new Map(
    metadataDetails.map((d, index) => [
      d.filePath,
      1 - index / (METADATA_COLOR_SHIFT_SPREAD * metadataDetails.length),
    ]),
  );
};

const buildEdgesFromNodes = (nodes) => {
  const nodeIds = new Set(nodes.map((n) => n.id));
  const snapshotPathById = {};
  nodes.forEach((n) => {
    if (n.type === FileType.SNAPSHOT && n.details?.snapshot_id != null) {
      snapshotPathById[String(n.details.snapshot_id)] = n.id;
    }
  });

  const edges = [];
  nodes.forEach((node) => {
    const details = node.details || {};

    if (
      node.type === FileType.MAIN_METADATA ||
      node.type === FileType.METADATA
    ) {
      const mainPath =
        details.snapshot_id != null
          ? snapshotPathById[String(details.snapshot_id)]
          : null;
      if (mainPath && nodeIds.has(mainPath)) {
        edges.push({ from: node.id, to: mainPath });
      }

      const branchNamesBySnapId = {};
      Object.entries(details.refs || {}).forEach(([name, attrs]) => {
        if (attrs?.type === "branch" && name !== MAIN_BRANCH_NAME) {
          const snapId = String(attrs["snapshot-id"]);
          if (!branchNamesBySnapId[snapId]) branchNamesBySnapId[snapId] = [];
          branchNamesBySnapId[snapId].push(name);
        }
      });
      Object.entries(branchNamesBySnapId).forEach(([snapId, names]) => {
        const branchPath = snapshotPathById[snapId];
        if (branchPath && nodeIds.has(branchPath)) {
          edges.push({
            from: node.id,
            to: branchPath,
            branch_names: names.join(", "),
          });
        }
      });
      return;
    }

    const deleted = new Set(details.deleted_child_files || []);
    const childFiles = details.child_files || [];
    childFiles.forEach((childPath) => {
      if (!nodeIds.has(childPath)) return;
      const edge = { from: node.id, to: childPath };
      if (node.type === FileType.MANIFEST && deleted.has(childPath))
        edge.is_deleted = true;
      edges.push(edge);
    });
  });
  return edges;
};

export default function TableLayout() {
  const search = useSearch({ strict: false });
  const navigate = useNavigate();
  const {
    detailsOpen,
    setDetailsOpen,
    selectionDetail,
    setSelectionDetail,
    setRawData,
    setErrors,
    setWarnings,
    issuesOpen,
    setIssuesOpen,
    errors,
    warnings,
  } = useTableSpecs();
  const detailPanelRef = useRef(null);

  useEffect(() => {
    sessionStorage.removeItem("last_graph_selection");
  }, []);

  useEffect(() => {
    const handleKey = (e) => {
      if (e.key === "Escape") {
        setDetailsOpen(false);
        setSelectionDetail(null);
        setIssuesOpen(false);
      }
    };
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [setDetailsOpen, setSelectionDetail, setIssuesOpen]);

  useEffect(() => {
    const hasErrors = errors && Object.keys(errors).length > 0;
    const hasWarnings = warnings && Object.keys(warnings).length > 0;
    if (hasErrors || hasWarnings) {
      setIssuesOpen(true);
    }
  }, [errors, warnings, setIssuesOpen]);

  const tableName = search.table || "";
  const startSnapshot = search.start_snapshot_id || "";
  const endSnapshot = search.end_snapshot_id || "";
  const isDup = search.dup === "1";
  const cacheKey = isDup
    ? window.location.href
    : `graphData_${tableName}_${startSnapshot}_${endSnapshot}`;

  const [loading, setLoading] = useState(true);
  const [collectionStages, setCollectionStages] = useState(null);
  const [error, setError] = useState(null);
  const [graphData, setGraphData] = useState(null);
  const [jobId, setJobId] = useState(null);
  const [jobToken, setJobToken] = useState(null);
  const [showDiff, setShowDiff] = useState(false);
  const [specJsonCopied, setSpecJsonCopied] = useState(false);

  useEffect(() => {
    setShowDiff(false);
  }, [selectionDetail]);

  useEffect(() => {
    if (selectionDetail && detailPanelRef.current) {
      detailPanelRef.current.scrollIntoView({
        behavior: "smooth",
        block: "nearest",
      });
    }
  }, [selectionDetail]);

  const buildGraphData = (data) => {
    const nodeDetails = data.nodes || [];
    const colorShiftByFilePath = buildMetadataColorShifts(nodeDetails);

    const styledNodes = nodeDetails.map((details) => {
      const style = NODE_STYLE_MAP[details.type] || {
        rgb: [100, 100, 100],
        level: 0,
      };
      const [r, g, b] = details.error ? ERROR_NODE_RGB : style.rgb;
      const colorShift = details.error
        ? 1
        : (colorShiftByFilePath.get(details.file_path) ?? 1);

      return {
        id: details.file_path,
        label: fileNameFromPath(details.file_path),
        type: details.type,
        details: localizeNodeTimestamps(details),
        shape: "box",
        color: `rgba(${r},${g},${b},${colorShift})`,
        level: style.level,
      };
    });
    const styledEdges = buildEdgesFromNodes(styledNodes).map((edge) => {
      const newEdge = { ...edge };
      if (edge.is_deleted) {
        newEdge.color = DELETED_DATA_FILE_CONNECTION_COLOR;
        newEdge.title = "deleted";
      } else if (edge.branch_names) {
        newEdge.dashes = [15, 20, 5, 20];
        newEdge.color = BRANCH_CONNECTION_COLOR;
        newEdge.title = edge.branch_names;
      }
      return newEdge;
    });
    return {
      nodes: styledNodes,
      edges: styledEdges,
      metadata: data.metadata,
      errors: data.errors || {},
    };
  };

  const submitGraphJob = async (table, start, end) => {
    try {
      const body = new URLSearchParams();
      body.append("table_name", table);
      if (start) body.append("start_snapshot_id", start);
      if (end) body.append("end_snapshot_id", end);

      const res = await fetch("/api/v1/graph-data", {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: body.toString(),
      });

      if (!res.ok) {
        throw new Error("Failed to submit job");
      }

      const result = await res.json();
      setJobToken(result["X-IceGraph-Job-Token"]);
      setJobId(result.key);
    } catch (err) {
      setError(err.message || "Failed to submit job");
      setLoading(false);
    }
  };

  const pollJobStatus = useEffectEvent(
    async (polledJobId, polledJobToken, signal) => {
      try {
        const response = await fetch(`/api/v1/graph-data/${polledJobId}`, {
          headers: { "X-IceGraph-Job-Token": polledJobToken },
          signal,
        });
        if (response.status === 200) {
          const text = await response.text();
          setRawData(text);
          const data = JSONbig({ storeAsString: true }).parse(text);

          setGraphData(buildGraphData(data));
          setErrors(data.errors || {});
          setWarnings(data.warnings || {});
          setLoading(false);
          setJobId(null);
          setJobToken(null);

          return false;
        } else if (response.status === 202) {
          const data = await response.json();
          setCollectionStages(data.stages || null);
          return true;
        } else {
          const data = await response.json();
          setCollectionStages(data.stages || null);
          setError(data.error || "Job failed");
          setLoading(false);
          setJobId(null);
          setJobToken(null);

          return false;
        }
      } catch (error) {
        if (signal.aborted) return false;

        setError(error.message || "Failed to check job status");
        setLoading(false);
        setJobId(null);
        setJobToken(null);

        return false;
      }
    },
  );

  useEffect(() => {
    if (!tableName) {
      setError("No table name provided.");
      setLoading(false);
      return;
    }

    if (isDup) {
      (async () => {
        try {
          const cached = await getCachedData(cacheKey);
          if (cached) {
            setRawData(cached);
            const data = JSONbig({ storeAsString: true }).parse(cached);
            setGraphData(buildGraphData(data));
            setErrors(data.errors || {});
            setWarnings(data.warnings || {});
            setLoading(false);

            navigate({
              to: ".",
              search: (prev) => ({
                ...prev,
                dup: undefined,
                cache_id: undefined,
              }),
              replace: true,
            });
            return;
          } else {
            throw new Error("No cached data found.");
          }
        } catch (err) {
          console.error("Failed to restore from cache:", err);
          setError("No cached data found.");
          setLoading(false);
        }
      })();
      return;
    }

    setError(null);
    setErrors({});
    setWarnings({});
    setCollectionStages(null);
    submitGraphJob(tableName, startSnapshot, endSnapshot);
  }, [tableName, startSnapshot, endSnapshot]);

  useEffect(() => {
    if (!jobId || !jobToken) return;

    const abortController = new AbortController();
    let pollTimeoutId = null;

    const poll = async () => {
      const shouldContinuePolling = await pollJobStatus(
        jobId,
        jobToken,
        abortController.signal,
      );

      if (shouldContinuePolling && !abortController.signal.aborted) {
        pollTimeoutId = setTimeout(poll, 1000);
      }
    };

    poll();

    return () => {
      abortController.abort();
      if (pollTimeoutId) clearTimeout(pollTimeoutId);
    };
  }, [jobId, jobToken]);

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
          <GraphCollectionChecklist stages={collectionStages} />
        </div>
      </div>
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
            onClick={() =>
              IS_MOCK
                ? navigate({
                    to: "/table/timeline",
                    search: { table: MOCK_TABLE },
                  })
                : navigate({ to: "/" })
            }
          >
            ← Back to Home
          </button>
        </div>
      </div>
    );
  }

  const metadata = graphData.metadata;

  const showDetail = (type, id) => {
    if (!metadata) return;
    let data = null;
    let label = "";

    if (type === "schema") {
      data = metadata.schemas?.find((s) => s["schema-id"] === id);
      label = `Schema ID: ${id}`;
    } else if (type === "spec") {
      data = metadata["partition-specs"]?.find((s) => s["spec-id"] === id);
      label = `Partition ID: ${id}`;
    } else if (type === "order") {
      data = metadata["sort-orders"]?.find((s) => s["order-id"] === id);
      label = `Order ID: ${id}`;
    }

    if (data) setSelectionDetail({ label, data, type, id });
  };

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

      {detailsOpen && metadata && (
        <div
          className="fixed inset-0 z-[9999] bg-black/50 flex items-center justify-center font-sans"
          onClick={() => {
            setDetailsOpen(false);
            setSelectionDetail(null);
          }}
        >
          <div
            className="w-1/2 min-w-85 max-w-3xl bg-surface rounded-xl shadow-2xl border border-edge max-h-[80dvh] flex flex-col"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex items-center justify-between px-6 py-4 border-b border-edge shrink-0">
              <div>
                <div className={UI_DIALOG_TITLE_CLASS}>Table Specification</div>
                <div className={`${UI_MONO_MUTED_CLASS} mt-0.5`}>
                  {metadata?.["table-name"]}
                </div>
              </div>
              <button
                className="w-7 h-7 rounded-full bg-edge text-slate-400 flex items-center justify-center text-base cursor-pointer hover:bg-edge-hover hover:text-slate-200 transition"
                onClick={() => {
                  setDetailsOpen(false);
                  setSelectionDetail(null);
                }}
              >
                ✕
              </button>
            </div>

            <div className="overflow-y-auto px-6 py-5 flex flex-col gap-4">
              <MetadataStructured
                metadata={metadata}
                onSelect={showDetail}
                selectedId={selectionDetail?.label}
              />

              {selectionDetail &&
                (() => {
                  const config = DETAIL_TYPE_CONFIG[selectionDetail.type];
                  const list = metadata?.[config.listKey] ?? [];
                  const idx = list.findIndex(
                    (s) => s[config.idKey] === selectionDetail.id,
                  );
                  const prevItem = idx > 0 ? list[idx - 1] : null;
                  const hasPrev = prevItem !== null;

                  const diff =
                    showDiff && hasPrev
                      ? diffFieldLists(
                          prevItem.fields,
                          selectionDetail.data.fields,
                          config.fieldIdKey,
                        )
                      : null;

                  return (
                    <div
                      ref={detailPanelRef}
                      className="rounded-lg border-2 border-accent"
                    >
                      <div className="flex items-center justify-between px-4 py-2 bg-accent">
                        <span className="text-sm font-bold text-white">
                          {selectionDetail.label}
                        </span>
                        <div className="flex items-center gap-2">
                          <div className="flex items-center gap-0">
                            <button
                              className={`text-xs font-bold px-2 py-0.5 rounded-l-full border border-white/30 transition ${!showDiff ? "bg-white text-accent" : "bg-transparent text-white/70 hover:text-white"}`}
                              onClick={() => setShowDiff(false)}
                            >
                              Full
                            </button>
                            <button
                              disabled={!hasPrev}
                              title={
                                !hasPrev
                                  ? config.noPrevLabel
                                  : "Show diff to previous version"
                              }
                              className={`text-xs font-bold px-2 py-0.5 rounded-r-full border border-white/30 transition ${showDiff ? "bg-white text-accent" : !hasPrev ? "bg-transparent text-white/30 cursor-not-allowed" : "bg-transparent text-white/70 hover:text-white cursor-pointer"}`}
                              onClick={() => hasPrev && setShowDiff(true)}
                            >
                              Diff
                            </button>
                          </div>
                          <button
                            className="text-xs font-bold px-2 py-0.5 rounded-full border border-white/30 bg-transparent text-white/70 hover:text-white transition cursor-pointer"
                            onClick={() => {
                              navigator.clipboard.writeText(
                                JSON.stringify(selectionDetail.data, null, 2),
                              );
                              setSpecJsonCopied(true);
                              setTimeout(() => setSpecJsonCopied(false), 2000);
                            }}
                          >
                            {specJsonCopied ? "✓ Copied" : "Copy JSON"}
                          </button>
                          <button
                            className="text-white/70 hover:text-white text-xl leading-none cursor-pointer transition"
                            onClick={() => setSelectionDetail(null)}
                          >
                            ×
                          </button>
                        </div>
                      </div>
                      <div className="px-4 py-3 max-h-[300px] overflow-y-auto">
                        {!showDiff && selectionDetail.type === "schema" && (
                          <SchemaFieldList schema={selectionDetail.data} />
                        )}
                        {!showDiff && selectionDetail.type === "spec" && (
                          <PartitionSpecList spec={selectionDetail.data} />
                        )}
                        {!showDiff && selectionDetail.type === "order" && (
                          <SortOrderList order={selectionDetail.data} />
                        )}
                        {showDiff &&
                          diff &&
                          selectionDetail.type === "schema" && (
                            <SchemaDiffView diff={diff} />
                          )}
                        {showDiff &&
                          diff &&
                          selectionDetail.type === "spec" && (
                            <PartitionDiffView diff={diff} />
                          )}
                        {showDiff &&
                          diff &&
                          selectionDetail.type === "order" && (
                            <SortDiffView diff={diff} />
                          )}
                      </div>
                    </div>
                  );
                })()}
            </div>
          </div>
        </div>
      )}
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
                  {Object.entries(errors).map(([op, err], i) => (
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
                        <div className="text-xs text-red-300 font-semibold whitespace-pre-wrap leading-relaxed overflow-y-auto tracking-wide">
                          {err}
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
                  {Object.entries(warnings).map(([op, msg], i) => (
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
                        <div className="text-xs text-amber-300 font-semibold whitespace-pre-wrap leading-relaxed overflow-y-auto tracking-wide">
                          {msg}
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
