import JSONbig from "json-bigint";
import { Suspense, useEffect, useRef, useState } from "react";
import { useHotkey } from "@tanstack/react-hotkeys";
import { Outlet, useNavigate, useSearch } from "@tanstack/react-router";
import { TableGraphDataContext } from "../features/table/tableGraphData";
import PageLoader from "../components/PageLoader";
import GraphCollectionChecklist from "../components/GraphCollectionChecklist";
import { formatLocaleDateTime, parseUtcDate } from "../utils/dateUtils";
import { IS_MOCK, MOCK_TABLE_SEARCH } from "../appConstants";
import {
  UI_BODY_MUTED_CLASS,
  UI_DIALOG_TITLE_CLASS,
  UI_MONO_MUTED_CLASS,
} from "../uiTypography";

import MetadataStructured from "../components/MetadataStructured";
import PartitionFieldTable from "../features/specs/components/PartitionFieldTable";
import SortFieldTable from "../features/specs/components/SortFieldTable";
import { diffSpecRows, plainSpecRows } from "../features/specs/specFieldRows";
import SchemaDiffView from "../features/schema/components/SchemaDiffView";
import SchemaFieldList from "../features/schema/components/SchemaFieldList";
import { parseIcebergSchema } from "../features/schema/schemaModel";
import {
  integerText,
  schemaColumnNames,
} from "../features/metadata/metadataPresentation";
import {
  specSelectionLabel,
  findPreviousSpec,
  useTableSpecs,
} from "../features/table/tableSpecs";
import {
  BRANCH_CONNECTION_COLOR,
  DELETED_DATA_FILE_CONNECTION_COLOR,
  ERROR_NODE_RGB,
  FileType,
  MAIN_BRANCH_NAME,
  NODE_STYLE_MAP,
} from "../graphConstants";

const DETAIL_TYPE_CONFIG = {
  schema: {
    listKey: "schemas",
    noPrevLabel: "No previous schema",
  },
  partition: {
    listKey: "partition-specs",
    fieldIdentity: (field) => field["field-id"],
    noPrevLabel: "No previous partition spec",
  },
  order: {
    listKey: "sort-orders",
    fieldIdentity: (field) =>
      `${String(field["source-id"])}\u0000${field.transform ?? ""}`,
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
    unresolvedSelection,
    clearSpecSelection,
    openSpec,
    specView,
    setSpecView,
    graphQuery,
    collectionStages,
    issuesOpen,
    setIssuesOpen,
    errors,
    warnings,
  } = useTableSpecs();
  const detailPanelRef = useRef(null);
  const specsDialogRef = useRef(null);

  useEffect(() => {
    if (!detailsOpen) return;
    const previousFocus = document.activeElement;
    const dialog = specsDialogRef.current;
    const focusable = () => [
      ...(dialog?.querySelectorAll(
        'button:not([disabled]), [href], [tabindex="0"]',
      ) ?? []),
    ];
    focusable()[0]?.focus();
    const trapFocus = (event) => {
      if (event.key !== "Tab") return;
      const elements = focusable();
      const first = elements[0];
      const last = elements[elements.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last?.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first?.focus();
      }
    };
    dialog?.addEventListener("keydown", trapFocus);
    return () => {
      dialog?.removeEventListener("keydown", trapFocus);
      if (previousFocus instanceof HTMLElement && previousFocus.isConnected)
        previousFocus.focus({ preventScroll: true });
    };
  }, [detailsOpen]);

  useEffect(() => {
    sessionStorage.removeItem("last_graph_selection");
  }, []);

  useHotkey(
    "Escape",
    () => {
      if (issuesOpen) setIssuesOpen(false);
      else setDetailsOpen(false);
    },
    {
      conflictBehavior: "allow",
      enabled: detailsOpen || issuesOpen,
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
  const [specJsonCopied, setSpecJsonCopied] = useState(false);

  const selectedType = selectionDetail?.type;
  const selectedId = selectionDetail?.id;
  useEffect(() => {
    if (selectedType && detailPanelRef.current) {
      detailPanelRef.current.scrollIntoView({
        behavior: "smooth",
        block: "nearest",
      });
    }
  }, [selectedType, selectedId]);

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
                    search: MOCK_TABLE_SEARCH,
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
  // Source IDs are resolved against the current schema. The ID stays visible in
  // every row, so an older spec pointing at a since-renamed column still shows
  // the value Iceberg recorded.
  const specColumnNames = schemaColumnNames(
    parseIcebergSchema(
      metadata?.schemas?.find(
        (schema) =>
          integerText(schema["schema-id"]) ===
          integerText(metadata["current-schema-id"]),
      ),
    ),
  );

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
          }}
        >
          <div
            ref={specsDialogRef}
            role="dialog"
            aria-modal="true"
            aria-label="Table Specification"
            className="w-[90vw] max-w-6xl bg-surface rounded-xl shadow-2xl border border-edge max-h-[80dvh] flex flex-col"
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
                aria-label="Close Specs"
                className="w-7 h-7 rounded-full bg-edge text-slate-400 flex items-center justify-center text-base cursor-pointer hover:bg-edge-hover hover:text-slate-200 transition"
                onClick={() => {
                  setDetailsOpen(false);
                }}
              >
                ✕
              </button>
            </div>

            <div className="overflow-y-auto px-6 py-5 flex flex-col gap-4">
              <MetadataStructured
                metadata={metadata}
                onSelect={(kind, id) => openSpec({ kind, id })}
                selection={selectionDetail}
              />

              {unresolvedSelection && (
                <div className="rounded-xl border border-amber-500/40 bg-amber-900/20 px-4 py-3">
                  <p className="text-sm text-amber-300">
                    {specSelectionLabel(unresolvedSelection)} is not in this
                    table&apos;s loaded metadata. It may belong to another table
                    or fall outside the selected snapshot range.
                  </p>
                  <button
                    type="button"
                    className="mt-2 text-xs text-accent-text hover:underline cursor-pointer"
                    onClick={() => clearSpecSelection()}
                  >
                    Clear selection
                  </button>
                </div>
              )}

              {selectionDetail &&
                (() => {
                  const config = DETAIL_TYPE_CONFIG[selectionDetail.type];
                  const list = metadata?.[config.listKey] ?? [];
                  const prevItem = findPreviousSpec(list, selectionDetail.data);
                  const hasPrev = prevItem !== null;
                  const showDiff = specView === "diff" && hasPrev;

                  const diffRows =
                    showDiff && hasPrev && selectionDetail.type !== "schema"
                      ? diffSpecRows(
                          prevItem.fields,
                          selectionDetail.data.fields,
                          config.fieldIdentity,
                          selectionDetail.type === "order",
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
                              type="button"
                              aria-pressed={!showDiff}
                              className={`text-xs font-bold px-2 py-0.5 rounded-l-full border border-white/30 transition ${!showDiff ? "bg-white text-accent" : "bg-transparent text-white/70 hover:text-white"}`}
                              onClick={() => setSpecView("full")}
                            >
                              Full
                            </button>
                            <button
                              type="button"
                              aria-pressed={showDiff}
                              disabled={!hasPrev}
                              title={
                                !hasPrev
                                  ? config.noPrevLabel
                                  : "Show diff to previous version"
                              }
                              className={`text-xs font-bold px-2 py-0.5 rounded-r-full border border-white/30 transition ${showDiff ? "bg-white text-accent" : !hasPrev ? "bg-transparent text-white/30 cursor-not-allowed" : "bg-transparent text-white/70 hover:text-white cursor-pointer"}`}
                              onClick={() => hasPrev && setSpecView("diff")}
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
                            onClick={() => clearSpecSelection()}
                          >
                            ×
                          </button>
                        </div>
                      </div>
                      <div className="max-h-[300px] overflow-y-auto">
                        {!showDiff && selectionDetail.type === "schema" && (
                          <SchemaFieldList schema={selectionDetail.data} />
                        )}
                        {!showDiff && selectionDetail.type === "partition" && (
                          <PartitionFieldTable
                            rows={plainSpecRows(selectionDetail.data.fields)}
                            columnNames={specColumnNames}
                          />
                        )}
                        {!showDiff && selectionDetail.type === "order" && (
                          <SortFieldTable
                            rows={plainSpecRows(selectionDetail.data.fields)}
                            columnNames={specColumnNames}
                          />
                        )}
                        {showDiff &&
                          hasPrev &&
                          selectionDetail.type === "schema" && (
                            <SchemaDiffView
                              previousSchema={prevItem}
                              currentSchema={selectionDetail.data}
                            />
                          )}
                        {showDiff &&
                          diffRows &&
                          selectionDetail.type === "partition" && (
                            <PartitionFieldTable
                              rows={diffRows}
                              columnNames={specColumnNames}
                            />
                          )}
                        {showDiff &&
                          diffRows &&
                          selectionDetail.type === "order" && (
                            <SortFieldTable
                              rows={diffRows}
                              columnNames={specColumnNames}
                            />
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
