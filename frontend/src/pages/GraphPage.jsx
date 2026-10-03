import { useCallback, useEffect, useRef, useState, useMemo } from "react";
import { useLocation, useNavigate, useSearch } from "@tanstack/react-router";
import { useTableGraphData } from "../features/table/tableGraphData";
import ForceGraph2D from "react-force-graph-2d";
import {
  PanelDetailRow,
  PanelHeader,
  PANEL_STATUS_BADGE_CLASS,
} from "../components/PanelContent";
import PanelIssueNotice from "../components/PanelIssueNotice";
import HelpTerm from "../shared/components/HelpTerm";
import DataFileReadableMetricsTable from "../components/DataFileReadableMetricsTable";
import PartitionStatisticsTable from "../features/table/components/PartitionStatisticsTable";
import PartitionDistributionTable from "../features/table/components/PartitionDistributionTable";
import ReadableMetricsSummary from "../components/ReadableMetricsSummary";
import { isEmptyValue } from "../shared/lib/isEmptyValue";
import { isKeyboardInputTarget } from "../shared/lib/keyboard";
import {
  UI_DIALOG_SECTION_TITLE_CLASS,
  UI_POPUP_HINT_CLASS,
  UI_TOOLBAR_BUTTON_LAYOUT,
  toolbarButtonClass,
} from "../uiTypography";
import ResizableSidePanel from "../components/ResizableSidePanel";
import {
  GRAPH_SETTINGS,
  CATALOG_NODE_ID,
  CATALOG_NODE_SCALE,
  DELETED_DATA_FILE_CONNECTION_COLOR,
  FileType,
  NODE_STYLE_MAP,
  fileTypeLabel,
} from "../graphConstants";
import {
  getNavHeightPx,
  GRAPH_NODE_FONT_REM,
  GRAPH_NODE_PADDING_X_REM,
  GRAPH_NODE_PADDING_Y_REM,
  PANEL_GUTTER_REM,
  PANEL_WIDTH_DEFAULT_REM,
  PANEL_WIDTH_RELAXED_REM,
  remToPx,
} from "../layoutConstants";
import { bindMouseScrollHandoff } from "../utils/smoothScroll";
import { SELECT_NODE_ID_PARAM } from "../appConstants";

const POPUP_KEYS = "abdegmnopqstuvwxyz";

const LINK_CURVATURE = 0.1;
const DELETED_CONNECTION_LABLE = "deleted";
const DATA_AND_DELETE_FILE_TYPES = new Set([
  FileType.DATA,
  FileType.POSITION_DELETE,
  FileType.EQUALITY_DELETE,
]);

function buildCatalogNodeAndEdge(nodes, metadata) {
  const mainMetadataNode = nodes.find((n) => n.type === FileType.MAIN_METADATA);
  if (!mainMetadataNode) return null;

  const style = NODE_STYLE_MAP[FileType.CATALOG];

  return {
    node: {
      id: CATALOG_NODE_ID,
      label: [fileTypeLabel(FileType.CATALOG), metadata?.["table-name"]]
        .filter(Boolean)
        .join(" · "),
      type: FileType.CATALOG,
      details: {
        type: FileType.CATALOG,
        "table-name": metadata?.["table-name"],
        "table-uuid": metadata?.["table-uuid"],
        location: metadata?.location,
        "format-version": metadata?.["format-version"],
      },
      color: `rgb(${style.rgb.join(",")})`,
      level: style.level,
    },
    edge: { from: CATALOG_NODE_ID, to: mainMetadataNode.id },
  };
}

function getFileSizeBytes(details) {
  const rawFileSize = details?.file_size_in_bytes;
  if (typeof rawFileSize !== "string" && typeof rawFileSize !== "number") {
    return null;
  }

  const fileSizeBytes = Number(rawFileSize);
  return Number.isSafeInteger(fileSizeBytes) && fileSizeBytes >= 0
    ? fileSizeBytes
    : null;
}

function getGraphNodeMetrics() {
  return {
    fontSize: remToPx(GRAPH_NODE_FONT_REM),
    paddingX: remToPx(GRAPH_NODE_PADDING_X_REM),
    paddingY: remToPx(GRAPH_NODE_PADDING_Y_REM),
    linkFontSize: remToPx(3.75),
  };
}

const measureNode = (node, ctx, { fontSize, paddingX, paddingY }) => {
  const isCatalog = node.type === FileType.CATALOG;
  const scale = isCatalog ? CATALOG_NODE_SCALE : 1;
  const nodeFontSize = fontSize * scale;
  ctx.font = `${isCatalog ? 700 : 500} ${nodeFontSize}px "system-ui"`;
  if (!node.__pillW || node.__metricsKey !== nodeFontSize) {
    node.__pillW =
      ctx.measureText(node.label || String(node.id)).width +
      paddingX * 2 * scale;
    node.__pillH = nodeFontSize + paddingY * 2 * scale;
    node.__metricsKey = nodeFontSize;
  }
  return { width: node.__pillW, height: node.__pillH };
};

function getLineage(nodeId, links) {
  const relatedNodes = new Set([String(nodeId)]);

  const toLinks = {};
  const fromLinks = {};

  links.forEach((l) => {
    const s = String(l.source.id ?? l.source);
    const t = String(l.target.id ?? l.target);
    if (!toLinks[s]) toLinks[s] = [];
    if (!fromLinks[t]) fromLinks[t] = [];
    toLinks[s].push(t);
    fromLinks[t].push(s);
  });

  const traverse = (currentId, direction) => {
    const neighbors =
      direction === "to"
        ? toLinks[currentId] || []
        : fromLinks[currentId] || [];
    neighbors.forEach((neighborId) => {
      if (!relatedNodes.has(neighborId)) {
        relatedNodes.add(neighborId);
        traverse(neighborId, direction);
      }
    });
  };

  traverse(String(nodeId), "to");
  traverse(String(nodeId), "from");

  return relatedNodes;
}

export default function GraphPage() {
  const {
    nodes: rawNodes,
    edges: rawEdges,
    metadata,
    errors,
  } = useTableGraphData();

  const search = useSearch({ strict: false });
  const navigate = useNavigate();
  const graphSelectionFromHistory = useLocation({
    select: (location) =>
      "graphSelection" in location.state
        ? location.state.graphSelection
        : undefined,
  });
  const fgRef = useRef();
  const hasInitialized = useRef(false);
  const isResettingRef = useRef(false);
  const appliedGraphSelectionRef = useRef(null);
  const previousGraphSelectionRef = useRef(undefined);

  const [highlightNodes, setHighlightNodes] = useState(new Set());
  const [isInspectMode, setIsInspectMode] = useState(true);
  const [isFullView, setIsFullView] = useState(true);
  const [stickyNode, setStickyNodeInternal] = useState(null);
  const [movementPopup, setMovementPopup] = useState(null);
  const [panelLayout, setPanelLayout] = useState({
    isFullscreen: false,
    panelWidthRem: PANEL_WIDTH_DEFAULT_REM,
  });
  const [nodeMetrics, setNodeMetrics] = useState(getGraphNodeMetrics);

  const [dimensions, setDimensions] = useState({
    width: window.innerWidth,
    height: window.innerHeight - getNavHeightPx(),
  });

  const isInspectModeRef = useRef(isInspectMode);
  const highlightNodesRef = useRef(highlightNodes);
  const stickyNodeRef = useRef(null);
  const setStickyNode = useCallback((val) => {
    const next = val;
    stickyNodeRef.current = next;
    if (next) sessionStorage.setItem("last_graph_selection", next.id);
    setStickyNodeInternal(next);
  }, []);
  const graphDataRef = useRef({ nodes: [], links: [] });
  const treeMapRef = useRef({ incoming: {}, outgoing: {} });
  const movementPopupRef = useRef(null);
  const stickyPanelRef = useRef(null);
  const stickyScrollTargetRef = useRef(0);
  const stickyScrollRafRef = useRef(null);
  const popupListRef = useRef(null);
  const popupScrollTargetRef = useRef(0);
  const popupScrollRafRef = useRef(null);

  useEffect(() => {
    isInspectModeRef.current = isInspectMode;
  }, [isInspectMode]);
  useEffect(() => {
    highlightNodesRef.current = highlightNodes;
  }, [highlightNodes]);
  useEffect(() => {
    movementPopupRef.current = movementPopup;
    if (!movementPopup) {
      popupScrollTargetRef.current = 0;
      if (popupListRef.current) popupListRef.current.scrollTop = 0;
    }
  }, [movementPopup]);

  useEffect(() => {
    const handleResize = () => {
      setNodeMetrics(getGraphNodeMetrics());
      setDimensions({
        width: window.innerWidth,
        height: window.innerHeight - getNavHeightPx(),
      });
    };

    window.addEventListener("resize", handleResize);
    return () => window.removeEventListener("resize", handleResize);
  }, []);

  const graphData = useMemo(() => {
    if (!rawNodes) return { nodes: [], links: [] };

    const catalog = buildCatalogNodeAndEdge(rawNodes, metadata);
    const nodeArray = catalog ? [catalog.node, ...rawNodes] : rawNodes;
    const edgeArray = catalog
      ? [...(rawEdges || []), catalog.edge]
      : rawEdges || [];

    const processedNodes = nodeArray.map((n) => {
      return {
        ...n,
        color: n.color,
        level: n.level,
      };
    });

    const processedLinks = edgeArray.map((e) => ({
      source: e.from,
      target: e.to,
      color: e.color || "#999",
      curvature: e.curvature,
      label:
        e.color === DELETED_DATA_FILE_CONNECTION_COLOR
          ? DELETED_CONNECTION_LABLE
          : e.branch_names || "",
    }));

    const { levelSeparation, nodeSpacing } = GRAPH_SETTINGS;
    const levelsMap = {};
    processedNodes.forEach((n) => {
      const level = n.level || 0;
      if (!levelsMap[level]) levelsMap[level] = [];
      levelsMap[level].push(n);
    });

    Object.entries(levelsMap).forEach(([level, nodes]) => {
      const x = parseInt(level) * levelSeparation;
      const totalHeight = (nodes.length - 1) * nodeSpacing;
      nodes.forEach((node, i) => {
        node.fx = node.originalFx = x;
        node.fy = node.originalFy = i * nodeSpacing - totalHeight / 2;
      });
    });

    return { nodes: processedNodes, links: processedLinks };
  }, [rawNodes, rawEdges, metadata]);
  useEffect(() => {
    graphDataRef.current = graphData;
  }, [graphData]);

  const treeMap = useMemo(() => {
    const incoming = {};
    const outgoing = {};
    graphData.links.forEach((l) => {
      const src = String(l.source.id ?? l.source);
      const tgt = String(l.target.id ?? l.target);
      if (!outgoing[src]) outgoing[src] = [];
      if (!incoming[tgt]) incoming[tgt] = [];
      outgoing[src].push(tgt);
      incoming[tgt].push(src);
    });
    return { incoming, outgoing };
  }, [graphData]);
  useEffect(() => {
    treeMapRef.current = treeMap;
  }, [treeMap]);

  const resetZoom = useCallback(() => {
    graphData.nodes.forEach((node) => {
      node.fx = node.originalFx;
      node.fy = node.originalFy;
      node.x = node.originalFx;
      node.y = node.originalFy;
      node.vx = 0;
      node.vy = 0;
    });

    isResettingRef.current = true;
    fgRef.current?.zoomToFit(500, 50);
    setTimeout(() => {
      isResettingRef.current = false;
    }, 700);
  }, [graphData]);

  const selectNodeInHistory = useCallback(
    (nodeId, { replace = false } = {}) => {
      appliedGraphSelectionRef.current = nodeId;
      navigate({
        to: ".",
        search: (prev) => prev,
        state: (prev) => ({ ...prev, graphSelection: nodeId }),
        replace,
      });
    },
    [navigate],
  );

  const navigateTo = useCallback(
    (node) => {
      setStickyNode(node);
      setHighlightNodes(getLineage(node.id, graphDataRef.current.links));
      setIsFullView(false);
      fgRef.current?.centerAt(node.fx ?? node.x, node.fy ?? node.y, 300);
      selectNodeInHistory(node.id);
      stickyScrollTargetRef.current = 0;
      if (stickyPanelRef.current) stickyPanelRef.current.scrollTop = 0;
    },
    [selectNodeInHistory],
  );

  const closeStickyPanel = useCallback(() => {
    setStickyNode(null);
    setPanelLayout({
      isFullscreen: false,
      panelWidthRem: PANEL_WIDTH_DEFAULT_REM,
    });
  }, [setStickyNode]);

  const deselectNode = useCallback(() => {
    setHighlightNodes(new Set());
    closeStickyPanel();
    selectNodeInHistory(null, { replace: true });
  }, [closeStickyPanel, selectNodeInHistory]);

  const resetView = useCallback(() => {
    deselectNode();
    setIsFullView(true);
    sessionStorage.removeItem("last_graph_selection");
    resetZoom();
  }, [deselectNode, resetZoom]);

  useEffect(() => {
    const goToNeighbors = (neighborIds, direction) => {
      const { nodes } = graphDataRef.current;
      const neighbors = neighborIds
        .map((id) => nodes.find((n) => String(n.id) === String(id)))
        .filter(Boolean);
      if (!neighbors.length) return;
      if (neighbors.length === 1) {
        navigateTo(neighbors[0]);
        return;
      }
      const keyLen = Math.floor(neighbors.length / POPUP_KEYS.length) + 1;
      const combos = neighbors.map((_, i) => {
        let combo = "",
          num = i;
        for (let k = 0; k < keyLen; k++) {
          combo = POPUP_KEYS[num % POPUP_KEYS.length] + combo;
          num = Math.floor(num / POPUP_KEYS.length);
        }
        return combo;
      });
      setMovementPopup({
        nodes: neighbors,
        direction,
        combos,
        keyLen,
        input: "",
      });
    };

    const makeScroller = (targetRef, rafRef, elRef) => (delta) => {
      const el = elRef.current;
      if (!el) return;
      targetRef.current = Math.max(
        0,
        Math.min(targetRef.current + delta, el.scrollHeight - el.clientHeight),
      );
      if (rafRef.current) return;
      const animate = () => {
        const diff = targetRef.current - el.scrollTop;
        if (Math.abs(diff) < 0.5) {
          el.scrollTop = targetRef.current;
          rafRef.current = null;
          return;
        }
        el.scrollTop += diff * 0.14;
        rafRef.current = requestAnimationFrame(animate);
      };
      rafRef.current = requestAnimationFrame(animate);
    };
    const scrollSticky = makeScroller(
      stickyScrollTargetRef,
      stickyScrollRafRef,
      stickyPanelRef,
    );
    const scrollPopup = makeScroller(
      popupScrollTargetRef,
      popupScrollRafRef,
      popupListRef,
    );

    const handleKey = (e) => {
      if (e.ctrlKey || e.metaKey || e.altKey) return;
      if (isKeyboardInputTarget(e.target)) return;
      if (e.key === "i") {
        setIsInspectMode((p) => !p);
        return;
      }
      if (e.key === "c") {
        resetZoom();
        return;
      }
      if (e.key === "r") {
        resetView();
        return;
      }

      if (
        movementPopupRef.current &&
        (e.key === "j" || e.key === "ArrowDown")
      ) {
        e.preventDefault();
        scrollPopup(80);
        return;
      }
      if (movementPopupRef.current && (e.key === "k" || e.key === "ArrowUp")) {
        e.preventDefault();
        scrollPopup(-80);
        return;
      }
      if (stickyNodeRef.current && (e.key === "j" || e.key === "ArrowDown")) {
        e.preventDefault();
        scrollSticky(80);
        return;
      }
      if (stickyNodeRef.current && (e.key === "k" || e.key === "ArrowUp")) {
        e.preventDefault();
        scrollSticky(-80);
        return;
      }

      if (movementPopupRef.current) {
        if (e.key === "Escape") {
          setMovementPopup(null);
          return;
        }
        const popup = movementPopupRef.current;
        const char = e.key.toLowerCase();
        if (!POPUP_KEYS.includes(char)) return;
        const newInput = popup.input + char;
        if (newInput.length === popup.keyLen) {
          const idx = popup.combos.indexOf(newInput);
          if (idx >= 0) {
            navigateTo(popup.nodes[idx]);
            setMovementPopup(null);
          } else setMovementPopup({ ...popup, input: "" });
        } else {
          setMovementPopup({ ...popup, input: newInput });
        }
        return;
      }

      if (e.key === "Escape") {
        closeStickyPanel();
        return;
      }

      if (isInspectModeRef.current) return;

      if (e.key === "Enter" || e.key === " ") {
        if (stickyNodeRef.current) return;
        const mainMeta = graphDataRef.current.nodes.find(
          (n) => n.type === FileType.MAIN_METADATA,
        );
        if (mainMeta) navigateTo(mainMeta);
        return;
      }
      if (e.key === "h" || e.key === "ArrowLeft") {
        e.preventDefault();
        if (!stickyNodeRef.current) return;
        goToNeighbors(
          treeMapRef.current.incoming[String(stickyNodeRef.current.id)] || [],
          "in",
        );
        return;
      }
      if (e.key === "l" || e.key === "ArrowRight") {
        e.preventDefault();
        if (!stickyNodeRef.current) return;
        goToNeighbors(
          treeMapRef.current.outgoing[String(stickyNodeRef.current.id)] || [],
          "out",
        );
        return;
      }
    };
    window.addEventListener("keydown", handleKey);
    const unbindSticky = bindMouseScrollHandoff(
      () => stickyPanelRef.current,
      stickyScrollTargetRef,
      stickyScrollRafRef,
    );
    const unbindPopup = bindMouseScrollHandoff(
      () => popupListRef.current,
      popupScrollTargetRef,
      popupScrollRafRef,
    );
    return () => {
      window.removeEventListener("keydown", handleKey);
      unbindSticky();
      unbindPopup();
      if (stickyScrollRafRef.current)
        cancelAnimationFrame(stickyScrollRafRef.current);
      if (popupScrollRafRef.current)
        cancelAnimationFrame(popupScrollRafRef.current);
    };
  }, [navigateTo, resetZoom, resetView, closeStickyPanel]);

  const selectNodeIdFromSearch = search[SELECT_NODE_ID_PARAM] ?? null;

  useEffect(() => {
    if (
      !fgRef.current ||
      graphData.nodes.length === 0 ||
      hasInitialized.current
    )
      return;
    hasInitialized.current = true;
    fgRef.current.d3ReheatSimulation();

    const historyId = graphSelectionFromHistory;
    const queryId = selectNodeIdFromSearch;
    const sessionId = sessionStorage.getItem("last_graph_selection");
    const targetNodeId = historyId || queryId || sessionId;

    if (targetNodeId) {
      const node = graphData.nodes.find(
        (n) => String(n.id) === String(targetNodeId),
      );
      if (node) {
        const lineage = getLineage(node.id, graphData.links);
        setHighlightNodes(lineage);
        setStickyNode(node);
        setIsFullView(false);
        appliedGraphSelectionRef.current = targetNodeId;

        if (queryId) {
          navigate({
            to: ".",
            search: (prev) => ({ ...prev, [SELECT_NODE_ID_PARAM]: undefined }),
            state: (prev) => ({ ...prev, graphSelection: targetNodeId }),
            replace: true,
          });
        }

        setTimeout(() => {
          fgRef.current?.centerAt(
            node.originalFx ?? node.fx ?? node.x,
            node.originalFy ?? node.fy ?? node.y,
            500,
          );
        }, 100);
      }
    } else {
      setTimeout(() => resetView(), 100);
    }
  }, [
    graphData,
    graphSelectionFromHistory,
    selectNodeIdFromSearch,
    navigate,
    resetView,
  ]);

  useEffect(() => {
    if (graphSelectionFromHistory === undefined) return;

    const hasHistoryMoved =
      graphSelectionFromHistory !== previousGraphSelectionRef.current;
    previousGraphSelectionRef.current = graphSelectionFromHistory;

    if (!hasInitialized.current || !hasHistoryMoved) return;
    if (graphSelectionFromHistory === appliedGraphSelectionRef.current) return;
    appliedGraphSelectionRef.current = graphSelectionFromHistory;

    if (graphSelectionFromHistory === null) {
      resetView();
      return;
    }

    const node = graphData.nodes.find(
      (n) => String(n.id) === String(graphSelectionFromHistory),
    );
    if (node) {
      setHighlightNodes(getLineage(node.id, graphData.links));
      fgRef.current?.centerAt(node.fx ?? node.x, node.fy ?? node.y, 500);
      setStickyNode(node);
      setIsFullView(false);
    }
  }, [graphData, graphSelectionFromHistory, resetView, setStickyNode]);

  useEffect(() => {
    const handleVisibilityChange = () => {
      if (!fgRef.current || graphData.nodes.length === 0) return;
      if (document.hidden) {
        graphData.nodes.forEach((node) => {
          node._savedFx = node.fx;
          node._savedFy = node.fy;
          node.fx = node.x;
          node.fy = node.y;
        });
      } else {
        graphData.nodes.forEach((node) => {
          node.fx = node._savedFx !== undefined ? node._savedFx : node.fx;
          node.fy = node._savedFy !== undefined ? node._savedFy : node.fy;
        });
      }
    };
    document.addEventListener("visibilitychange", handleVisibilityChange);
    return () =>
      document.removeEventListener("visibilitychange", handleVisibilityChange);
  }, [graphData]);

  const handleNodeClick = useCallback(
    (node) => {
      if (!isInspectModeRef.current) {
        const lineage = getLineage(node.id, graphData.links);
        setHighlightNodes(lineage);
        setIsFullView(false);
        fgRef.current.centerAt(node.fx ?? node.x, node.fy ?? node.y, 500);
      }
      setStickyNode(node);
      selectNodeInHistory(node.id);
    },
    [graphData, selectNodeInHistory],
  );

  const paintNode = useCallback(
    (node, ctx) => {
      const label = node.label || String(node.id);
      const { width: w, height: h } = measureNode(node, ctx, nodeMetrics);
      const x = Math.round(node.x - w / 2);
      const y = Math.round(node.y - h / 2);
      const isCatalog = node.type === FileType.CATALOG;

      ctx.shadowBlur = 0;

      ctx.beginPath();
      if (ctx.roundRect) {
        ctx.roundRect(x, y, w, h, 4);
      } else {
        ctx.rect(x, y, w, h);
      }

      ctx.fillStyle =
        node.color ||
        getComputedStyle(document.documentElement)
          .getPropertyValue("--color-edge")
          .trim() ||
        "#2d3748";
      ctx.fill();

      ctx.strokeStyle = "#ffffff";
      ctx.lineWidth = isCatalog ? 6 : 2;
      ctx.stroke();

      ctx.textAlign = "center";
      ctx.textBaseline = "middle";

      if (isCatalog) {
        ctx.fillStyle = "#0f172a";
        ctx.fillText(label, node.x, node.y);
      } else {
        ctx.strokeStyle = "#000000";
        ctx.lineWidth = 10.0;
        ctx.lineJoin = "round";
        ctx.strokeText(label, node.x, node.y);

        ctx.fillStyle = "#ffffff";
        ctx.fillText(label, node.x, node.y);
      }

      ctx.lineWidth = 1;
    },
    [nodeMetrics],
  );

  const paintPointerArea = useCallback(
    (node, color, ctx) => {
      const { fontSize, paddingY } = nodeMetrics;
      const w = node.__pillW || remToPx(2.5);
      const h = node.__pillH || fontSize + paddingY * 2;
      ctx.fillStyle = color;
      ctx.fillRect(node.x - w / 2, node.y - h / 2, w, h);
    },
    [nodeMetrics],
  );

  const linkCurvatures = useMemo(() => {
    const map = new Map();
    graphData.links.forEach((l) => {
      map.set(
        l,
        l.curvature ??
          (!l.label || l.label === DELETED_CONNECTION_LABLE
            ? 0
            : LINK_CURVATURE),
      );
    });
    return map;
  }, [graphData.links]);

  const paintLink = useCallback(
    (link, ctx, globalScale) => {
      const start = link.source;
      const end = link.target;
      if (
        start?.x == null ||
        start.y == null ||
        end?.x == null ||
        end.y == null
      )
        return;

      const sourceSize = measureNode(start, ctx, nodeMetrics);
      const targetSize = measureNode(end, ctx, nodeMetrics);
      const isVertical =
        Math.abs(end.x - start.x) < (sourceSize.width + targetSize.width) / 2;
      const direction = Math.sign(
        isVertical ? end.y - start.y : end.x - start.x,
      );
      const sx =
        start.x + (isVertical ? 0 : (direction * sourceSize.width) / 2);
      const sy =
        start.y + (isVertical ? (direction * sourceSize.height) / 2 : 0);
      const ex = end.x - (isVertical ? 0 : (direction * targetSize.width) / 2);
      const ey = end.y - (isVertical ? (direction * targetSize.height) / 2 : 0);
      if (direction * (isVertical ? ey - sy : ex - sx) <= 0) return;

      const curvature = linkCurvatures.get(link) || 0;
      const controlX = Math.max(
        Math.min(sx, ex),
        Math.min(Math.max(sx, ex), (sx + ex) / 2 + (ey - sy) * curvature),
      );
      const controlY = isVertical
        ? (sy + ey) / 2
        : (sy + ey) / 2 - (ex - sx) * curvature;

      ctx.shadowBlur = 0;
      ctx.strokeStyle = link.color;
      ctx.lineWidth = 1 / globalScale;
      ctx.beginPath();
      ctx.moveTo(sx, sy);
      ctx.quadraticCurveTo(controlX, controlY, ex, ey);
      ctx.stroke();

      const angle = Math.atan2(ey - controlY, ex - controlX);
      const arrowLength = Math.min(
        8 / globalScale,
        Math.hypot(ex - sx, ey - sy) / 3,
      );
      const arrowX = ex - Math.cos(angle) * arrowLength;
      const arrowY = ey - Math.sin(angle) * arrowLength;
      ctx.beginPath();
      ctx.moveTo(ex, ey);
      ctx.lineTo(
        arrowX + (Math.sin(angle) * arrowLength) / 2,
        arrowY - (Math.cos(angle) * arrowLength) / 2,
      );
      ctx.lineTo(
        arrowX - (Math.sin(angle) * arrowLength) / 2,
        arrowY + (Math.cos(angle) * arrowLength) / 2,
      );
      ctx.closePath();
      ctx.fillStyle = link.color;
      ctx.fill();

      if (!link.label) return;
      const position = link.label === DELETED_CONNECTION_LABLE ? 0.5 : 0.25;
      const remaining = 1 - position;
      const qX =
        remaining ** 2 * sx +
        2 * remaining * position * controlX +
        position ** 2 * ex;
      const qY =
        remaining ** 2 * sy +
        2 * remaining * position * controlY +
        position ** 2 * ey;
      ctx.font = `500 ${nodeMetrics.linkFontSize}px "system-ui"`;
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      ctx.fillStyle = "#e3f8f5ff";
      ctx.fillText(link.label, qX, qY);
    },
    [linkCurvatures, nodeMetrics],
  );

  const nodeVisibility = useCallback((n) => {
    const hl = highlightNodesRef.current;
    return hl.size === 0 || hl.has(String(n.id));
  }, []);

  const linkVisibility = useCallback((l) => {
    const hl = highlightNodesRef.current;
    if (hl.size === 0) return true;
    const s = String(l.source.id || l.source);
    const t = String(l.target.id || l.target);
    return hl.has(s) && hl.has(t);
  }, []);

  const sticky = stickyNode
    ? {
        rows: Object.entries(stickyNode.details)
          .filter(
            ([label]) =>
              ![
                "type",
                "errors",
                "warnings",
                "readable_metrics",
                "virtual_read",
                "sampled_partitions",
                "partition_distribution",
              ].includes(label.toLowerCase()),
          )
          .map(([label, value]) => ({
            label,
            value,
          })),
      }
    : null;
  const stickyReadableMetrics =
    stickyNode !== null &&
    DATA_AND_DELETE_FILE_TYPES.has(stickyNode.details.type) &&
    stickyNode.details.readable_metrics &&
    Object.keys(stickyNode.details.readable_metrics).length > 0
      ? stickyNode.details.readable_metrics
      : null;
  const stickyFileSizeBytes =
    stickyReadableMetrics === null
      ? null
      : getFileSizeBytes(stickyNode.details);

  return (
    <div className="relative w-full overflow-hidden h-graph bg-graph-grid">
      <ForceGraph2D
        ref={fgRef}
        width={dimensions.width}
        height={dimensions.height}
        graphData={graphData}
        backgroundColor="#00000000"
        nodeLabel={() => ""}
        nodeCanvasObject={paintNode}
        nodePointerAreaPaint={paintPointerArea}

        nodeVisibility={nodeVisibility}
        linkVisibility={linkVisibility}

        linkWidth={1}
        linkColor={(l) => l.color}
        linkCurvature={(l) => linkCurvatures.get(l) || 0}
        linkCanvasObjectMode={() => "replace"}
        linkCanvasObject={paintLink}

        onNodeClick={handleNodeClick}
        onNodeDrag={() => setTimeout(() => setIsFullView(false), 0)}
        onNodeDragEnd={() => setTimeout(() => setIsFullView(false), 0)}
        onZoom={() => {
          if (!isResettingRef.current)
            setTimeout(() => setIsFullView(false), 0);
        }}

        warmupTicks={1}
        cooldownTicks={0}
        d3AlphaDecay={1}
      />

      <div className="absolute top-4 left-4 flex flex-col gap-2 z-[10] font-sans w-52">
        <button
          className={toolbarButtonClass(isFullView)}
          onClick={() => !isFullView && resetView()}
          onMouseDown={(e) => e.preventDefault()}
        >
          Reset Full View
        </button>

        <button
          className={`${UI_TOOLBAR_BUTTON_LAYOUT} flex overflow-hidden ${
            isInspectMode
              ? "bg-accent text-white border border-accent hover:bg-accent-dark"
              : "bg-surface text-accent border border-accent hover:bg-edge"
          }`}
          onClick={() => setIsInspectMode((p) => !p)}
          onMouseDown={(e) => e.preventDefault()}
        >
          <span className="w-9 flex items-center justify-center text-lg bg-black/5 shrink-0 py-2.5">
            {isInspectMode ? "🔒" : "🔍"}
          </span>
          <span className="flex-1 flex items-center justify-center py-2.5 px-2 leading-tight">
            {isInspectMode ? "Inspect (Locked)" : "Lineage Traversal"}
          </span>
        </button>

        <button
          className={toolbarButtonClass(false)}
          onClick={() => resetZoom()}
          onMouseDown={(e) => e.preventDefault()}
        >
          Center Graph
        </button>
      </div>

      {movementPopup && (
        <div className="absolute inset-0 flex items-center justify-center z-[1100] pointer-events-none">
          <div className="bg-surface/57 backdrop-blur-md border border-edge rounded-xl shadow-2xl p-4 pointer-events-auto w-[70dvw] max-w-4xl font-sans">
            <div className={UI_DIALOG_SECTION_TITLE_CLASS}>
              {movementPopup.direction === "in"
                ? "Navigate to parent"
                : "Navigate to child"}
            </div>
            <div
              ref={popupListRef}
              className="flex flex-col max-h-[60dvh] overflow-y-auto"
            >
              {movementPopup.nodes.map((node, i) => {
                const combo = movementPopup.combos[i];
                const typed = movementPopup.input;
                return (
                  <button
                    key={node.id}
                    onClick={() => {
                      navigateTo(node);
                      setMovementPopup(null);
                    }}
                    className="flex items-center gap-3 py-2 px-2 border-b border-edge last:border-0 hover:bg-surface-hover rounded transition cursor-pointer text-left"
                  >
                    <span className="rounded bg-accent/35 text-xs font-bold font-mono px-1.5 py-0.5 shrink-0 tracking-widest border border-accent">
                      <span className="text-white">
                        {combo.slice(0, typed.length)}
                      </span>
                      <span className="text-white">
                        {combo.slice(typed.length)}
                      </span>
                    </span>
                    <span className="text-sm text-ink font-mono">
                      {node.label}
                    </span>
                  </button>
                );
              })}
            </div>
            {movementPopup.keyLen > 1 && (
              <div className="mt-3 font-mono text-sm text-center text-slate-300 tracking-widest min-h-[1.5em]">
                {movementPopup.input || (
                  <span className="text-slate-600">type combo…</span>
                )}
              </div>
            )}
            <div className={UI_POPUP_HINT_CLASS}>
              Type combo to select · Esc to cancel
            </div>
          </div>
        </div>
      )}

      {sticky && (
        <ResizableSidePanel
          ref={stickyPanelRef}
          accentColor={stickyNode.color}
          header={
            <PanelHeader
              title={fileTypeLabel(stickyNode.details.type)}
              titleColor={stickyNode.color}
            />
          }
          onClose={closeStickyPanel}
          onLayoutChange={setPanelLayout}
          maxContainerWidth={dimensions.width - remToPx(PANEL_GUTTER_REM)}
        >
          {isInspectMode && (
            <span className={PANEL_STATUS_BADGE_CLASS}>🔒 Locked View</span>
          )}
          {stickyNode.details.virtual_read === true && (
            <span className={PANEL_STATUS_BADGE_CLASS}>
              <HelpTerm label="Virtual read">
                Details come from Iceberg metadata. IceGraph does not read this
                file or verify that it exists.
              </HelpTerm>
            </span>
          )}
          <PanelIssueNotice type="error">
            {stickyNode.details.errors}
          </PanelIssueNotice>
          <PanelIssueNotice type="warning">
            {stickyNode.details.warnings}
          </PanelIssueNotice>
          {sticky.rows
            .filter((r) => !isEmptyValue(r.value))
            .map((r, i) => (
              <PanelDetailRow
                key={i}
                label={r.label}
                value={r.value}
                relaxedCollapse={
                  panelLayout.isFullscreen ||
                  panelLayout.panelWidthRem >= PANEL_WIDTH_RELAXED_REM
                }
              />
            ))}
          {stickyNode.details.type === FileType.PARTITION_STATISTICS &&
            stickyNode.details.partition_distribution && (
              <PartitionDistributionTable
                partitionDistribution={
                  stickyNode.details.partition_distribution
                }
              />
            )}
          {stickyNode.details.type === FileType.PARTITION_STATISTICS &&
            Array.isArray(stickyNode.details.sampled_partitions) && (
              <PartitionStatisticsTable
                partitionsCount={stickyNode.details.partitions_count ?? null}
                sampledPartitions={stickyNode.details.sampled_partitions}
              />
            )}
          {stickyReadableMetrics && (
            <>
              <ReadableMetricsSummary
                readableMetrics={stickyReadableMetrics}
                sizeScope="file"
                totalFileSizeBytes={stickyFileSizeBytes}
              />
              <DataFileReadableMetricsTable
                readableMetrics={stickyReadableMetrics}
                sizeScope="file"
                totalFileSizeBytes={stickyFileSizeBytes}
              />
            </>
          )}
        </ResizableSidePanel>
      )}
    </div>
  );
}
