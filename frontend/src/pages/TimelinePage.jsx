import { useEffect, useMemo, useRef, useState } from 'react'
import { useOutletContext } from 'react-router-dom'
import CopyIconButton from '../components/CopyIconButton'
import { useViewInGraph } from '../hooks/useViewInGraph'
import {
  PanelDetailRow,
  PanelHeader,
  PanelSectionTitle,
  PANEL_DIFF_AFTER_LABEL_CLASS,
  PANEL_DIFF_AFTER_VALUE_CLASS,
  PANEL_DIFF_BEFORE_LABEL_CLASS,
  PANEL_DIFF_BEFORE_VALUE_CLASS,
  PANEL_FIELD_LABEL_CLASS,
  PANEL_FIELD_LABEL_WIDE_CLASS,
  PANEL_COLLAPSE_TOGGLE_CLASS,
  DEFAULT_COLLAPSE_LINES,
} from '../components/PanelContent'
import {
  UI_BODY_MUTED_ITALIC_CLASS,
  UI_HELPER_TEXT_CLASS,
  UI_TOOLBAR_BUTTON_DEFAULT,
  UI_ZOOM_INDICATOR_CLASS,
} from '../uiTypography'
import ResizableSidePanel from '../components/ResizableSidePanel'
import { FileType } from '../graphConstants'
import { parseUtcDate, formatUtcOffset } from '../utils/dateUtils'
import { bindMouseScrollHandoff } from '../utils/smoothScroll'

const COLOR_A = '#1964B9'
const COLOR_B = '#6437D2'
const COLOR_C = '#0EA5E9'
const COLOR_INIT = '#D97706'

function formatTs(tsStr) {
  if (!tsStr) return null
  try {
    const d = new Date(tsStr)
    const ms = String(d.getMilliseconds()).padStart(3, '0')
    const offset = formatUtcOffset(d)
    const time = d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false })
    const day = String(d.getDate()).padStart(2, '0')
    const month = String(d.getMonth() + 1).padStart(2, '0')
    const date = `${day}-${month}-${d.getFullYear()}`
    return {
      date,
      time,
      ms,
      offset,
      full: `${date} ${time}.${ms}${offset}`,
    }
  } catch (e) {
    console.error(e)
    return null
  }
}

function formatDuration(tsA, tsB) {
  const dA = parseUtcDate(tsA)
  const dB = parseUtcDate(tsB)
  if (!dA || !dB) return '—'
  const diff = Math.abs(dA - dB)
  if (diff >= 86400000) return `${Math.round(diff / 86400000)}d`
  if (diff >= 3600000) return `${Math.round(diff / 3600000)}h`
  if (diff >= 60000) return `${Math.round(diff / 60000)}m`
  return `${Math.round(diff / 1000)}s`
}

function hasFieldChanged(before, after) {
  if (typeof before === 'object' || typeof after === 'object') {
    return JSON.stringify(before) !== JSON.stringify(after)
  }
  return before !== after
}

function diffListEntries(beforeArr, afterArr) {
  const key = (v) => JSON.stringify(v)
  const n = beforeArr.length
  const m = afterArr.length

  const dp = Array.from({ length: n + 1 }, () => new Array(m + 1).fill(0))
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i][j] = key(beforeArr[i]) === key(afterArr[j])
        ? dp[i + 1][j + 1] + 1
        : Math.max(dp[i + 1][j], dp[i][j + 1])
    }
  }

  const entries = []
  let i = 0, j = 0
  while (i < n && j < m) {
    if (key(beforeArr[i]) === key(afterArr[j])) {
      entries.push({ tone: 'same', value: beforeArr[i] })
      i++; j++
    } else if (dp[i + 1][j] >= dp[i][j + 1]) {
      entries.push({ tone: 'removed', value: beforeArr[i] })
      i++
    } else {
      entries.push({ tone: 'added', value: afterArr[j] })
      j++
    }
  }
  while (i < n) { entries.push({ tone: 'removed', value: beforeArr[i] }); i++ }
  while (j < m) { entries.push({ tone: 'added', value: afterArr[j] }); j++ }

  return entries
}

function colorFor(type) {
  if (type === 'A') return COLOR_A
  if (type === 'B') return COLOR_B
  if (type === 'C') return COLOR_C
  return COLOR_INIT
}

function labelFor(type) {
  if (type === 'A') return 'Write'
  if (type === 'B') return 'Metadata Op'
  if (type === 'C') return 'Branch Write'
  return 'Init'
}

function GraphLinkButton({ label, onClick, loading, disabled }) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      title={`Open ${label.toLowerCase()} in a new tab, selected in the graph`}
      className="inline-flex items-center gap-1.5 text-xs font-bold uppercase tracking-wide text-accent border border-accent/40 rounded-md px-2.5 py-1.5 hover:bg-accent-muted hover:border-accent transition-colors cursor-pointer disabled:opacity-50 disabled:cursor-not-allowed"
    >
      <svg className="w-3.5 h-3.5 shrink-0" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.8">
        <circle cx="4" cy="8" r="2" />
        <circle cx="12" cy="4" r="2" />
        <circle cx="12" cy="12" r="2" />
        <path d="M6 7.2L10 4.8M6 8.8L10 11.2" strokeLinecap="round" />
      </svg>
      {loading ? 'Opening…' : label}
    </button>
  )
}

const ZOOM_MIN = 0.35
const ZOOM_MAX = 3
const DEFAULT_VIEW = { zoom: 1, panX: 0, panY: 0 }

const TITLE_MAX_LINES = 3
const TITLE_LINE_HEIGHT = 1.25

function timelineSizes(zoom) {
  return {
    padY: 48 * zoom,
    padX: 64 * zoom,
    node: 44 * zoom,
    connector: 56 * zoom,
    gap: 8 * zoom,
    textMax: 160 * zoom,
    titleMax: 110 * zoom,
    fontMicro: 9.6 * zoom,
    fontDetail: 11.2 * zoom,
    fontXs: 12 * zoom,
    titleHeight: 12 * TITLE_LINE_HEIGHT * TITLE_MAX_LINES * zoom,
    arrowTop: 4 * zoom,
    arrowBottom: 4 * zoom,
    arrowLeft: 7 * zoom,
    nodeBorder: 2 * zoom,
    outline: 2 * zoom,
    outlineOffset: 3 * zoom,
    durMb: 4 * zoom,
  }
}

function contentNaturalSize(content, zoom) {
  return {
    width: content.offsetWidth / zoom,
    height: content.offsetHeight / zoom,
  }
}

function DiffBox({ label, lineCount, openBracket, closeBracket, children }) {
  const [isCollapsed, setIsCollapsed] = useState(true)
  const isCollapsible = lineCount > DEFAULT_COLLAPSE_LINES

  return (
    <div className="flex flex-col gap-2">
      <div className="flex items-center justify-between gap-2">
        <span className={`block ${PANEL_FIELD_LABEL_WIDE_CLASS}`}>
          {label.replace(/_/g, ' ')}
        </span>
        {isCollapsible && (
          <button
            type="button"
            onClick={() => setIsCollapsed(p => !p)}
            className={PANEL_COLLAPSE_TOGGLE_CLASS}
          >
            {isCollapsed ? `(${lineCount} lines) ▼` : '▲'}
          </button>
        )}
      </div>
      <div
        className="bg-diff-bg border border-edge rounded-lg py-3 font-mono text-xs overflow-x-auto shadow-2xl flex flex-col"
        style={
          isCollapsible && isCollapsed
            ? {
              maxHeight: `${DEFAULT_COLLAPSE_LINES}lh`,
              overflow: 'hidden',
              maskImage: 'linear-gradient(to bottom, black 60%, transparent 100%)',
              WebkitMaskImage: 'linear-gradient(to bottom, black 60%, transparent 100%)',
            }
            : {}
        }
      >
        <div className="px-4 py-0.5 text-slate-500 opacity-40">{openBracket}</div>
        <div className="flex flex-col">{children}</div>
        <div className="px-4 py-0.5 text-slate-500 opacity-40">{closeBracket}</div>
      </div>
    </div>
  )
}

function DiffLine({ tone, sign, children }) {
  const toneClass = tone === 'removed'
    ? 'bg-red-500/8 text-red-400'
    : tone === 'added'
      ? 'bg-green-500/8 text-green-400'
      : 'text-slate-300'
  const signOpacity = tone === 'same' ? 'opacity-20' : 'opacity-50'

  return (
    <div className={`${toneClass} px-8 py-0.5 flex gap-2`}>
      <span className={`w-3 shrink-0 ${signOpacity} text-center`}>{sign}</span>
      <span className="break-all">{children}</span>
    </div>
  )
}

function DiffRow({ label, before, after }) {
  const tryParse = (val) => (val && typeof val === 'object') ? val : null

  const beforeObj = tryParse(before)
  const afterObj = tryParse(after)

  if (beforeObj && afterObj) {
    if (Array.isArray(beforeObj) && Array.isArray(afterObj)) {
      const entries = diffListEntries(beforeObj, afterObj)
      return (
        <DiffBox label={label} lineCount={entries.length + 2} openBracket="[" closeBracket="]">
          {entries.map((entry, idx) => (
            <DiffLine key={idx} tone={entry.tone} sign={entry.tone === 'removed' ? '-' : entry.tone === 'added' ? '+' : ' '}>
              {JSON.stringify(entry.value)}
            </DiffLine>
          ))}
        </DiffBox>
      )
    }

    const allKeys = Array.from(new Set([...Object.keys(beforeObj), ...Object.keys(afterObj)])).sort()
    const lineCount = allKeys.reduce((n, key) => {
      const bVal = beforeObj[key]
      const aVal = afterObj[key]
      const changed = bVal !== undefined && aVal !== undefined && JSON.stringify(bVal) !== JSON.stringify(aVal)
      return n + (changed ? 2 : 1)
    }, 2)

    return (
      <DiffBox label={label} lineCount={lineCount} openBracket="{" closeBracket="}">
        {allKeys.map(key => {
          const bVal = beforeObj[key]
          const aVal = afterObj[key]
          const bStr = JSON.stringify(bVal)
          const aStr = JSON.stringify(aVal)

          if (bVal !== undefined && aVal === undefined) {
            return <DiffLine key={key} tone="removed" sign="-">"{key}": {bStr}</DiffLine>
          }
          if (bVal === undefined && aVal !== undefined) {
            return <DiffLine key={key} tone="added" sign="+">"{key}": {aStr}</DiffLine>
          }
          if (bStr !== aStr) {
            return (
              <div key={key} className="flex flex-col">
                <DiffLine tone="removed" sign="-">"{key}": {bStr}</DiffLine>
                <DiffLine tone="added" sign="+">"{key}": {aStr}</DiffLine>
              </div>
            )
          }
          return <DiffLine key={key} tone="same" sign=" ">"{key}": {bStr}</DiffLine>
        })}
      </DiffBox>
    )
  }

  const tryFormat = (val) => {
    if (!val) return val
    return typeof val === 'object' ? JSON.stringify(val, null, 2) : val
  }

  return (
    <div className="flex flex-col gap-1.5">
      <span className={`block ${PANEL_FIELD_LABEL_CLASS}`}>
        {label.replace(/_/g, ' ')}
      </span>
      <div className="grid grid-cols-2 gap-2">
        <div>
          <div className={PANEL_DIFF_BEFORE_LABEL_CLASS}>Before</div>
          <div className="relative">
            {before != null && before !== '' && <CopyIconButton text={tryFormat(before)} className="absolute top-1.5 right-1.5 z-10" />}
            <pre className={PANEL_DIFF_BEFORE_VALUE_CLASS}>
              {tryFormat(before) ?? '—'}
            </pre>
          </div>
        </div>
        <div>
          <div className={PANEL_DIFF_AFTER_LABEL_CLASS}>After</div>
          <div className="relative">
            {after != null && after !== '' && <CopyIconButton text={tryFormat(after)} className="absolute top-1.5 right-1.5 z-10" />}
            <pre className={PANEL_DIFF_AFTER_VALUE_CLASS}>
              {tryFormat(after) ?? '—'}
            </pre>
          </div>
        </div>
      </div>
    </div>
  )
}


function SnapSummary({ summary }) {
  const entries = summary ? Object.entries(summary) : []
  if (entries.length === 0) return null
  return (
    <div>
      <PanelSectionTitle>Summary</PanelSectionTitle>
      <div className="flex flex-col gap-3">
        {entries.map(([key, value]) => (
          <PanelDetailRow key={key} label={key} value={value} />
        ))}
      </div>
    </div>
  )
}

function DiffList({ diff }) {
  const rows = diff.filter(({ key }) => key.toLowerCase() !== 'type')
  return rows.length > 0
    ? (
      <div className="flex flex-col gap-4">
        {rows.map(({ key, before, after }) => <DiffRow key={key} label={key} before={before} after={after} />)}
      </div>
    )
    : <p className={UI_BODY_MUTED_ITALIC_CLASS}>No tracked field changes detected.</p>
}

export default function TimelinePage() {
  const { nodes } = useOutletContext()
  const { viewInGraph, duplicatingNodeId, canViewInGraph } = useViewInGraph()
  const [selected, setSelected] = useState(null)
  const [view, setView] = useState(DEFAULT_VIEW)
  const [isDragging, setIsDragging] = useState(false)
  const selectedRef = useRef(null)
  const eventsRef = useRef([])
  const viewportRef = useRef(null)
  const contentRef = useRef(null)
  const dragRef = useRef(null)
  const didPanRef = useRef(false)
  const popupScrollRef = useRef(null)
  const popupScrollTargetRef = useRef(0)
  const popupScrollRafRef = useRef(null)
  const viewAnimRafRef = useRef(null)
  const fitTimelineRef = useRef(null)

  useEffect(() => {
    const animatePopupScroll = () => {
      const el = popupScrollRef.current
      if (!el) { popupScrollRafRef.current = null; return }
      const diff = popupScrollTargetRef.current - el.scrollTop
      if (Math.abs(diff) < 0.5) {
        el.scrollTop = popupScrollTargetRef.current
        popupScrollRafRef.current = null
        return
      }
      el.scrollTop += diff * 0.14
      popupScrollRafRef.current = requestAnimationFrame(animatePopupScroll)
    }

    const scrollPopup = (delta) => {
      const el = popupScrollRef.current
      if (!el) return
      popupScrollTargetRef.current = Math.max(0, Math.min(popupScrollTargetRef.current + delta, el.scrollHeight - el.clientHeight))
      if (!popupScrollRafRef.current) popupScrollRafRef.current = requestAnimationFrame(animatePopupScroll)
    }

    const handleKey = (e) => {
      if (e.ctrlKey || e.metaKey || e.altKey) return
      if (['INPUT', 'TEXTAREA', 'SELECT'].includes(e.target.tagName) || e.target.isContentEditable) return
      if (e.key === 'Escape') { setSelected(null); return }
      if (selectedRef.current && (e.key === 'ArrowDown' || e.key === 'j')) {
        e.preventDefault()
        scrollPopup(80)
        return
      }
      if (selectedRef.current && (e.key === 'ArrowUp' || e.key === 'k')) {
        e.preventDefault()
        scrollPopup(-80)
        return
      }
      if (e.key === 'ArrowLeft' || e.key === 'h') {
        e.preventDefault()
        const evts = eventsRef.current
        if (!evts.length) return
        const idx = evts.indexOf(selectedRef.current)
        if (idx < 0) setSelected(evts[0])
        else if (idx > 0) setSelected(evts[idx - 1])
      }
      if (e.key === 'ArrowRight' || e.key === 'l') {
        e.preventDefault()
        const evts = eventsRef.current
        if (!evts.length) return
        const idx = evts.indexOf(selectedRef.current)
        if (idx < 0) setSelected(evts[evts.length - 1])
        else if (idx < evts.length - 1) setSelected(evts[idx + 1])
      }
      if (e.key === 'r' || e.key === 'R') {
        e.preventDefault()
        fitTimelineRef.current?.()
      }
    }
    window.addEventListener('keydown', handleKey)
    const unbindHandoff = bindMouseScrollHandoff(() => popupScrollRef.current, popupScrollTargetRef, popupScrollRafRef)
    return () => {
      window.removeEventListener('keydown', handleKey)
      unbindHandoff()
      if (popupScrollRafRef.current) cancelAnimationFrame(popupScrollRafRef.current)
    }
  }, [])

  const { events, snapshotMap, snapshotNodeIdMap } = useMemo(() => {
    const allNodes = nodes || []

    const metaNodes = allNodes
      .filter(n => n.type === FileType.METADATA || n.type === FileType.MAIN_METADATA)
      .filter(n => n.details.timestamp)
      .sort((a, b) => new Date(a.details.timestamp) - new Date(b.details.timestamp))

    const snapMap = {}
    const snapNodeIdMap = {}
    allNodes
      .filter(n => n.type === FileType.SNAPSHOT)
      .forEach(n => {
        const d = n.details
        if (d.snapshot_id) {
          snapMap[d.snapshot_id] = d
          snapNodeIdMap[d.snapshot_id] = n.id
        }
      })

    const timeline = metaNodes.map(({ details, id: metadataNodeId }, i) => {
      const prev = i > 0 ? metaNodes[i - 1].details : null
      let type = !prev
        ? 'init'
        : details.snapshot_id !== prev.snapshot_id
          ? 'A'
          : 'B'

      let branchSnapId = null
      let branchName = null

      if (prev && details.refs && prev.refs) {
        const currentRefs = details.refs
        const prevRefs = prev.refs

        for (const key of Object.keys(prevRefs)) {
          if (currentRefs[key] && prevRefs[key]) {
            const currentSnapId = currentRefs[key]['snapshot-id']
            const prevSnapId = prevRefs[key]['snapshot-id']
            if (currentSnapId !== prevSnapId) {
              branchSnapId = currentSnapId
              branchName = key
              break
            }
          }
        }
      }

      if (type === 'B' && branchSnapId) {
        type = 'C'
      }

      const diff =
        (type === 'B' || type === 'C') && prev
          ? Object.keys(details)
            .filter(k => hasFieldChanged(details[k], prev[k]))
            .map(k => ({ key: k, before: prev[k], after: details[k] }))
          : []

      return {
        details,
        type,
        diff,
        snapshotId: type === 'C' ? branchSnapId : details.snapshot_id,
        branchName,
        metadataNodeId,
      }
    })

    return { events: timeline, snapshotMap: snapMap, snapshotNodeIdMap: snapNodeIdMap }
  }, [nodes])

  const animateToView = (target, duration = 450) => {
    if (viewAnimRafRef.current) cancelAnimationFrame(viewAnimRafRef.current)
    const { zoom: startZoom, panX: startPanX, panY: startPanY } = view
    const startTime = performance.now()
    const easeOutCubic = (t) => 1 - Math.pow(1 - t, 3)
    const step = (now) => {
      const t = Math.min(1, (now - startTime) / duration)
      const e = easeOutCubic(t)
      setView({
        zoom: startZoom + (target.zoom - startZoom) * e,
        panX: startPanX + (target.panX - startPanX) * e,
        panY: startPanY + (target.panY - startPanY) * e,
      })
      if (t < 1) viewAnimRafRef.current = requestAnimationFrame(step)
      else viewAnimRafRef.current = null
    }
    viewAnimRafRef.current = requestAnimationFrame(step)
  }

  const fitTimeline = () => {
    const viewport = viewportRef.current
    const content = contentRef.current
    if (!viewport || !content) return
    const padding = 48
    const { width, height } = contentNaturalSize(content, view.zoom)
    const fitZoom = Math.min(
      1,
      (viewport.clientWidth - padding) / width,
      (viewport.clientHeight - padding) / height,
    )
    animateToView({
      zoom: fitZoom,
      panX: (viewport.clientWidth - width * fitZoom) / 2,
      panY: (viewport.clientHeight - height * fitZoom) / 2,
    })
  }
  fitTimelineRef.current = fitTimeline

  useEffect(() => {
    selectedRef.current = selected
    popupScrollTargetRef.current = 0
    if (popupScrollRef.current) popupScrollRef.current.scrollTop = 0
  }, [selected])
  useEffect(() => { eventsRef.current = events }, [events])

  useEffect(() => {
    if (events.length === 0) return
    const id = requestAnimationFrame(fitTimeline)
    return () => cancelAnimationFrame(id)
  }, [events.length])

  useEffect(() => () => {
    if (viewAnimRafRef.current) cancelAnimationFrame(viewAnimRafRef.current)
  }, [])

  useEffect(() => {
    const viewport = viewportRef.current
    if (!viewport) return

    const onWheel = (e) => {
      e.preventDefault()
      const rect = viewport.getBoundingClientRect()

      if (Math.abs(e.deltaX) > Math.abs(e.deltaY)) {
        setView(v => ({ ...v, panX: v.panX - e.deltaX }))
        return
      }
      if (e.shiftKey) {
        setView(v => ({ ...v, panX: v.panX - e.deltaY }))
        return
      }

      const cx = e.clientX - rect.left
      const cy = e.clientY - rect.top
      const factor = Math.exp(-e.deltaY * 0.002)
      setView(v => {
        const newZoom = Math.min(ZOOM_MAX, Math.max(ZOOM_MIN, v.zoom * factor))
        const scale = newZoom / v.zoom
        return {
          zoom: newZoom,
          panX: cx - (cx - v.panX) * scale,
          panY: cy - (cy - v.panY) * scale,
        }
      })
    }

    viewport.addEventListener('wheel', onWheel, { passive: false })
    return () => viewport.removeEventListener('wheel', onWheel)
  }, [])

  useEffect(() => {
    const onMouseMove = (e) => {
      const drag = dragRef.current
      if (!drag) return
      const dx = e.clientX - drag.x
      const dy = e.clientY - drag.y
      if (Math.abs(dx) > 3 || Math.abs(dy) > 3) didPanRef.current = true
      setView(v => ({
        ...v,
        panX: drag.panX + dx,
        panY: drag.panY + dy,
      }))
    }
    const onMouseUp = () => { dragRef.current = null; setIsDragging(false) }
    window.addEventListener('mousemove', onMouseMove)
    window.addEventListener('mouseup', onMouseUp)
    return () => {
      window.removeEventListener('mousemove', onMouseMove)
      window.removeEventListener('mouseup', onMouseUp)
    }
  }, [])

  const startPan = (e) => {
    if (e.button !== 0) return
    dragRef.current = { x: e.clientX, y: e.clientY, panX: view.panX, panY: view.panY }
    didPanRef.current = false
    setIsDragging(true)
  }

  const selectEvent = (event) => {
    if (!didPanRef.current) setSelected(event)
  }

  if (events.length === 0) {
    return (
      <div className="flex-1 flex items-center justify-center bg-graph-grid">
        <p className={UI_BODY_MUTED_ITALIC_CLASS}>No metadata history available.</p>
      </div>
    )
  }

  const selectedSnap = selected ? snapshotMap[selected.snapshotId] : null
  const closePanel = () => setSelected(null)
  const tl = timelineSizes(view.zoom)

  return (
    <div className="flex-1 flex flex-col bg-graph-grid overflow-hidden relative">
      <div className="shrink-0 px-8 pt-5 flex items-center gap-5">
        {[['init', 'Initial State'], ['A', 'Write'], ['B', 'Metadata Op'], ['C', 'Branch Write']].map(([type, lbl]) => (
          <div key={type} className="flex items-center gap-1.5">
            <div className="w-2.5 h-2.5 rounded-full" style={{ backgroundColor: colorFor(type) }} />
            <span className={UI_HELPER_TEXT_CLASS}>{lbl}</span>
          </div>
        ))}
      </div>

      <div
        ref={viewportRef}
        className={`flex-1 relative overflow-hidden ${isDragging ? 'cursor-grabbing' : 'cursor-grab'}`}
        onMouseDown={startPan}
      >
        <div
          className="absolute top-0 left-0"
          style={{ transform: `translate(${view.panX}px, ${view.panY}px)` }}
        >
          <div
            ref={contentRef}
            className="flex items-start min-w-max"
            style={{ padding: `${tl.padY}px ${tl.padX}px` }}
          >
            {events.map((event, i) => {
              const ts = formatTs(event.details.timestamp)
              const canShowOperation = event.type === 'A' || event.type === 'C' || event.type === 'init'
              const operation = canShowOperation ? snapshotMap[event.snapshotId]?.operation : null
              return (
                <div key={i} className="flex items-center">
                  {i > 0 && (
                    <div className="flex flex-col items-center shrink-0">
                      <div
                        className="text-slate-500"
                        style={{ fontSize: tl.fontMicro, marginBottom: tl.durMb }}
                      >
                        {formatDuration(events[i - 1].details.timestamp, events[i].details.timestamp)}
                      </div>
                      <div className="flex items-center">
                        <div className="h-px bg-edge" style={{ width: tl.connector }} />
                        <div style={{
                          width: 0, height: 0,
                          borderTop: `${tl.arrowTop}px solid transparent`,
                          borderBottom: `${tl.arrowBottom}px solid transparent`,
                          borderLeft: `${tl.arrowLeft}px solid #2d3748`,
                        }} />
                      </div>
                    </div>
                  )}
                  <div
                    className="relative flex flex-col items-center cursor-pointer select-none"
                    style={{ gap: tl.gap, paddingTop: tl.titleHeight + tl.gap }}
                    onClick={() => selectEvent(event)}
                  >
                    <div
                      className="absolute top-0 left-1/2 -translate-x-1/2 text-center flex flex-col justify-end"
                      style={{ width: tl.titleMax, height: tl.titleHeight }}
                    >
                      <div
                        className="font-mono font-bold leading-tight break-words capitalize line-clamp-3"
                        style={{ fontSize: tl.fontXs, color: colorFor(event.type) }}
                        title={event.details.file_path ?? ''}
                      >
                        {operation || labelFor(event.type)}
                      </div>
                    </div>
                    <div
                      className="rounded-full shadow-lg shrink-0 transition-[outline]"
                      style={{
                        width: tl.node,
                        height: tl.node,
                        borderWidth: tl.nodeBorder,
                        borderStyle: 'solid',
                        backgroundColor: colorFor(event.type),
                        borderColor: colorFor(event.type),
                        ...(selected === event ? { outline: `${tl.outline}px solid white`, outlineOffset: tl.outlineOffset } : {}),
                      }}
                    />
                    <div className="text-center" style={{ maxWidth: tl.textMax }}>
                      {ts && (
                        <>
                          <div className="text-slate-500 leading-tight" style={{ fontSize: tl.fontDetail }}>{ts.date}</div>
                          <div className="text-slate-300 leading-tight" style={{ fontSize: tl.fontDetail }}>{ts.time}</div>
                        </>
                      )}
                    </div>
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      </div>

      <div className="absolute bottom-4 left-4 flex flex-col gap-2 z-10 font-sans w-52">
        <button
          type="button"
          className={UI_TOOLBAR_BUTTON_DEFAULT}
          onClick={fitTimeline}
          onMouseDown={e => e.preventDefault()}
        >
          Fit Timeline
        </button>
        <div className={UI_ZOOM_INDICATOR_CLASS}>
          {Math.round(view.zoom * 100)}%
        </div>
      </div>

      {selected && (
        <ResizableSidePanel
          ref={popupScrollRef}
          accentColor={colorFor(selected.type)}
          onClose={closePanel}
          header={(
            <PanelHeader
              title={labelFor(selected.type)}
              titleColor={colorFor(selected.type)}
              subtitle={selected.details.file_path}
              meta={formatTs(selected.details.timestamp)?.full}
            />
          )}
        >
          <div className="flex items-center justify-center gap-2 pb-4 border-b border-edge">
            <GraphLinkButton
              label="Metadata"
              onClick={e => viewInGraph(e, selected.metadataNodeId)}
              loading={duplicatingNodeId === selected.metadataNodeId}
              disabled={!canViewInGraph || !!duplicatingNodeId}
            />
            {snapshotNodeIdMap[selected.snapshotId] && (
              <GraphLinkButton
                label="Snapshot"
                onClick={e => viewInGraph(e, snapshotNodeIdMap[selected.snapshotId])}
                loading={duplicatingNodeId === snapshotNodeIdMap[selected.snapshotId]}
                disabled={!canViewInGraph || !!duplicatingNodeId}
              />
            )}
          </div>

          {selected.type === 'A' && (
            <>
              <PanelDetailRow label="Snapshot ID" value={selected.snapshotId} />
              {selectedSnap && (
                <>
                  <PanelDetailRow label="Operation" value={selectedSnap.operation} />
                  <SnapSummary summary={selectedSnap.summary} />
                </>
              )}
            </>
          )}

          {selected.type === 'C' && (
            <>
              <PanelDetailRow label="Branch Name" value={selected.branchName} />
              <PanelDetailRow label="Snapshot ID" value={selected.snapshotId} />
              {selectedSnap && (
                <>
                  <PanelDetailRow label="Operation" value={selectedSnap.operation} />
                  <SnapSummary summary={selectedSnap.summary} />
                </>
              )}
              <div className="mt-2 border-t border-edge pt-4">
                <PanelSectionTitle className="mb-3">Metadata Changes</PanelSectionTitle>
                <DiffList diff={selected.diff} />
              </div>
            </>
          )}

          {selected.type === 'B' && <DiffList diff={selected.diff} />}

          {selected.type === 'init' && (
            <>
              <PanelDetailRow label="Snapshot ID" value={selected.details.snapshot_id} />
              <PanelDetailRow label="Schema ID" value={selected.details.current_schema_id} />
              <PanelDetailRow label="Spec ID" value={selected.details.partition_spec_id} />
              <PanelDetailRow label="Sort Order ID" value={selected.details.sort_order_id} />
              {selectedSnap && (
                <>
                  <PanelDetailRow label="Operation" value={selectedSnap.operation} />
                  <SnapSummary summary={selectedSnap.summary} />
                </>
              )}
              {selected.details.properties && Object.keys(selected.details.properties).length > 0 && (
                <div>
                  <PanelSectionTitle>Properties</PanelSectionTitle>
                  <div className="flex flex-col gap-3">
                    {Object.entries(selected.details.properties).map(([key, value]) => (
                      <PanelDetailRow key={key} label={key} value={value} />
                    ))}
                  </div>
                </div>
              )}
            </>
          )}
        </ResizableSidePanel>
      )}
    </div>
  )
}
