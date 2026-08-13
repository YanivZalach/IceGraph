import { useState } from "react";
import CopyIconButton from "./CopyIconButton";
import {
  UI_BODY_MUTED_ITALIC_CLASS,
  UI_FIELD_LABEL_CLASS,
  UI_FIELD_LABEL_WIDE_CLASS,
  UI_SECTION_HEADING_CLASS,
} from "../uiTypography";

export const PANEL_TITLE_CLASS =
  "text-base font-bold uppercase tracking-wide text-ink leading-snug";

export const PANEL_SUBTITLE_CLASS =
  "text-sm font-mono text-slate-400 mt-1 break-words";

export const PANEL_META_CLASS = "text-sm text-ink mt-1";

export const PANEL_FIELD_LABEL_CLASS = UI_FIELD_LABEL_CLASS;

export const PANEL_FIELD_LABEL_WIDE_CLASS = UI_FIELD_LABEL_WIDE_CLASS;

export const PANEL_SECTION_TITLE_CLASS = UI_SECTION_HEADING_CLASS;

export const PANEL_VALUE_CLASS =
  "block font-mono bg-canvas text-slate-200 pl-3 pr-9 py-2 rounded-lg text-xs whitespace-pre overflow-x-auto break-normal";

export const PANEL_COLLAPSE_TOGGLE_CLASS =
  "text-xs font-bold uppercase tracking-wide text-accent hover:text-white transition shrink-0";

export const PANEL_STATUS_BADGE_CLASS =
  "inline-flex items-center gap-1.5 bg-accent/10 text-accent px-2.5 py-1 rounded-md text-xs font-bold uppercase tracking-wide w-fit";

export const PANEL_EMPTY_MESSAGE_CLASS = UI_BODY_MUTED_ITALIC_CLASS;

export const PANEL_DIFF_COMPARE_LABEL_CLASS = "text-xs font-semibold mb-0.5";

export const PANEL_DIFF_BEFORE_LABEL_CLASS = `${PANEL_DIFF_COMPARE_LABEL_CLASS} text-red-400/90`;

export const PANEL_DIFF_AFTER_LABEL_CLASS = `${PANEL_DIFF_COMPARE_LABEL_CLASS} text-green-400/90`;

export const PANEL_DIFF_VALUE_BASE_CLASS =
  "text-xs rounded p-2 pr-9 overflow-x-auto whitespace-pre-wrap break-all min-h-8";

export const PANEL_DIFF_BEFORE_VALUE_CLASS = `${PANEL_DIFF_VALUE_BASE_CLASS} bg-red-950/30 border border-red-900/40 text-red-300`;

export const PANEL_DIFF_AFTER_VALUE_CLASS = `${PANEL_DIFF_VALUE_BASE_CLASS} bg-green-950/30 border border-green-900/40 text-green-300`;

export const DEFAULT_COLLAPSE_LINES = 15;

const colorParseCtx = document.createElement("canvas").getContext("2d");

function stripAlpha(color) {
  if (typeof color !== "string") return color;
  colorParseCtx.fillStyle = color;
  const m = colorParseCtx.fillStyle.match(/^rgba?\((\d+),\s*(\d+),\s*(\d+)/);

  return m ? `rgb(${m[1]}, ${m[2]}, ${m[3]})` : colorParseCtx.fillStyle;
}

function renderBreakablePath(path) {
  const segments = path.split("/");
  return segments.map((segment, i) => (
    <span key={i}>
      {segment}
      {i < segments.length - 1 ? (
        <>
          /<wbr />
        </>
      ) : null}
    </span>
  ));
}

export function PanelHeader({ title, titleColor, subtitle, meta }) {
  const opaqueColor = stripAlpha(titleColor);
  return (
    <div className="min-w-0 pr-4">
      <div
        className={PANEL_TITLE_CLASS}
        style={opaqueColor ? { color: opaqueColor } : undefined}
      >
        {title}
      </div>
      {subtitle ? (
        <div className={PANEL_SUBTITLE_CLASS}>
          {renderBreakablePath(subtitle)}
        </div>
      ) : null}
      {meta ? <div className={PANEL_META_CLASS}>{meta}</div> : null}
    </div>
  );
}

export function PanelSectionTitle({ children, className = "" }) {
  return (
    <div className={`${PANEL_SECTION_TITLE_CLASS} ${className}`}>
      {children}
    </div>
  );
}

export function isEmptyValue(value) {
  if (value == null || value === "") return true;
  if (Array.isArray(value)) return value.length === 0;
  if (typeof value === "object") return Object.keys(value).length === 0;
  return false;
}

export function PanelDetailRow({
  label,
  value,
  relaxedCollapse = false,
  collapseLineCount = DEFAULT_COLLAPSE_LINES,
}) {
  const displayValue =
    typeof value === "object" && value !== null
      ? JSON.stringify(value, null, 2)
      : value;

  const textToCopy =
    displayValue != null && displayValue !== "" ? String(displayValue) : "";
  const hasValue = textToCopy !== "";
  const lineCount = hasValue ? textToCopy.split("\n").length : 0;
  const isCollapsible = lineCount > collapseLineCount;
  const [isCollapsed, setIsCollapsed] = useState(true);

  return (
    <div>
      <div className="flex items-center justify-between mb-1 gap-2">
        <span className={`block ${PANEL_FIELD_LABEL_CLASS}`}>{label}</span>
        {isCollapsible && (
          <button
            type="button"
            onClick={() => setIsCollapsed((p) => !p)}
            className={PANEL_COLLAPSE_TOGGLE_CLASS}
          >
            {isCollapsed ? `(${lineCount} lines) ▼` : "▲"}
          </button>
        )}
      </div>
      <div className="relative">
        {hasValue && (
          <CopyIconButton
            text={textToCopy}
            className="absolute top-1 right-2 z-10"
          />
        )}
        <span
          className={PANEL_VALUE_CLASS}
          style={
            isCollapsible && isCollapsed
              ? {
                  maxHeight: relaxedCollapse ? "22lh" : "10lh",
                  overflow: "hidden",
                  maskImage:
                    "linear-gradient(to bottom, black 60%, transparent 100%)",
                  WebkitMaskImage:
                    "linear-gradient(to bottom, black 60%, transparent 100%)",
                }
              : {}
          }
        >
          {hasValue ? textToCopy : "-"}
        </span>
      </div>
    </div>
  );
}
