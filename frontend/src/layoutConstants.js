/** Layout values in rem — use with CSS tokens or remToPx() for canvas / drag math. */
export const NAV_HEIGHT_REM = 4;
export const PANEL_WIDTH_DEFAULT_REM = 25;
export const PANEL_WIDTH_MIN_REM = 20;
export const PANEL_WIDTH_RELAXED_REM = 35;
export const PANEL_GUTTER_REM = 2;
export const PANEL_ACCENT_BORDER_REM = 0.3125;

export const GRAPH_NODE_FONT_REM = 5;
export const GRAPH_NODE_PADDING_X_REM = 2.5;
export const GRAPH_NODE_PADDING_Y_REM = 1.75;

export function getRootFontSize() {
  if (typeof document === "undefined") return 16;
  return parseFloat(getComputedStyle(document.documentElement).fontSize) || 16;
}

export function remToPx(rem) {
  return rem * getRootFontSize();
}

export function pxToRem(px) {
  return px / getRootFontSize();
}

export function getNavHeightPx() {
  return remToPx(NAV_HEIGHT_REM);
}
