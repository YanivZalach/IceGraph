// Shared muted body text (slate-400, sm)
export const UI_BODY_MUTED_CLASS = "text-sm text-slate-400";

export const UI_BODY_MUTED_ITALIC_CLASS = `${UI_BODY_MUTED_CLASS} italic`;

export const UI_EMPTY_PLACEHOLDER_CLASS = "text-slate-600 italic";

export const UI_HELPER_TEXT_CLASS = "text-xs text-slate-400";

export const UI_POPUP_HINT_CLASS = `${UI_HELPER_TEXT_CLASS} mt-2`;

export const UI_FOOTER_TEXT_CLASS = "text-center text-xs text-slate-500 py-4";

export const UI_ERROR_TEXT_CLASS = "text-xs text-rose-400";

export const UI_ERROR_TEXT_SPACED_CLASS = `mt-2 ${UI_ERROR_TEXT_CLASS}`;

// Uppercase field labels (caption size, slate-500)
export const UI_FIELD_LABEL_CLASS =
  "text-xs font-bold text-slate-500 uppercase tracking-wider";

export const UI_FIELD_LABEL_WIDE_CLASS = `${UI_FIELD_LABEL_CLASS} tracking-widest`;

export const UI_FIELD_LABEL_MB_CLASS = `${UI_FIELD_LABEL_CLASS} mb-1`;

export const UI_SECTION_HEADING_CLASS = `${UI_FIELD_LABEL_CLASS} mb-2`;

export const UI_DIALOG_SECTION_TITLE_CLASS = `${UI_FIELD_LABEL_CLASS} mb-3`;

// Uppercase form/metadata labels (xs size, slate-400)
export const UI_CAPTION_LABEL_SLATE400_CLASS =
  "text-xs font-bold text-slate-400 uppercase tracking-wider";

export const UI_FORM_LABEL_CLASS = `block ${UI_CAPTION_LABEL_SLATE400_CLASS}`;

export const UI_FORM_LABEL_MB_CLASS = `${UI_FORM_LABEL_CLASS} mb-2`;

export const UI_METADATA_SECTION_TITLE_CLASS = UI_CAPTION_LABEL_SLATE400_CLASS;

export const UI_LINK_BUTTON_CLASS =
  "text-xs font-bold text-accent hover:text-accent-dark disabled:text-slate-500 disabled:cursor-not-allowed transition";

const UI_PRIMARY_BUTTON_BASE =
  "bg-accent hover:bg-accent-dark text-white font-bold rounded-lg transition text-sm";

export const UI_PRIMARY_BUTTON_CLASS = `${UI_PRIMARY_BUTTON_BASE} active:bg-accent-dark py-2.5 tracking-wide`;

export const UI_PRIMARY_BUTTON_SM_CLASS = `${UI_PRIMARY_BUTTON_BASE} py-2`;

const UI_TEXT_INPUT_BASE =
  "w-full border border-edge bg-edge text-ink placeholder-slate-500 focus:outline-none focus:ring-2 focus:ring-accent/40 focus:border-accent transition";

export const UI_TEXT_INPUT_CLASS = `${UI_TEXT_INPUT_BASE} rounded-lg px-3 py-2 text-sm`;

export const UI_TEXT_INPUT_LG_CLASS = `${UI_TEXT_INPUT_BASE} rounded-lg px-4 py-2.5 text-sm`;

export const UI_FILTER_INPUT_CLASS = `${UI_TEXT_INPUT_BASE} rounded-md px-3 py-1.5 text-xs`;

export const UI_TOOLBAR_BUTTON_LAYOUT =
  "w-full rounded-lg cursor-pointer font-bold text-xs uppercase tracking-wide shadow-md transition";

export const UI_TOOLBAR_BUTTON_BASE = `${UI_TOOLBAR_BUTTON_LAYOUT} py-2.5`;

export const UI_TOOLBAR_BUTTON_DEFAULT = `${UI_TOOLBAR_BUTTON_BASE} bg-surface text-accent border border-accent hover:bg-edge`;

export const UI_TOOLBAR_BUTTON_ACTIVE = `${UI_TOOLBAR_BUTTON_BASE} bg-accent text-white hover:bg-accent-dark border border-accent`;

export function toolbarButtonClass(active, extra = "") {
  const base = active ? UI_TOOLBAR_BUTTON_ACTIVE : UI_TOOLBAR_BUTTON_DEFAULT;
  return extra ? `${base} ${extra}` : base;
}

export const UI_ZOOM_INDICATOR_CLASS =
  "text-center text-tiny text-slate-500 tabular-nums";

export const UI_DOCS_NAV_TITLE_CLASS =
  "text-xs font-semibold uppercase tracking-wider text-slate-500 mb-3";

export const UI_DOCS_BODY_CLASS = "text-slate-300 text-sm leading-relaxed";

export const UI_MONO_MUTED_CLASS = "text-xs font-mono text-slate-400";

export const UI_MONO_MUTED_NOWRAP_CLASS = `${UI_MONO_MUTED_CLASS} whitespace-nowrap`;

export const UI_MONO_VALUE_CLASS = "text-sm font-mono text-ink break-all";

export const UI_TABLE_NAME_BUTTON_CLASS =
  "text-sm font-mono px-3 py-1 rounded-md border border-slate-600 text-slate-300 hover:text-white hover:border-slate-400 hover:bg-surface-hover transition cursor-pointer";

export const UI_PAGE_TITLE_CLASS = "text-xl font-bold text-ink";

export const UI_DIALOG_TITLE_CLASS = "font-bold text-ink text-sm";

export const UI_STRUCTURED_SECTION_TITLE_CLASS =
  "text-sm font-black text-slate-400 uppercase tracking-normal mb-3 border-b border-edge pb-1.5 flex justify-between items-center";

export const UI_FILE_COUNT_BADGE_CLASS =
  "text-base font-bold bg-edge text-slate-400 px-2 py-0.5 rounded-full";

export const UI_COPYABLE_VALUE_CLASS =
  "block text-sm text-ink break-all pl-3 pr-9 py-2";
