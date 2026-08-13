# CLAUDE.md — Frontend

Guidance for work under `frontend/`. The repo-root [CLAUDE.md](../CLAUDE.md) also applies — read it for workflow rules (plan-first, bugs, scope), project overview, commands, backend architecture, API flow, and deployment notes. Paths below are relative to the repo root.

All frontend code conventions live in the project philosophy and are mandatory:

@PHILOSOPHY.md

## Hard rules most often violated

- No `any`, ever.
- No `useEffect` without justifying it to the user first.
- No `memo` / `useMemo` / `useCallback` — React Compiler handles it.
- `useSuspenseQuery` by default; plain `useQuery` needs justification.
- No what-comments; why-comments must carry a URL/issue number.
- No shortened names — `overdueInvoices`, never `fltrd`. Full naming rules in the philosophy.

## Frontend Change Policy

- When a change affects the user interface (behavior, navigation, interactions, views), update the relevant section in the Docs page (`frontend/src/pages/DocsPage.jsx`).
- When a change affects the frontend's URL structure (routes, path params, query params), update `claude-plugin/skills/icegraph/SKILL.md` to match.
- The Issues panel content (errors and warnings) is driven entirely by the backend response (`data.errors`, `data.warnings`). UI changes to this panel must be coordinated with backend error/warning emission logic.

## Dev Notes

- `npm run dev` / `npm run build` run `copy-skill` first, which copies `claude-plugin/skills/icegraph/SKILL.md` into `public/` — `public/SKILL.md` is generated, never edit it directly.
- Set `VITE_USE_MSW=true` to run against `mocks/` instead of a live backend (this is how the GitHub Pages demo works). New or changed API responses must be reflected in `mocks/` or the demo breaks.
- The Vite `base` path is `/IceGraph/` for GitHub Pages but `/` for Docker — never hardcode absolute asset or route paths.
- Vite dev server runs on port 3000 and proxies `/api` to the backend on port 5000.

## Current codebase (pre-refactor)

The sections below describe the existing code, which predates [PHILOSOPHY.md](PHILOSOPHY.md) (plain JSX, no TypeScript, no router/query/state libraries, hand-rolled UI tokens). A refactor toward the philosophy is planned. **New and refactored code follows the philosophy; use this section only to navigate code not yet migrated.** Delete each part as the migration lands.

### Layout

React SPA in `frontend/src/`:

- Pages: `GraphPage` (force-graph visualization), `MetadataPage`, `TimelinePage`, `FileTreePage`, `SnapshotSelectionPage`, `HomePage`, `DocsPage`
- `TableLayout.jsx` wraps all table-specific pages; `context/TableSpecsContext.jsx` shares table state
- `graphConstants.js` defines node/link visual constants and `fileTypeLabel()` for human-readable node types
- `uiTypography.js` — shared Tailwind class tokens for labels, body text, inputs, buttons, and toolbar controls
- `layoutConstants.js` / `appConstants.js` — layout dimensions and app-wide constants
- `components/PanelContent.jsx` — side-panel components (`PanelHeader`, `PanelDetailRow`, `PanelSectionTitle`) and panel-specific typography tokens
- `components/ResizableSidePanel.jsx` — draggable side panel shell used by Graph and Timeline
- `hooks/`, `utils/` — shared behavior and helpers; check here before writing a new one
- `mocks/` — MSW handlers used in the GitHub Pages demo (no real backend)

### Styling Conventions (legacy token system)

Typography and repeated UI patterns live in `frontend/src/uiTypography.js`. When touching unmigrated code, prefer importing tokens from there instead of duplicating Tailwind class strings.

**Token layers:**

| File                | Role                                                                                      |
| ------------------- | ----------------------------------------------------------------------------------------- |
| `uiTypography.js`   | App-wide tokens: form labels, muted body text, inputs, primary/toolbar buttons, mono text |
| `PanelContent.jsx`  | Side-panel tokens and components; re-exports field-label tokens from `uiTypography.js`    |
| `graphConstants.js` | Graph-specific visuals (`NODE_STYLE_MAP`, `fileTypeLabel`)                                |

**Common tokens:**

- `UI_BODY_MUTED_CLASS` — secondary paragraph text (`text-sm text-slate-400`)
- `UI_FIELD_LABEL_CLASS` — uppercase field labels in panels and metadata rows (caption size, slate-500)
- `UI_FORM_LABEL_CLASS` — uppercase form labels (xs size, slate-400, block)
- `UI_TOOLBAR_BUTTON_BASE` — standard graph toolbar button with `py-2.5`
- `UI_TOOLBAR_BUTTON_LAYOUT` — same toolbar look **without** vertical padding; use for split buttons (e.g. Inspect/Locked) where inner spans supply `py-2.5`
- `toolbarButtonClass(active)` — active/inactive toolbar button variant

**Side panel:** Graph and Timeline both use `ResizableSidePanel` + `PanelContent`. Panel headers call `fileTypeLabel(nodeType)` from `graphConstants.js`. Timeline diff rows use `PANEL_DIFF_*` tokens for Before/After labels and values.
