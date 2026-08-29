# Frontend Development

Read [`philosophy.md`](philosophy.md) before changing `frontend/`. This file defines the workflow;
the philosophy defines technical conventions. Paths are relative to the repository root.

The product invariants in `AGENTS.md` apply. In addition, the frontend must not recreate backend
domain rules and must support remote backends, the Docker demo, and MSW mock mode.

## Before planning

Trace the current behavior from its route or user action through URL state, client state, server
data, transformation, rendering, and errors. Identify one primary owner and its direct dependencies.

Describe the proposed change as one end-to-end flow. Explain any new ownership boundary. Do not
present disconnected file edits as an architecture.

## Implementation choices

- Choose the smallest coherent model that makes correct behavior unsurprising.
- Extend the existing owner instead of creating a parallel path.
- Keep related logic together until extraction creates an immediate readability or ownership
  benefit.
- Add a file, hook, component, helper, or state container only when it has one clear responsibility.
- Prefer direct data flow and derived values over duplicated or synchronized state.
- Reuse an abstraction only when its contract fits exactly. Similar-looking code is not sufficient.
- Prefer fewer lines when readability improves. Do not compress responsibilities or use clever code
  to reduce line count.
- Preserve behavior and architecture outside the approved scope.

## Impact check

Before finishing, inspect each category and report which ones applied:

- **Entry points:** routes, tabs, buttons, keyboard shortcuts, and cross-tab links.
- **Views:** Timeline, Metadata, FileTree, and Graph when they share graph or snapshot state.
- **State:** URL parameters, reload and browser history, new-tab flows, reset behavior, and the
  persisted [browser graph cache](../../browser-graph-cache.md), including identity, validation,
  restoration, invalidation, and explicit recompilation.
- **Contracts:** API schemas, backend responses, and MSW handlers.
- **Documentation:** the relevant content under `frontend/src/features/docs/content/`.
- **External skill:** `claude-plugin/skills/icegraph/SKILL.md` for route or deep-link changes.

Check the reverse flow. State introduced by opening, selecting, filtering, or locking must have a
clear way to close, clear, reset, or leave without affecting the next view.

## Simplification pass

Before presenting the result, ask:

- Can a new file, layer, state value, or abstraction be removed?
- Does understanding the change require unnecessary jumps between files?
- Is a derived value stored or synchronized?
- Is one concept represented multiple ways without a boundary requiring it?
- Can the full change be understood from the primary owner and its direct dependencies?

Do not broaden scope during this pass. Ask before a simplification changes behavior.

## Repository-specific constraints

- Generated skill copies are documented in `AGENTS.md`; never edit them directly.
- Use Vite's configured base path. Never hardcode deployment-root asset or route paths.

Use the smallest direct proof for the changed behavior, then run the mandatory checks from
`AGENTS.md`.
