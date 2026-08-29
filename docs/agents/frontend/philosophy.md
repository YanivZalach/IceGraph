# Frontend Philosophy

Optimize for minimum cognitive load. Code should be obvious from its names, ownership, and data
flow, without cleverness or speculative abstractions.

These conventions govern new TypeScript code and substantially changed TypeScript files. Legacy
JavaScript and JSX under `src/pages/`, `src/components/`, `src/utils/`, `src/hooks/`, and
`src/context/` use reduced lint rules while migration continues. Touching a legacy file does not
authorize converting or restructuring unrelated code. New architecture belongs under
`src/features/` or `src/shared/` according to ownership.

## Tooling & enforcement

- `frontend/tsconfig.json` defines TypeScript requirements.
- `frontend/eslint.config.js` and `.prettierrc.json` define enforceable style.
- Do not copy configuration into code or weaken it to silence a failure.

## Ownership

- The backend owns Iceberg interpretation, analysis, and graph normalization.
- API schemas validate data at network boundaries.
- TanStack Query owns server state.
- Routes and validated search parameters own shareable, refresh-survivable state.
- Components own feature-local presentation and interaction.
- Shared code owns a proven cross-feature responsibility, not hypothetical reuse.

## TypeScript

- New and substantially changed files use strict TypeScript.
- Never use `any`, `enum`, `@ts-ignore`, or type assertions other than `as const`.
- Use `unknown` and narrow it. Validate external input with Zod.
- Infer local types. Type exported functions, props, hook returns, and API boundaries explicitly.
- Use `interface` for object shapes and `type` for unions, intersections, and derived types.
- `null` means intentionally empty. `undefined` means absent.
- Keep snapshot IDs and potentially unsafe integers as strings unless exact numeric safety is proven.

## Functions & components

- Use arrow functions.
- Keep one primary component per file and match the filename to it.
- Extract a component or hook when it owns meaningful behavior or makes the primary flow easier to
  read, not merely because a block is long.
- TypeScript component files have a lint-enforced 200-line limit, excluding blank lines and comments.
- Pass intent-named callbacks such as `onSelect`, not state setters or dispatch functions.
- Avoid prop spreading outside low-level shared UI primitives.
- React Compiler owns memoization. Do not import `memo`, `useMemo`, or `useCallback`.

## State and effects

- Derive values during render instead of storing synchronized copies.
- Handle user actions in event handlers and server data through TanStack Query.
- Use local state for local interaction and URL state for shareable behavior.
- Use `useEffect` only to synchronize with a non-React system.

## Data and errors

- Fetch through `frontend/src/shared/lib/api.ts`; do not scatter raw `fetch` calls.
- Parse runtime boundaries once, then use trusted internal types.
- Keep query keys and query functions with their feature API code.
- Preserve backend errors and warnings rather than inventing conflicting frontend interpretations.
- Handle failures at the nearest useful query or error boundary. Avoid scattered catch-and-ignore
  behavior.

## Routing

- Keep route files thin and feature behavior outside them.
- Use typed TanStack Router navigation, not constructed URL strings.
- Validate search parameters with Zod and preserve their string representation.
- Remove view-specific parameters when navigating to a route that does not own them.

## Naming and comments

- Use complete domain names. Avoid abbreviations except common forms such as `id`, `URL`, `API`,
  `props`, `ref`, `min`, and `max`.
- Prefix booleans with `is`, `has`, `should`, or `can`.
- Use `handleX` for internal handlers and `onX` for callback props.
- Comments explain a non-obvious reason, constraint, or external workaround. Prefer clearer code over
  comments that narrate behavior.
- Suppressions must remain narrowly scoped and satisfy the justification enforced by ESLint.
  `@ts-expect-error` requires a URL or issue number.

## Styling and structure

- Use existing Tailwind and shared UI patterns before introducing a new visual system.
- Use `cn()` for conditional classes.
- Keep feature-specific code in its feature. Move code to `shared/` only after it has a stable
  cross-feature responsibility.
- Avoid barrel files and deep feature nesting that hide ownership.
- Do not add a dependency to replace a small, clear implementation.
