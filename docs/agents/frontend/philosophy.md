# Frontend Philosophy

React + TypeScript. Guiding principle: **minimum cognitive load**, understandable at a glance, with no cleverness, unnecessary indirection, or premature abstraction. The implementation workflow lives in [development.md](development.md); this file defines technical conventions.

## Platform

- **Vite SPA, client-only.** No SSR, no server-component patterns.
- **React: latest stable, upgraded cautiously.** React Compiler enabled.
- **Browser floor: Chrome 110+.** Use anything it supports freely; no polyfills for older.

## Dependencies

- **Minimal.** Every new dependency must be justified; the default answer is no. Small utilities are written in `shared/lib`, not installed.
- Approved stack: React, TanStack Router, TanStack Query, Jotai, react-hook-form, Zod, Tailwind, date-fns, shadcn/ui and its satellites (Radix packages, `lucide-react`, `clsx`, `tailwind-merge`, `class-variance-authority`).
- **Dates:** date-fns for math; native `Intl.DateTimeFormat` / `Intl.RelativeTimeFormat` for display. Not dayjs, not moment.

## Tooling & enforcement

- **ESLint + Prettier** in CI — failing lint fails the build. The linter is this document made executable; if they disagree, fix one — never silently disable.
- `eslint-disable` and `@ts-expect-error` require a linked issue. `@ts-ignore` is banned.
- **Tried and tested beats bespoke.** Tooling config (tsconfig, ESLint, Prettier, CI workflows) starts from the official or de-facto community template (Vite `react-ts` template, `typescript-eslint` presets) and deviates only where this document demands it. When this document and an established community default conflict for no articulated reason, prefer the community default and amend this document.

## tsconfig

Maximum strictness. Beyond `strict: true`:

```jsonc
{
  "noUncheckedIndexedAccess": true,
  "exactOptionalPropertyTypes": true,
  "noImplicitReturns": true,
  "noFallthroughCasesInSwitch": true,
  "noUnusedLocals": true,
  "noUnusedParameters": true,
  "verbatimModuleSyntax": true,
}
```

Never weaken these to silence an error — fix the code.

## TypeScript (non-negotiable)

- **Never `any`.** Use `unknown` and narrow.
- **Never `enum`.** Union types (`type Status = "draft" | "sent" | "paid"`), or `as const` objects when a value lookup is needed.
- **Assertions banned except `as const`.** If a type doesn't flow naturally, fix the data flow or parse with Zod. Rare exceptions exist (e.g., a narrowing the compiler genuinely can't follow) — each needs a why-comment with a linked issue, same as `@ts-expect-error`. `as unknown as X` is never acceptable.
- **`null` means intentionally empty; `undefined` means absent.** Never interchangeable.
- `interface` for object shapes; `type` for unions, intersections, derived types.
- **Infer locally, explicit at boundaries** — exported functions, props, hook returns, API layers.
- **Zod at runtime boundaries, plain types inside.** Never hand-write a type for data that has a schema (`z.infer` it); never write a schema for data that's never validated. Schemas: API responses, forms, env, URL params. Plain types: props, internal state, function signatures.

## Functions & components

- Arrow functions everywhere. No `function` declarations.
- One component per file. **Default export for the component**, matching the filename; named exports for everything else.
- Split into small functions, but **don't over-generalize** — no generics or options objects for hypothetical future needs.
- Extract a custom hook when component logic gets hard to scan — not before.
- **Components max 200 lines** (lint-enforced). Over the limit → extract sub-components or a hook.
- Express variation however is simplest per case: composition, variant unions, or a boolean prop.
- **No hand-memoization.** React Compiler handles it — never `memo`, `useMemo`, or `useCallback`. No speculative optimization of any kind.

## Props & data flow

- **Never pass a state setter or dispatch as a prop.** Pass intent-named callbacks (`onSelect`, `onClose`). If you're tempted to drill a setter, that state wants to be a Jotai atom.
- **Prop spreading (`{...props}`) only in `shared/ui/` primitives** forwarding to a DOM element or Radix part. Feature components list every prop explicitly.
- **Prop drilling: two levels max**, then composition (pass children) or an atom.

## useEffect

- **Effectively banned.** Deriving data → compute during render. User actions → event handlers. Fetching → TanStack Query. Reacting to state → derived Jotai atoms.
- Legitimate only for syncing with a non-React system (DOM APIs, subscriptions, third-party widgets) — and justified in the PR.

## Comments

- **No what-comments.** Code self-documents through naming and structure; if it needs explaining, rewrite it.
- The exception: **why-comments carrying a URL or issue number** (library-bug workarounds, mandated business rules). A why-comment without a link is banned. This is where `eslint-disable` / `@ts-expect-error` links live.

## Naming

Names are the documentation. **Never shorten anything** — a long precise name beats a short vague one.

Casing:

- `camelCase` — variables, functions, hooks, non-component files (`useSelectedInvoice.ts`)
- `PascalCase` — components, component files (`InvoiceSummaryCard.tsx`), interfaces, types
- `SCREAMING_SNAKE_CASE` — true module-level constants only (`MAX_UPLOAD_SIZE_BYTES`)
- Acronyms follow camel casing: `userId`, `apiUrl` — not `userID`, `APIURL`

Mandatory prefixes:

- Booleans: `is` / `has` / `should` / `can` — `isSubmitting`, `hasUnsavedChanges`
- Handlers inside components: `handleX`; callback props: `onX`
- Hooks: `useX`; Jotai atoms: `xAtom`; Zod schemas: `xSchema` (camelCase — they're values), derived type `Invoice`

Allowed abbreviations — only `id`, `URL`, `API`, `props`, `ref`, `min`/`max`. Never `usr`, `btn`, `idx`, `err`, `res`, `req`, `val`, `tmp`, `arr`, `num`, `e`.

| Bad                              | Good                                                              |
| -------------------------------- | ----------------------------------------------------------------- |
| `const d = getDiff(a, b)`        | `const daysUntilDueDate = getDaysBetween(today, invoice.dueDate)` |
| `const fltrd = invs.filter(...)` | `const overdueInvoices = invoices.filter(...)`                    |
| `const flag = user.subs > 0`     | `const hasActiveSubscription = user.activeSubscriptionCount > 0`  |
| `onClick={(e) => del(e, i)}`     | `onClick={() => handleInvoiceDelete(invoice.id)}`                 |
| `calcTot(items)`                 | `calculateOrderTotal(lineItems)`                                  |
| `InvCard.tsx`                    | `InvoiceSummaryCard.tsx`                                          |

The test: could a new maintainer read the name alone and know exactly what it holds or does? If not, rename it.

## State & data

- **Jotai** for client state — small, focused atoms; derive with derived atoms instead of syncing manually.
- **TanStack Query** for all server state. Server data never lives in atoms.
- Query keys are typed and centralized per feature.

## Ownership boundaries

- The backend owns Iceberg interpretation, analysis, and graph normalization. The frontend presents
  normalized results and must not independently recreate domain rules.
- API schemas own runtime validation at the network boundary.
- Routes and validated search parameters own shareable and refresh-survivable URL state.
- TanStack Query owns server state. Jotai owns client-only state.
- Components own presentation and interaction local to their feature.
- Shared abstractions follow a real ownership boundary or repeated responsibility. Similar-looking
  code alone is not enough reason to create one.

## Routing

- **TanStack Router**, file-based in `src/routes/` — route files stay thin and compose feature components.
- Typed navigation only: `<Link to="...">` and typed `navigate` — never string-built paths.
- **Search params validated with Zod** via `validateSearch` — URL state is external input; it gets a schema.
- Prefer URL state over Jotai for anything shareable or refresh-survivable: filters, tabs, pagination.

## Environment variables

- One Zod-validated module — `shared/lib/env.ts` parses `import.meta.env` at startup and exports typed `env`. **Crash immediately on invalid or missing env.**
- Never read `import.meta.env` anywhere else.

## Loading & Suspense

- **`useSuspenseQuery` by default** — data guaranteed below the boundary; no `isPending` checks, no `data?.` chaining.
- Suspense boundaries at route or feature level, paired with error boundaries.
- Plain `useQuery` only when a section must render before its data — justify it.

## Imports

- **Relative imports, no path aliases.** Zero config, every tool understands them, and editors rewrite them on file moves. Reaching `shared/` from inside a feature is ~3 levels — that's normal. The smell is depth _inside_ a feature (`../../../components/...` within the same feature) — that means the feature's internal nesting is too deep; flatten it.

## Data fetching & errors

- **The app is currently read-only** — no mutations. Write conventions get decided when the first mutation arrives; don't introduce one without raising it.
- **One typed API client in `shared/lib/api.ts`** — owns base URL (from `env`), auth headers, and Zod parsing; every call takes a schema, returns typed data. Never raw `fetch`.
- All fetching through TanStack Query. No `fetch` in `useEffect`.
- **Error boundaries + TanStack error states** (`isError`, `error`, `throwOnError`). No scattered try/catch in components.

## Forms

- **Every field is a shadcn/ui form component** (`Form`, `FormField`, `FormItem`...) — no raw `<input>`, no `register()` spread.
- **Always controlled**, via `Controller` (which `FormField` wraps) — one pattern for every field.
- **Validated with Zod** through `zodResolver` — the schema owns shape, validation, and error messages.
- Form values that drive queries (search, filters) live in URL search params, not component state. (Write-form submissions will use TanStack mutations when they arrive.)

## Styling

- **Tailwind only.** No CSS files, no styled-components, no inline `style`.
- Repeated class combinations become a component, not a string constant.
- **Conditional classes through `cn()`** (`shared/lib/cn.ts`) — never template-literal building.
- **Arbitrary values:** banned for design tokens — colors, fonts, shadows, radii come from the theme. Allowed for one-off layout dimensions (`w-[347px]`).

## UI primitives

- **shadcn/ui** for interaction-heavy widgets (dialogs, dropdowns, tooltips) — copied into `shared/ui/`, owned like our own code.
- **Copied components are conformed to this document on arrival:** default export, arrow functions, house naming. No vendored-style exemptions. Conform surface style only — don't restructure the Radix wiring.
- Don't hand-roll focus traps, positioning, or keyboard handling — that's the Radix layer's job.
- Simple elements (buttons, cards, inputs) are plain JSX + Tailwind.

## Structure

```
src/
  routes/        TanStack Router file-based routes (thin, compose features)
  shared/
    ui/          shadcn/ui components (copied in, owned by us)
    lib/
      env.ts     Zod-validated env, the only import.meta.env access
      api.ts     the one typed fetch wrapper (base URL, auth, Zod parse)
      cn.ts      clsx + tailwind-merge helper for conditional classes
  features/
    <feature>/
      components/
      hooks/
      api/       query functions, query keys, zod schemas
      atoms.ts
```

- Used by one feature → lives in that feature. Used by two or more → `shared/`.
- Props interfaces live in the same file as their component.
- No barrel files beyond what's necessary — they add indirection.

## Reference example

```tsx
import { useAtomValue } from "jotai";
import { cn } from "../../../shared/lib/cn";
import { selectedUserIdAtom } from "../atoms";
import type { User } from "../api/schemas";

interface UserCardProps {
  user: User;
  onSelect: (id: string) => void;
}

const UserCard = ({ user, onSelect }: UserCardProps) => {
  const selectedUserId = useAtomValue(selectedUserIdAtom);
  const isSelected = selectedUserId === user.id;

  return (
    <button
      onClick={() => onSelect(user.id)}
      className={cn(
        "flex w-full items-center gap-3 rounded-lg p-3 text-left",
        isSelected ? "bg-blue-50" : "hover:bg-gray-50",
      )}
    >
      <span className="font-medium">{user.name}</span>
      <span className="text-sm text-gray-500">{user.email}</span>
    </button>
  );
};

export default UserCard;
```

Demonstrates: arrow function, one component per file, default export matching filename, props interface beside the component, explicit boundary types with local inference, `User` from a Zod schema, `isSelected` derived during render (no effect, no stored state), `cn()` conditionals, zero comments, Tailwind only.

## Tech stack summary

| Concern       | Choice                                                     |
| ------------- | ---------------------------------------------------------- |
| Build         | Vite (SPA, client-only)                                    |
| Language      | TypeScript, maximum strictness                             |
| UI            | React latest stable + React Compiler                       |
| Routing       | TanStack Router (file-based, typed, Zod search params)     |
| Server state  | TanStack Query (`useSuspenseQuery` default)                |
| Client state  | Jotai                                                      |
| Forms         | react-hook-form + shadcn fields, controlled, `zodResolver` |
| Validation    | Zod (API, forms, env, URL params)                          |
| Styling       | Tailwind (+ `cn()` for conditionals)                       |
| UI primitives | shadcn/ui (Radix, lucide-react, cva)                       |
| Dates         | date-fns for math, `Intl` for display                      |
| Lint/format   | ESLint + Prettier, CI-enforced                             |
| Browser floor | Chrome 110+                                                |
