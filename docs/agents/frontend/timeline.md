# Timeline

Rules for `frontend/src/features/timeline/`. Read [`development.md`](development.md) and
[`philosophy.md`](philosophy.md) first; these rules add to them. Paths are relative to the feature
directory.

## The flow

Data moves one way, through pure steps:

1. **Parse** (`lib/parseTimelineNodes.ts`): raw nodes become readable metadata files, unreadable
   but placeable files, and snapshots.
2. **Sequence** (`lib/sequenceTimelineFiles.ts`): orders all files by the `previous_file` chain
   (`lib/orderMetadataFiles.ts`; timestamps only order files the chain cannot connect), pairs each
   readable file with what comes right before it (a readable file, an unreadable one, or
   nothing), and turns each run of unreadable files into one gap.
3. **Rows** (`lib/buildTimeline.ts`): each sequenced item becomes a row. A file with a readable
   predecessor is described by `describeCommit` (`lib/classification/`, `lib/definitionChanges/`).
   The oldest file, a file after a gap, and a gap are turned into rows by `buildTimeline.ts`
   itself, because none of them compares two files.

Components render the rows and may call pure `lib/` functions. React state, data fetching, and
keyboard handling live in `hooks/`.

## Rules

1. `buildTimeline.ts` calls parse and sequence, then maps items to rows. Comparing two files
   belongs in `lib/classification/`, ordering and gaps in the sequence step. Do not add either to
   `buildTimeline.ts`.
2. A row is described only by comparing a file with its immediate readable predecessor. Never
   compare two files with anything unreadable or missing between them.
3. A row never claims a change it cannot prove. When the predecessor is unknown, the row is an
   `unknown-change` row: no title that names a change, no chips, no snapshot id, no branch, no
   diff, and neither a write nor a metadata marker or filter. Its details may show the file's own
   state.
4. Nothing is read across a gap: not a comparison, not a draft's publication, not ref badges.
5. A gap is a row of its own (`GapRow`). Never record a gap as fields on a neighbouring row.
6. Only the sequence step decides where gaps are. The classifiers never see a gap: they always
   receive two readable files.
7. `TimelineRow` is a union. Narrow on `kind` before reading commit fields. Do not add a field to
   one member that only has meaning for another.
8. Whatever picks a row's title sets its `category` in the same place. Components choose colors,
   icons, and filters from `kind` and `category`, never by matching `title` text.
9. `lib/` and `api/` import no React, router, DOM, or component code. `lib/` functions are pure.
10. Every zod schema lives in `api/`. `lib/` receives parsed, trusted types.
11. Large Iceberg integers stay strings or `BigInt`. Never pass a snapshot id or a count through
    `Number()`.
12. User-facing wording that appears in more than one place has one function or constant in
    `lib/format/`.

## Before finishing

- With an unreadable metadata node at the newest end, in the middle, and at the oldest end, the
  list must show a gap row in each place and `Change unknown` for the event after it.
- Update `frontend/src/features/docs/content/timeline-view.mdx` when list or details behavior
  changes.
