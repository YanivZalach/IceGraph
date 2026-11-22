# Timeline

Rules for `frontend/src/features/timeline/`. They add to [`development.md`](development.md) and
[`philosophy.md`](philosophy.md). Paths are relative to the feature directory.

## Flow

1. **Parse** (`lib/parseTimelineNodes.ts`): nodes become readable files, unreadable files, and
   snapshots.
2. **Sequence** (`lib/sequenceTimelineFiles.ts`): orders files, groups unreadable runs into gaps,
   and pairs each readable file with what comes right before it.
3. **Rows** (`lib/buildTimeline.ts`): each item becomes a row. `lib/classification/` describes a
   file against its readable predecessor.
4. **Lanes** (`lib/buildBranchLanes.ts`): the branch graph drawn in the rail. A lane's life comes
   from each file's own branch refs; fork and merge edges only between readable neighbors.

## Rules

1. One job per step. Ordering and gaps belong in sequence, comparing two files in
   `lib/classification/`. `buildTimeline.ts` only maps items to rows.
2. Compare a file only with the readable file right before it. Nothing is read across a gap: no
   comparison, no draft publication, no ref badges.
3. Show every metadata file as a row, but never claim a change you cannot prove. With no readable
   predecessor the row is `unknown-change` and shows only the file's own state. The oldest file of
   a truncated range is the exception: a commit that adds a snapshot stamps the file with that
   snapshot's commit time, so a snapshot with the file's exact timestamp proves the write.
4. Each row type carries only its own fields. A gap is its own member of `TimelineRow`, never
   fields on a commit row.
5. Components never decide meaning. Colors, icons, and filters come from `kind` and `category`,
   never from `title` text. Anything worked out from the data is worked out in `lib/`.
6. `lib/` is pure: no React, router, or DOM imports.

## Check

Make a metadata node unreadable at the newest end, the middle, and the oldest end. Each must show
a gap row, with `Change unknown` on the event after it.
