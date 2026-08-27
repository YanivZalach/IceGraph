const TimelineViewSection = () => (
  <div className="space-y-4">
    <p>
      A chronological list of every snapshot in your selected range. Each row
      shows when the snapshot was created, what operation produced it, and how
      many files and records changed.
    </p>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">Operation types</h3>
      <ul className="list-disc list-inside space-y-2">
        <li>
          <strong className="text-white">append</strong> - new data was added to
          the table
        </li>
        <li>
          <strong className="text-white">overwrite</strong> - new data was
          written, and any existing data in the affected partitions was replaced
        </li>
        <li>
          <strong className="text-white">replace</strong> - files were rewritten
          without changing the actual records (compaction, rewriting manifests,
          etc)
        </li>
        <li>
          <strong className="text-white">delete</strong> - rows or files were
          removed from the table
        </li>
      </ul>
    </div>
    <p>
      Use the Timeline to pinpoint when a large write happened, spot unexpected
      deletes, or verify that a compaction job ran as expected.
    </p>
    <p>
      A red <strong className="text-white">Unknown Events</strong> marker
      appears when metadata or snapshot data could not be read. Snapshots that
      simply fall outside the selected range are not flagged. It indicates that
      one or more events occurred in that part of the timeline, even when the
      exact events cannot be determined. The next readable event is compared
      with the previous readable metadata. Its details show the metadata
      changes, and when the snapshot changed they also show that snapshot's
      operation.
    </p>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">Zoom &amp; pan</h3>
      <p>
        Scroll the mouse wheel to zoom in and out (text and nodes scale
        together, like Graph view). Drag the timeline background to pan. Use
        horizontal trackpad scroll or Shift + wheel to pan sideways without
        zooming. <strong className="text-white">Fit Timeline</strong> scales the
        full history to the viewport and centers it.
      </p>
    </div>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">Details panel</h3>
      <p>
        Click a timeline event to open its details in a panel on the right - the
        same panel used in Graph view. Drag the left-edge grip to widen it, use
        fullscreen to expand, and copy field values with the clipboard icon.
        Long JSON diffs are collapsed by default and can be expanded or
        collapsed with <strong className="text-white">▼</strong> /{" "}
        <strong className="text-white">▲</strong>.
      </p>
      <p>
        When the event's snapshot points at the job that wrote it, an{" "}
        <strong className="text-white">
          <code>action_link</code>
        </strong>{" "}
        row links out to it, opening in a new tab.
      </p>
    </div>
  </div>
);

export default TimelineViewSection;
