const IssuesPanelSection = () => (
  <div className="space-y-4">
    <p>
      When the backend reports problems during metadata collection, an{" "}
      <strong className="text-white">Issues</strong> button appears in the
      navbar. There are two severity levels:
    </p>
    <div className="space-y-3">
      <div className="space-y-1">
        <h3 className="text-red-400 font-semibold">Critical Errors</h3>
        <p>
          Something failed while reading the table's metadata - for example, a
          file could not be accessed or the backend encountered an unexpected
          state. The graph may be incomplete or missing sections entirely.
        </p>
      </div>
      <div className="space-y-1">
        <h3 className="text-amber-400 font-semibold">Warnings</h3>
        <p>
          A collection limit was reached, so the graph represents a partial view
          of the table. When the data file limit is hit, the backend stops
          attaching data files, but the snapshot and manifest structure is still
          complete. When the metadata file limit is hit, only the oldest
          metadata files are dropped, so every metadata file you see is complete
          and accurate.
        </p>
      </div>
    </div>
    <p>
      The manifest limit behaves differently: if your snapshot range contains
      more manifests than the backend allows, the request fails instead of
      returning a partial graph, and nothing is rendered. Select a smaller
      snapshot range.
    </p>
    <p>
      The panel opens automatically when the backend reports any issue. Even so,
      always check it when the graph looks incomplete or the data file count
      seems lower than expected.
    </p>
  </div>
);

export default IssuesPanelSection;
