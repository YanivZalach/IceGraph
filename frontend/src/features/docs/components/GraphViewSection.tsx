const GraphViewSection = () => (
  <div className="space-y-5">
    <p>
      The Graph view shows all Iceberg metadata objects in your selected range
      as a directed acyclic graph. Each node is a file. Links show parent→child
      relationships.
    </p>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">Node types</h3>
      <ul className="list-disc list-inside space-y-2">
        <li>
          <strong className="text-white">Metadata file</strong> - JSON file
          describing the current full state of the table, combining schema,
          partition spec, snapshot history, and data file references
        </li>
        <li>
          <strong className="text-white">Snapshot</strong> - Avro file that
          represents a point-in-time version of the table produced by a data
          operation (append, overwrite, etc.)
        </li>
        <li>
          <strong className="text-white">Manifest</strong> - tracks which data
          files exist and stores per-file statistics
        </li>
        <li>
          <strong className="text-white">Data file</strong> - the actual
          Parquet, ORC, or Avro file containing your rows. IceGraph is not
          reading the data file: all of the data shown comes from the manifest
          entries that point at it. When available, the details panel also shows
          human-readable per-column metrics derived from the manifest entry,
          including the inferred metadata size that is not represented by
          reported column sizes
        </li>
        <li>
          <strong className="text-white">Unreadable file</strong> - a file whose
          metadata could not be obtained is drawn in red and shows the reason in
          its details panel. The rest of the graph still loads
        </li>
      </ul>
    </div>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">Interactions</h3>
      <ul className="list-disc list-inside space-y-1">
        <li>
          Click a node to select it and open its details in the side panel
        </li>
        <li>Drag a node to reposition it</li>
        <li>Scroll to zoom, drag the background to pan</li>
      </ul>
    </div>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">Details panel</h3>
      <p>
        The panel on the right lists every metadata field for the selected node.
        The header shows the file type, path, and timestamp; fields below use
        the same layout as Timeline and other views.
      </p>
      <ul className="list-disc list-inside space-y-1">
        <li>
          <strong className="text-white">Resize</strong> - drag the grip handle
          on the left edge of the panel to widen it. Wider panels give text
          fields more room and show more lines before you need to expand a
          field.
        </li>
        <li>
          <strong className="text-white">Fullscreen</strong> - click the expand
          button in the panel header to fill the graph area. Click the compress
          button or press <strong className="text-white">Esc</strong> to exit.
        </li>
        <li>
          <strong className="text-white">Copy</strong> - click the clipboard
          icon inside any field to copy its value.
        </li>
        <li>
          <strong className="text-white">Long values</strong> - fields with many
          lines can be expanded or collapsed individually with{" "}
          <strong className="text-white">▼</strong> /{" "}
          <strong className="text-white">▲</strong>.
        </li>
      </ul>
    </div>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">Reading the graph</h3>
      <p>
        Nodes shared across multiple snapshots mean Iceberg reused those files —
        data that didn't change is never rewritten. Seeing many shared data
        files between snapshots is normal and efficient. A snapshot with no
        shared manifests or data files means a full overwrite occurred.
      </p>
    </div>
  </div>
);

export default GraphViewSection;
