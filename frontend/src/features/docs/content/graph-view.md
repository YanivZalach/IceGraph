The Graph view shows all Iceberg metadata objects in your selected range as a directed acyclic graph. Each node is a file. Links show parent→child relationships.

### Node types

-

**Metadata file** - JSON file describing the current full state of the table, combining schema, partition spec, snapshot history, and data file references

-

**Snapshot** - Avro file that represents a point-in-time version of the table produced by a data operation (append, overwrite, etc.)

-

**Manifest** - tracks which data files exist and stores per-file statistics

-

**Data file** - the actual Parquet, ORC, or Avro file containing your rows. IceGraph is not reading the data file: all of the data shown comes from the manifest entries that point at it. When available, the details panel also shows human-readable per-column metrics derived from the manifest entry, including the inferred metadata size that is not represented by reported column sizes

-

**Unreadable file** - a file whose metadata could not be obtained is drawn in red and shows the reason in its details panel. The rest of the graph still loads

### Interactions

-

Click a node to select it and open its details in the side panel

- Drag a node to reposition it
- Scroll to zoom, drag the background to pan

### Details panel

The panel on the right lists every metadata field for the selected node. The header shows the file type, path, and timestamp; fields below use the same layout as Timeline and other views.

-

**Resize** - drag the grip handle on the left edge of the panel to widen it. Wider panels give text fields more room and show more lines before you need to expand a field.

-

**Fullscreen** - click the expand button in the panel header to fill the graph area. Click the compress button or press **Esc** to exit.

-

**Copy** - click the clipboard icon inside any field to copy its value.

-

**Long values** - fields with many lines can be expanded or collapsed individually with **▼** / **▲**.

### Reading the graph

Nodes shared across multiple snapshots mean Iceberg reused those files, data that didn't change is never rewritten. Seeing many shared data files between snapshots is normal and efficient. A snapshot with no shared manifests or data files means a full overwrite occurred.
