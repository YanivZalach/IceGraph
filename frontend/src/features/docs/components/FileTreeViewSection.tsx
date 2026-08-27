const FileTreeViewSection = () => (
  <div className="space-y-4">
    <p>
      Shows all data files in your selected snapshot range organized as a
      directory tree, grouped by their partition paths.
    </p>
    <p>
      This view solves a common source of confusion: if you look at the{" "}
      <strong className="text-white">raw storage directory</strong> written by
      your engine (Spark, for example), you see all files ever written —
      including files from old snapshots that have since been replaced, and
      files that belong to different table versions.
    </p>
    <p>
      <strong className="text-white">
        What's on disk is not the same as what Iceberg considers the current
        table.
      </strong>{" "}
      The FileTree view shows only the files Iceberg actually tracks as part of
      the selected snapshots, giving you a true picture of the table's data.
    </p>
    <ul className="list-disc list-inside space-y-1">
      <li>Expand directories to see individual files</li>
      <li>Focus a file or directory and press Enter or Space to inspect it</li>
      <li>
        Directories start collapsed, and large trees render only the rows
        visible in the viewport so expanding many files remains responsive
      </li>
      <li>Choose a branch, and within it, a snapshot to explore</li>
      <li>
        Switch between every file in a snapshot and only files added by the
        selected commit
      </li>
      <li>
        Branch, selected snapshot, file scope, grouping, and search are stored
        in the URL so the current FileTree view can be refreshed or shared
      </li>
      <li>
        Select a file, partition, or partition-path step to inspect file counts,
        sizes, rows, and readable per-column metrics, including null and NaN
        percentages, average bytes per value, column-size share, and inferred
        metadata size. Click a metric-table heading to sort by its original
        value. Click it again to reverse the order, then once more to restore
        schema order
      </li>
      <li>
        If the selected snapshot or one of its included files could not be read,
        an error notice identifies the file and explains why
      </li>
      <li>
        If graph data references a missing node, a warning explains that some
        files may be absent instead of silently showing an incomplete result
      </li>
      <li>
        If a readable snapshot has unreadable snapshots in its loaded history, a
        warning identifies them. Added in commit compares with the nearest
        readable parent and may therefore span multiple commits
      </li>
      <li>
        Many small files in one partition path often indicates a small-file
        problem
      </li>
      <li>
        Each file shows its{" "}
        <strong className="text-white">first appearing timestamp</strong>{" "}
        tracked by Iceberg in the{" "}
        <strong className="text-white">asked snapshot range</strong>
      </li>
      <li>
        Each folder shows a{" "}
        <strong className="text-white">last modified</strong> timestamp - the
        most recent first-appearing timestamp among all its files
      </li>
    </ul>
  </div>
);

export default FileTreeViewSection;
