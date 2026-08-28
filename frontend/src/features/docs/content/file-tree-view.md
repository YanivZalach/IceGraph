Shows all data files in your selected snapshot range organized as a directory tree, grouped by their partition paths.

This view solves a common source of confusion: if you look at the **raw storage directory** written by your engine (Spark, for example), you see all files ever written, including files from old snapshots that have since been replaced, and files that belong to different table versions.

**What's on disk is not the same as what Iceberg considers the current table.** The FileTree view shows only the files Iceberg actually tracks as part of the selected snapshots, giving you a true picture of the table's data.

- Expand directories to see individual files
- Focus a file or directory and press Enter or Space to inspect it
- Directories start collapsed, and large trees render only the rows visible in the viewport so expanding many files remains responsive

- Choose a branch, and within it, a snapshot to explore
- Switch between every file in a snapshot and only files added by the selected commit

- Branch, selected snapshot, file scope, grouping, and search are stored in the URL so the current FileTree view can be refreshed or shared

- Select a file, partition, or partition-path step to inspect file counts, sizes, rows, and readable per-column metrics, including null and NaN percentages, average bytes per value, column-size share, and inferred metadata size. Click a metric-table heading to sort by its original value. Click it again to reverse the order, then once more to restore schema order

- If the selected snapshot or one of its included files could not be read, an error notice identifies the file and explains why

- If graph data references a missing node, a warning explains that some files may be absent instead of silently showing an incomplete result

- If a readable snapshot has unreadable snapshots in its loaded history, a warning identifies them. Added in commit compares with the nearest readable parent and may therefore span multiple commits

- Many small files in one partition path often indicates a small-file problem

- Each file shows its **first appearing timestamp** tracked by Iceberg in the **asked snapshot range**

- Each folder shows a **last modified** timestamp - the most recent first-appearing timestamp among all its files
