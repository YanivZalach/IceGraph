import type { MetadataFileNode } from "../api/nodeSchemas";
import {
  orderMetadataFilesOldestFirst,
  type ChainedFile,
} from "./orderMetadataFiles";
import type { UnreadableFile } from "./parseTimelineNodes";

/** What comes right before a readable file, which decides what may be said about its change. */
export type Predecessor =
  | { kind: "file"; file: MetadataFileNode }
  | { kind: "unreadable" }
  | { kind: "none" };

/** A run of unreadable files, newest first, so it always has a newest one to stand for it. */
export type UnreadableRun = [UnreadableFile, ...UnreadableFile[]];

export type SequenceItem =
  | { kind: "commit"; file: MetadataFileNode; predecessor: Predecessor }
  | { kind: "gap"; files: UnreadableRun };

interface Placement extends ChainedFile {
  source:
    | { kind: "readable"; file: MetadataFileNode }
    | { kind: "unreadable"; file: UnreadableFile };
}

const placementOf = (
  file: ChainedFile,
  source: Placement["source"],
): Placement => ({
  file_path: file.file_path,
  previous_file: file.previous_file,
  timestamp: file.timestamp,
  source,
});

const predecessorOf = (
  file: MetadataFileNode,
  previousItem: SequenceItem | undefined,
): Predecessor => {
  if (previousItem === undefined) {
    return { kind: "none" };
  }
  if (previousItem.kind === "gap") {
    return { kind: "unreadable" };
  }
  // The file it replaced is neither readable nor placeable, so it is not the
  // one that happens to sit before it.
  const replacesAnotherFile =
    file.previous_file !== null &&
    file.previous_file !== previousItem.file.file_path;
  return replacesAnotherFile
    ? { kind: "unreadable" }
    : { kind: "file", file: previousItem.file };
};

/**
 * Oldest first. The only place that knows about gaps: every readable file leaves here already
 * knowing whether its change can be worked out, and every run of unreadable files is one item.
 */
export const sequenceTimelineFiles = (
  readableFiles: MetadataFileNode[],
  unreadableFiles: UnreadableFile[],
): SequenceItem[] => {
  const placements = orderMetadataFilesOldestFirst([
    ...readableFiles.map((file) =>
      placementOf(file, { kind: "readable", file }),
    ),
    ...unreadableFiles.map((file) =>
      placementOf(file, { kind: "unreadable", file }),
    ),
  ]);

  const items: SequenceItem[] = [];
  for (const { source } of placements) {
    const previousItem = items.at(-1);
    if (source.kind === "readable") {
      items.push({
        kind: "commit",
        file: source.file,
        predecessor: predecessorOf(source.file, previousItem),
      });
    } else if (previousItem?.kind === "gap") {
      previousItem.files.unshift(source.file);
    } else {
      items.push({ kind: "gap", files: [source.file] });
    }
  }
  return items;
};
