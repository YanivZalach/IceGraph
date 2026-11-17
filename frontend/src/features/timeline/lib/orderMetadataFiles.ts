export interface ChainedFile {
  file_path: string;
  previous_file: string | null;
  timestamp: number;
}

const byTimestampThenPath = (fileA: ChainedFile, fileB: ChainedFile): number =>
  fileA.timestamp - fileB.timestamp ||
  fileA.file_path.localeCompare(fileB.file_path);

/**
 * Each metadata file names the file it replaced, so that chain decides the order. Timestamps only
 * order files the chain cannot connect: separate segments around a gap, or two writers in the same
 * millisecond whose chain is broken.
 */
export const orderMetadataFilesOldestFirst = <File extends ChainedFile>(
  files: File[],
): File[] => {
  const knownPaths = new Set(files.map((file) => file.file_path));
  const successorsByPath = new Map<string, File[]>();
  const ready: File[] = [];

  for (const file of files) {
    const previousPath = file.previous_file;
    if (
      previousPath === null ||
      previousPath === file.file_path ||
      !knownPaths.has(previousPath)
    ) {
      ready.push(file);
      continue;
    }
    const successors = successorsByPath.get(previousPath) ?? [];
    successors.push(file);
    successorsByPath.set(previousPath, successors);
  }

  const ordered: File[] = [];
  const placedPaths = new Set<string>();
  while (ready.length > 0) {
    ready.sort(byTimestampThenPath);
    const next = ready.shift();
    if (next === undefined || placedPaths.has(next.file_path)) {
      continue;
    }
    ordered.push(next);
    placedPaths.add(next.file_path);
    ready.push(...(successorsByPath.get(next.file_path) ?? []));
  }

  const unplaced = files
    .filter((file) => !placedPaths.has(file.file_path))
    .sort(byTimestampThenPath);

  return [...ordered, ...unplaced];
};
