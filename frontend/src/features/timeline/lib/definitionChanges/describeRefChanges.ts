import type { SnapshotRefs } from "../../api/nodeSchemas";
import { sameTextChange, type DescribedChange } from "./describedChange";
import { formatShortId } from "../format/formatShortId";

export const describeRefChanges = (
  previousRefs: SnapshotRefs,
  currentRefs: SnapshotRefs,
): DescribedChange[] => {
  const changes: DescribedChange[] = [];

  for (const [name, currentRef] of Object.entries(currentRefs)) {
    const previousRef = previousRefs[name];

    if (previousRef === undefined) {
      changes.push(sameTextChange(`${currentRef.type} ${name} created`));
    } else if (previousRef["snapshot-id"] !== currentRef["snapshot-id"]) {
      changes.push(
        sameTextChange(
          `${currentRef.type} ${name} moved to snapshot ${formatShortId(currentRef["snapshot-id"])}`,
        ),
      );
    }
  }

  for (const [name, previousRef] of Object.entries(previousRefs)) {
    if (currentRefs[name] === undefined) {
      changes.push(sameTextChange(`${previousRef.type} ${name} deleted`));
    }
  }

  return changes;
};
