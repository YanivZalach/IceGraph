import type { SnapshotReference } from "../../api/snapshotReferenceSchema";
import type { SnapshotRefs } from "../../api/nodeSchemas";
import { sameTextChange, type DescribedChange } from "./describedChange";
import { formatDurationMs } from "../format/formatDuration";

const refChange = (
  refType: "branch" | "tag",
  name: string,
  action: string,
): DescribedChange => ({
  ...sameTextChange(`${refType} ${name} ${action}`),
  impactSegment: { kind: "ref", refType, name, action },
});

const RETENTION_FIELDS = [
  { key: "max-snapshot-age-ms", label: "max snapshot age", isDuration: true },
  {
    key: "min-snapshots-to-keep",
    label: "min snapshots to keep",
    isDuration: false,
  },
  { key: "max-ref-age-ms", label: "max ref age", isDuration: true },
] as const;

const formatRetentionValue = (
  value: number | string | null | undefined,
  isDuration: boolean,
): string => {
  if (value === null || value === undefined) {
    return "default";
  }
  return isDuration ? formatDurationMs(value) : String(value);
};

const describeRetentionChange = (
  name: string,
  previousRef: SnapshotReference,
  currentRef: SnapshotReference,
): DescribedChange | null => {
  const changedFields = RETENTION_FIELDS.filter(
    ({ key }) =>
      String(previousRef[key] ?? "") !== String(currentRef[key] ?? ""),
  );
  if (changedFields.length === 0) {
    return null;
  }

  const detailParts = changedFields.map(
    ({ key, label, isDuration }) =>
      `${label} ${formatRetentionValue(previousRef[key], isDuration)} → ${formatRetentionValue(currentRef[key], isDuration)}`,
  );

  return {
    ...refChange(currentRef.type, name, "retention changed"),
    detail: `${currentRef.type} ${name} retention: ${detailParts.join(", ")}`,
  };
};

export const describeRefChanges = (
  previousRefs: SnapshotRefs,
  currentRefs: SnapshotRefs,
  rowSnapshotId: string | null,
): DescribedChange[] => {
  const changes: DescribedChange[] = [];

  for (const [name, currentRef] of Object.entries(currentRefs)) {
    const previousRef = previousRefs[name];

    if (previousRef === undefined) {
      changes.push(refChange(currentRef.type, name, "created"));
      continue;
    }

    const hasMoved = previousRef["snapshot-id"] !== currentRef["snapshot-id"];
    const isTheRowsOwnBranchMove =
      currentRef.type === "branch" &&
      currentRef["snapshot-id"] === rowSnapshotId;
    if (hasMoved && !isTheRowsOwnBranchMove) {
      changes.push(
        refChange(
          currentRef.type,
          name,
          `moved to snapshot ${currentRef["snapshot-id"]}`,
        ),
      );
    }

    const retentionChange = describeRetentionChange(
      name,
      previousRef,
      currentRef,
    );
    if (retentionChange !== null) {
      changes.push(retentionChange);
    }
  }

  for (const [name, previousRef] of Object.entries(previousRefs)) {
    if (currentRefs[name] === undefined) {
      changes.push(refChange(previousRef.type, name, "deleted"));
    }
  }

  return changes;
};
