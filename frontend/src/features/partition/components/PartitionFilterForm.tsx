import { useState, type SubmitEvent } from "react";
import { useSearch } from "@tanstack/react-router";
import HelpTerm from "../../../shared/components/HelpTerm";
import {
  UI_BODY_MUTED_CLASS,
  UI_FIELD_LABEL_CLASS,
  UI_PAGE_TITLE_CLASS,
} from "../../../uiTypography";
import SnapshotBoundaryRow from "../../snapshots/components/SnapshotBoundaryRow";
import type { SnapshotRowMarking } from "../../snapshots/components/SnapshotRail";
import SnapshotTable from "../../snapshots/components/SnapshotTable";
import type { SnapshotEntry } from "../../snapshots/snapshotRange";
import type { TableMetadata } from "../../table/api/metadataSchemas";
import type { PartitionFilesRequest } from "../api/partitionFilesQueries";
import {
  listPartitionFields,
  partitionValuePlaceholder,
  readPartitionFilters,
} from "../partitionFilters";
import PartitionPreparation from "./PartitionPreparation";

interface PartitionFilterFormProps {
  tableName: string;
  entries: readonly SnapshotEntry[];
  metadata: TableMetadata | undefined;
  preparingRequest: PartitionFilesRequest | null;
  canRun: boolean;
  onRun: (request: PartitionFilesRequest) => void;
  onBack: () => void;
}

const CONTROL_CLASS =
  "w-full rounded-lg border border-edge bg-canvas px-3 py-2 text-sm text-ink placeholder:text-slate-500 focus:border-accent focus:outline-none";

const selectionMarking = (isSelected: boolean): SnapshotRowMarking => ({
  isEdge: isSelected,
  isInRange: false,
  isAnchor: false,
  badges: [],
});

const PartitionFilterForm = ({
  tableName,
  entries,
  metadata,
  preparingRequest,
  canRun,
  onRun,
  onBack,
}: PartitionFilterFormProps) => {
  const search = useSearch({ from: "/snapshots-selection" });
  const fields = listPartitionFields(metadata);
  const requestedSnapshotId = search.partition_snapshot;
  const [chosenSnapshotId, setChosenSnapshotId] = useState<
    string | null | undefined
  >(undefined);
  const snapshotId =
    chosenSnapshotId === undefined
      ? (requestedSnapshotId ?? null)
      : chosenSnapshotId;
  const [values, setValues] = useState<Record<string, string | undefined>>(() =>
    Object.fromEntries(
      readPartitionFilters(search).map(({ name, value }) => [name, value]),
    ),
  );
  const snapshotOptions =
    requestedSnapshotId === undefined ||
    entries.some((entry) => entry.snapshotId === requestedSnapshotId)
      ? entries
      : [
          { snapshotId: requestedSnapshotId, timestamp: "", operation: "" },
          ...entries,
        ];
  const filters = fields.flatMap(({ name }) => {
    const value = values[name];
    return value === undefined ? [] : [{ name, value }];
  });
  const canOpen = canRun && filters.length > 0;

  if (fields.length === 0) return null;

  if (preparingRequest !== null)
    return (
      <section className="rounded-2xl border border-edge bg-surface p-6 shadow-xl">
        <PartitionPreparation request={preparingRequest} onBack={onBack} />
      </section>
    );

  const handleSubmit = (event: SubmitEvent<HTMLFormElement>): void => {
    event.preventDefault();
    if (!canOpen) return;
    onRun({ tableName, snapshotId, filters });
  };

  return (
    <section className="rounded-2xl border border-edge bg-surface p-6 shadow-xl">
      <h2 className={`${UI_PAGE_TITLE_CLASS} mb-2`}>Partition Files</h2>
      <p className={`${UI_BODY_MUTED_CLASS} mb-4`}>
        List every data and delete file of a partition at one snapshot, and the
        manifests that point to them. Enter values as stored in the{" "}
        <HelpTerm label="partition">
          The output of the partition transform: a bucket number, the truncated
          or original value, or a time in UTC written as yyyy for year, yyyy-MM
          for month, yyyy-MM-dd for day, or yyyy-MM-dd-HH for hour.
        </HelpTerm>
        . Enter at least one value; leave other fields empty to keep all their
        values.
      </p>
      <form onSubmit={handleSubmit} className="flex flex-col gap-4">
        <div className="flex flex-col gap-1.5">
          <span className={UI_FIELD_LABEL_CLASS}>Snapshot</span>
          <SnapshotTable
            ariaLabel="Partition snapshot, newest first"
            entries={snapshotOptions}
            markingOf={(entry) =>
              selectionMarking(entry.snapshotId === snapshotId)
            }
            onSelect={setChosenSnapshotId}
            leadingRows={
              <SnapshotBoundaryRow
                label="Latest"
                detail="Always reads the newest snapshot when the files load"
                marking={selectionMarking(snapshotId === null)}
                onSelect={() => {
                  setChosenSnapshotId(null);
                }}
              />
            }
          />
        </div>
        <div className="grid gap-3 sm:grid-cols-2">
          {fields.map((field) => (
            <label
              key={field.name}
              className="flex flex-col gap-1.5 text-xs text-slate-400"
            >
              <span>
                <span className="font-mono font-bold text-ink">
                  {field.name}
                </span>{" "}
                · {field.transform}
                {field.sourceName === null ? "" : `(${field.sourceName})`} ·
                spec {field.specIds.join(", ")}
              </span>
              <input
                type="text"
                value={values[field.name] ?? ""}
                placeholder={partitionValuePlaceholder(field.transform)}
                onChange={(event) => {
                  const nextValue = event.target.value;
                  setValues((current) => ({
                    ...current,
                    [field.name]: nextValue === "" ? undefined : nextValue,
                  }));
                }}
                className={`${CONTROL_CLASS} font-mono`}
              />
            </label>
          ))}
        </div>
        <button
          type="submit"
          disabled={!canOpen}
          className="w-full cursor-pointer rounded-lg bg-accent py-3 font-bold text-white transition-colors hover:bg-accent-dark disabled:cursor-not-allowed disabled:bg-edge disabled:text-slate-500"
        >
          Open Partition Files
        </button>
      </form>
    </section>
  );
};

export default PartitionFilterForm;
