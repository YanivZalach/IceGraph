import { useState, type SubmitEvent } from "react";
import { useNavigate, useSearch } from "@tanstack/react-router";
import HelpTerm from "../../../shared/components/HelpTerm";
import {
  UI_BODY_MUTED_CLASS,
  UI_PAGE_TITLE_CLASS,
} from "../../../uiTypography";
import { formatLocaleDateTime, parseUtcDate } from "../../../utils/dateUtils";
import type { SnapshotEntry } from "../../snapshots/snapshotRange";
import type { TableMetadata } from "../../table/api/metadataSchemas";
import {
  listPartitionFields,
  partitionFilterSearch,
  partitionValuePlaceholder,
  readPartitionFilters,
} from "../partitionFilters";

interface PartitionFilterFormProps {
  tableName: string;
  entries: readonly SnapshotEntry[];
  metadata: TableMetadata | undefined;
}

const CONTROL_CLASS =
  "w-full rounded-lg border border-edge bg-canvas px-3 py-2 text-sm text-ink placeholder:text-slate-500 focus:border-accent focus:outline-none";

const PartitionFilterForm = ({
  tableName,
  entries,
  metadata,
}: PartitionFilterFormProps) => {
  const search = useSearch({ from: "/snapshots-selection" });
  const navigate = useNavigate();
  const fields = listPartitionFields(metadata);
  const [chosenSnapshotId, setChosenSnapshotId] = useState<string | null>(null);
  const [values, setValues] = useState<Record<string, string | undefined>>(() =>
    Object.fromEntries(
      readPartitionFilters(search).map(({ name, value }) => [name, value]),
    ),
  );
  const requestedSnapshotId = search.partition_snapshot;
  const snapshotId =
    chosenSnapshotId ?? requestedSnapshotId ?? entries[0]?.snapshotId ?? "";
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
  const canOpen = snapshotId !== "" && filters.length > 0;

  if (fields.length === 0) return null;

  const handleSubmit = (event: SubmitEvent<HTMLFormElement>): void => {
    event.preventDefault();
    if (!canOpen) return;
    void navigate({
      to: "/partition/filetree",
      search: {
        table: tableName,
        snapshot: snapshotId,
        ...partitionFilterSearch(filters),
      },
    });
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
        <label className="flex flex-col gap-1.5 text-xs font-bold uppercase tracking-wide text-slate-500">
          Snapshot
          <select
            value={snapshotId}
            onChange={(event) => {
              setChosenSnapshotId(event.target.value);
            }}
            className={CONTROL_CLASS}
          >
            {snapshotOptions.map((entry) => (
              <option key={entry.snapshotId} value={entry.snapshotId}>
                {[
                  formatLocaleDateTime(parseUtcDate(entry.timestamp)),
                  entry.operation,
                  entry.snapshotId,
                ]
                  .filter(Boolean)
                  .join(" · ")}
              </option>
            ))}
          </select>
        </label>
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
          className="w-full cursor-pointer rounded-lg border-2 border-accent py-3 font-bold text-accent transition-colors hover:bg-accent-muted disabled:cursor-not-allowed disabled:border-edge disabled:text-slate-600"
        >
          Open Partition Files
        </button>
      </form>
    </section>
  );
};

export default PartitionFilterForm;
