import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useHotkey } from "@tanstack/react-hotkeys";
import { useNavigate, useSearch } from "@tanstack/react-router";
import { snapshotMapQueryOptions } from "../features/snapshots/api/snapshotQueries";
import {
  defaultSnapshotRange,
  sortSnapshotEntries,
  type SnapshotRange,
} from "../features/snapshots/snapshotRange";
import { useTableSpecs } from "../features/specs/tableSpecs";
import { useMetadataKeyboardScroll } from "../features/metadata/useMetadataKeyboardScroll";
import SnapshotRangeSelector from "../features/snapshots/components/SnapshotRangeSelector";
import GraphPreparation from "../features/table/components/GraphPreparation";
import type { GraphRequestParameters } from "../features/table/api/graphCache";
import LatestMetadataSection from "../features/metadata/components/LatestMetadataSection";
import SpecDetailsOverlay from "../features/specs/components/SpecDetailsOverlay";
import LoadingIndicator from "../components/LoadingIndicator";
import { isKeyboardInputTarget } from "../shared/lib/keyboard";
import { UI_BODY_MUTED_CLASS } from "../uiTypography";

interface TableRangeSelection {
  tableName: string;
  range: SnapshotRange;
}

const isActivatableTarget = (target: EventTarget | null): boolean =>
  target instanceof HTMLElement &&
  target.closest("button, a, summary") !== null;

const SnapshotSelectionPage = () => {
  const search = useSearch({ from: "/snapshots-selection" });
  const navigate = useNavigate();
  const tableName = search.table ?? "";
  const { detailsOpen } = useTableSpecs();
  const snapshotQuery = useQuery(snapshotMapQueryOptions(tableName));
  const entries = sortSnapshotEntries(snapshotQuery.data ?? {});
  const [selection, setSelection] = useState<TableRangeSelection | null>(null);
  const range =
    selection?.tableName === tableName
      ? selection.range
      : defaultSnapshotRange(entries);
  const [graphParameters, setGraphParameters] =
    useState<GraphRequestParameters | null>(null);
  const isPreparingGraph = graphParameters?.tableName === tableName;
  const canGenerate =
    tableName !== "" && snapshotQuery.isSuccess && !isPreparingGraph;
  useMetadataKeyboardScroll(detailsOpen);

  const handleGenerate = (): void => {
    setGraphParameters({
      tableName,
      startSnapshotId: range.startSnapshotId,
      endSnapshotId: range.endSnapshotId,
    });
  };

  useHotkey(
    "Enter",
    (event) => {
      if (isKeyboardInputTarget(event.target)) return;
      if (isActivatableTarget(event.target)) return;
      handleGenerate();
    },
    { enabled: canGenerate && !detailsOpen, preventDefault: false },
  );

  if (tableName === "" || snapshotQuery.isError)
    return (
      <div className="flex-1 flex items-center justify-center p-8 text-red-400">
        <div className="bg-red-950/50 border border-red-800 p-8 rounded-xl text-center">
          <h2 className="font-bold mb-2">Failed to Load Snapshots</h2>
          <p className="text-sm mb-4">
            {snapshotQuery.error?.message ?? "Missing table name"}
          </p>
          <button
            type="button"
            onClick={() => {
              void navigate({ to: "/" });
            }}
            className="text-slate-400 hover:text-white text-sm"
          >
            Go Back
          </button>
        </div>
      </div>
    );

  if (snapshotQuery.isPending)
    return (
      <div
        className={`flex-1 flex flex-col items-center justify-center ${UI_BODY_MUTED_CLASS}`}
      >
        <LoadingIndicator
          title="Loading snapshots"
          description={`Reading snapshot history for ${tableName}. This may take a moment for large tables.`}
        />
      </div>
    );

  return (
    <div className="min-w-0 flex-1 bg-canvas">
      <main className="mx-auto flex max-w-5xl flex-col gap-7 px-5 py-8 md:px-8">
        <section className="rounded-2xl border border-edge bg-surface p-6 shadow-xl">
          {isPreparingGraph ? (
            <GraphPreparation
              parameters={graphParameters}
              onBack={() => {
                setGraphParameters(null);
              }}
            />
          ) : (
            <SnapshotRangeSelector
              tableName={tableName}
              entries={entries}
              range={range}
              onRangeChange={(nextRange) => {
                setSelection({ tableName, range: nextRange });
              }}
              onGenerate={handleGenerate}
            />
          )}
        </section>
        <LatestMetadataSection tableName={tableName} />
      </main>
      <SpecDetailsOverlay />
    </div>
  );
};
export default SnapshotSelectionPage;
