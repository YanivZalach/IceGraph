import { useEffect } from "react";
import { useNavigate, useSearch } from "@tanstack/react-router";
import {
  filterTimelineRows,
  type CommitKindFilter,
} from "../lib/filterTimelineRows";
import type { TimelineData } from "../lib/timelineRow";
import { useTimelineSelection } from "../hooks/useTimelineSelection";
import DockedSplit from "../../../components/DockedSplit";
import EventDetailsPanel from "./details/EventDetailsPanel";
import { branchAccentsByName, eventToneFor } from "./eventColor";
import TimelineList from "./list/TimelineList";
import TimelineToolbar from "./list/TimelineToolbar";

interface TimelineContentProps {
  timeline: TimelineData;
  table: string | undefined;
}

const countEventRows = (rows: TimelineData["rows"]): number =>
  rows.filter((row) => row.kind !== "boundary").length;

const TimelineContent = ({ timeline, table }: TimelineContentProps) => {
  const navigate = useNavigate({ from: "/table/timeline" });
  const search = useSearch({ from: "/table/timeline" });
  const kindFilter = search.timeline_kind ?? "all";
  const branchFilter = search.timeline_branch;
  const requestedEvent = search.timeline_event;
  const branchOptions = [...branchAccentsByName(timeline.rows).keys()];
  const hasStaleBranch =
    branchFilter !== undefined && !branchOptions.includes(branchFilter);
  const hasStaleEvent =
    requestedEvent !== undefined &&
    !timeline.rows.some(
      (row) => row.kind !== "boundary" && row.filePath === requestedEvent,
    );
  const activeBranchFilter = hasStaleBranch ? null : (branchFilter ?? null);

  useEffect(() => {
    if (!hasStaleBranch && !hasStaleEvent) return;
    void navigate({
      replace: true,
      state: (previous) => previous,
      search: (previous) => ({
        ...previous,
        timeline_branch:
          hasStaleBranch && previous.timeline_branch === branchFilter
            ? undefined
            : previous.timeline_branch,
        timeline_event:
          hasStaleEvent && previous.timeline_event === requestedEvent
            ? undefined
            : previous.timeline_event,
      }),
    });
  }, [branchFilter, hasStaleBranch, hasStaleEvent, navigate, requestedEvent]);
  const shownRows = filterTimelineRows(
    timeline.rows,
    kindFilter,
    activeBranchFilter,
  );
  const selectedRow =
    shownRows.find(
      (row) => row.kind !== "boundary" && row.filePath === requestedEvent,
    ) ?? null;
  const selectedFilePath = selectedRow?.filePath ?? null;
  const { handleSelect, clearSelection } = useTimelineSelection(
    shownRows,
    selectedFilePath,
    (filePath) => {
      void navigate({
        search: (previous) => ({ ...previous, timeline_event: filePath }),
      });
    },
  );

  const commitCount = countEventRows(timeline.rows);
  const isFiltering = kindFilter !== "all" || activeBranchFilter !== null;

  const setKindFilter = (kind: CommitKindFilter) => {
    void navigate({
      search: (previous) => ({
        ...previous,
        timeline_kind: kind === "all" ? undefined : kind,
      }),
    });
  };

  const setBranchFilter = (branch: string | null) => {
    void navigate({
      search: (previous) => ({
        ...previous,
        timeline_branch: branch ?? undefined,
      }),
    });
  };

  const handleClearFilters = () => {
    void navigate({
      search: (previous) => ({
        ...previous,
        timeline_kind: undefined,
        timeline_branch: undefined,
      }),
    });
  };

  return (
    <DockedSplit
      className="h-graph w-full min-h-0 flex-1 overflow-hidden bg-canvas"
      panelDefaultSize="35%"
      accentColor={
        selectedRow === null ? undefined : eventToneFor(selectedRow).color
      }
      main={
        <div className="h-full min-h-0 min-w-0 overflow-y-auto [scrollbar-gutter:stable]">
          {commitCount > 0 && (
            <TimelineToolbar
              commitCount={commitCount}
              shownCommitCount={countEventRows(shownRows)}
              unreadableCommitCount={timeline.unreadableCommitCount}
              isTableMetadataUnreadable={timeline.isTableMetadataUnreadable}
              kindFilter={kindFilter}
              onKindFilterChange={setKindFilter}
              branchOptions={branchOptions}
              branchFilter={activeBranchFilter}
              onBranchFilterChange={setBranchFilter}
            />
          )}
          <div className="mx-auto max-w-6xl px-3 py-4 sm:px-5">
            <TimelineList
              timeline={timeline}
              rows={shownRows}
              table={table}
              isFiltering={isFiltering}
              onClearFilters={handleClearFilters}
              selectedFilePath={selectedFilePath}
              onSelect={handleSelect}
            />
          </div>
        </div>
      }
      sidePanel={
        selectedRow === null ? null : (
          <EventDetailsPanel
            timeline={timeline}
            row={selectedRow}
            onClose={clearSelection}
          />
        )
      }
    />
  );
};

export default TimelineContent;
