import { useEffect } from "react";
import { useNavigate, useSearch } from "@tanstack/react-router";
import type { CommitKindFilter } from "../lib/filterTimelineRows";

interface TimelineSearchValidity {
  isKnownBranch: (branchName: string) => boolean;
  isSelectableEvent: (filePath: string) => boolean;
}

export interface TimelineSearchState {
  kindFilter: CommitKindFilter;
  branchFilter: string | null;
  requestedEvent: string | undefined;
  setKindFilter: (kind: CommitKindFilter) => void;
  setBranchFilter: (branchName: string | null) => void;
  clearFilters: () => void;
  setEvent: (filePath: string | undefined) => void;
}

/** A branch or event the loaded range no longer has is ignored, then dropped from the URL. */
export const useTimelineSearchState = ({
  isKnownBranch,
  isSelectableEvent,
}: TimelineSearchValidity): TimelineSearchState => {
  const navigate = useNavigate({ from: "/table/timeline" });
  const search = useSearch({ from: "/table/timeline" });
  const requestedBranch = search.timeline_branch;
  const requestedEvent = search.timeline_event;
  const hasStaleBranch =
    requestedBranch !== undefined && !isKnownBranch(requestedBranch);
  const hasStaleEvent =
    requestedEvent !== undefined && !isSelectableEvent(requestedEvent);

  useEffect(() => {
    if (!hasStaleBranch && !hasStaleEvent) return;
    void navigate({
      replace: true,
      state: (previous) => previous,
      search: (previous) => ({
        ...previous,
        timeline_branch:
          hasStaleBranch && previous.timeline_branch === requestedBranch
            ? undefined
            : previous.timeline_branch,
        timeline_event:
          hasStaleEvent && previous.timeline_event === requestedEvent
            ? undefined
            : previous.timeline_event,
      }),
    });
  }, [
    hasStaleBranch,
    hasStaleEvent,
    navigate,
    requestedBranch,
    requestedEvent,
  ]);

  return {
    kindFilter: search.timeline_kind ?? "all",
    branchFilter: hasStaleBranch ? null : (requestedBranch ?? null),
    requestedEvent,
    setKindFilter: (kind) => {
      void navigate({
        search: (previous) => ({
          ...previous,
          timeline_kind: kind === "all" ? undefined : kind,
        }),
      });
    },
    setBranchFilter: (branchName) => {
      void navigate({
        search: (previous) => ({
          ...previous,
          timeline_branch: branchName ?? undefined,
        }),
      });
    },
    clearFilters: () => {
      void navigate({
        search: (previous) => ({
          ...previous,
          timeline_kind: undefined,
          timeline_branch: undefined,
        }),
      });
    },
    setEvent: (filePath) => {
      void navigate({
        search: (previous) => ({ ...previous, timeline_event: filePath }),
      });
    },
  };
};
