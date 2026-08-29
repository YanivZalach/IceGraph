import { lazy } from "react";
import { createFileRoute, redirect } from "@tanstack/react-router";
import { IS_MOCK, MOCK_TABLE_SEARCH } from "../appConstants";
import { snapshotSelectionSearchSchema } from "../shared/lib/searchParams";

export const Route = createFileRoute("/snapshots-selection")({
  validateSearch: snapshotSelectionSearchSchema,
  beforeLoad: () => {
    if (IS_MOCK) {
      throw redirect({
        to: "/table/timeline",
        search: MOCK_TABLE_SEARCH,
        replace: true,
      });
    }
  },
  component: lazy(() => import("../pages/SnapshotSelectionPage")),
});
