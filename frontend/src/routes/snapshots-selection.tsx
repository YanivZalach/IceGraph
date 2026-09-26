import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { snapshotSelectionSearchSchema } from "../shared/lib/searchParams";

export const Route = createFileRoute("/snapshots-selection")({
  validateSearch: snapshotSelectionSearchSchema,
  component: lazy(() => import("../pages/SnapshotSelectionPage")),
});
