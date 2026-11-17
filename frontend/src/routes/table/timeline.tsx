import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { timelineSearchSchema } from "../../shared/lib/searchParams";

export const Route = createFileRoute("/table/timeline")({
  validateSearch: timelineSearchSchema,
  component: lazy(
    () => import("../../features/timeline/components/TimelinePage"),
  ),
});
