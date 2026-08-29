import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/table/timeline")({
  component: lazy(
    () => import("../../features/timeline/components/TimelinePage"),
  ),
});
