import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { partitionSearchSchema } from "../../shared/lib/searchParams";

export const Route = createFileRoute("/partition")({
  validateSearch: partitionSearchSchema,
  component: lazy(() => import("../../pages/PartitionLayout")),
});
