import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { partitionGraphSearchSchema } from "../../shared/lib/searchParams";

export const Route = createFileRoute("/partition/graph")({
  validateSearch: partitionGraphSearchSchema,
  component: lazy(() => import("../../pages/PartitionGraphPage")),
});
