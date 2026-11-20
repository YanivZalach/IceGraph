import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { partitionFilesSearchSchema } from "../../shared/lib/searchParams";

export const Route = createFileRoute("/partition/filetree")({
  validateSearch: partitionFilesSearchSchema,
  component: lazy(() => import("../../pages/PartitionFilesPage")),
});
