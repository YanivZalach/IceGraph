import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { tableSearchSchema } from "../../shared/lib/searchParams";

export const Route = createFileRoute("/table")({
  validateSearch: tableSearchSchema,
  component: lazy(() => import("../../pages/TableLayout")),
});
