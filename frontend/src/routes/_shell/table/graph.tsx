import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { graphSearchSchema } from "../../../shared/lib/searchParams";

export const Route = createFileRoute("/_shell/table/graph")({
  validateSearch: graphSearchSchema,
  component: lazy(() => import("../../../pages/GraphPage")),
});
