import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { docsSearchSchema } from "../shared/lib/searchParams";

export const Route = createFileRoute("/docs")({
  validateSearch: docsSearchSchema,
  component: lazy(() => import("../pages/DocsPage")),
});
