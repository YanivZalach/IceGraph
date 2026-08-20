import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/docs")({
  component: lazy(() => import("../pages/DocsPage")),
});
