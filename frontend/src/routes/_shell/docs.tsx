import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/_shell/docs")({
  component: lazy(() => import("../../pages/DocsPage")),
});
