import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/_shell/table/metadata")({
  component: lazy(() => import("../../../pages/MetadataPage")),
});
