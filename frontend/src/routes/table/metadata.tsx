import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/table/metadata")({
  component: lazy(() => import("../../features/metadata/MetadataPage")),
});
