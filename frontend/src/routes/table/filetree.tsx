import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/table/filetree")({
  component: lazy(() => import("../../pages/FileTreePage")),
});
