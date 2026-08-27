import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { fileTreeSearchSchema } from "../../shared/lib/searchParams";

export const Route = createFileRoute("/table/filetree")({
  validateSearch: fileTreeSearchSchema,
  component: lazy(() => import("../../pages/FileTreePage")),
});
