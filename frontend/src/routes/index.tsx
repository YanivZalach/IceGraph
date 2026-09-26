import { lazy } from "react";
import { createFileRoute } from "@tanstack/react-router";

export const Route = createFileRoute("/")({
  component: lazy(() => import("../pages/HomePage")),
});
