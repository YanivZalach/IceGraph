import { lazy } from "react";
import { createFileRoute, redirect } from "@tanstack/react-router";
import { IS_MOCK, MOCK_TABLE } from "../appConstants";

export const Route = createFileRoute("/")({
  beforeLoad: () => {
    if (IS_MOCK) {
      throw redirect({
        to: "/table/timeline",
        search: { table: MOCK_TABLE },
        replace: true,
      });
    }
  },
  component: lazy(() => import("../pages/HomePage")),
});
