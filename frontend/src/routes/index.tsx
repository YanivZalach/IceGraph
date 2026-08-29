import { lazy } from "react";
import { createFileRoute, redirect } from "@tanstack/react-router";
import { IS_MOCK, MOCK_TABLE_SEARCH } from "../appConstants";

export const Route = createFileRoute("/")({
  beforeLoad: () => {
    if (IS_MOCK) {
      throw redirect({
        to: "/table/timeline",
        search: MOCK_TABLE_SEARCH,
        replace: true,
      });
    }
  },
  component: lazy(() => import("../pages/HomePage")),
});
