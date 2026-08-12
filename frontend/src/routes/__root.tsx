import { Suspense } from "react";
import { Outlet, createRootRoute } from "@tanstack/react-router";
import { TableSpecsProvider } from "../context/TableSpecsContext";
import PageLoader from "../components/PageLoader";

export const Route = createRootRoute({
  component: () => (
    <TableSpecsProvider>
      <Suspense fallback={<PageLoader />}>
        <Outlet />
      </Suspense>
    </TableSpecsProvider>
  ),
});
