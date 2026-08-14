import { Suspense } from "react";
import { Outlet, createRootRoute } from "@tanstack/react-router";
import { TableSpecsProvider } from "../context/TableSpecsContext";
import NavBar from "../components/NavBar";
import PageLoader from "../components/PageLoader";

export const Route = createRootRoute({
  component: () => (
    <TableSpecsProvider>
      <div className="min-h-screen bg-canvas flex flex-col">
        <NavBar />
        <Suspense fallback={<PageLoader />}>
          <Outlet />
        </Suspense>
      </div>
    </TableSpecsProvider>
  ),
});
