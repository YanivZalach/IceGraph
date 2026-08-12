import { Outlet, createFileRoute } from "@tanstack/react-router";
import NavBar from "../components/NavBar";

export const Route = createFileRoute("/_shell")({
  component: () => (
    <div className="min-h-screen bg-canvas flex flex-col">
      <NavBar />
      <Outlet />
    </div>
  ),
});
