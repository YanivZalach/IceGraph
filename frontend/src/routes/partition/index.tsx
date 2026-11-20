import { createFileRoute, redirect } from "@tanstack/react-router";

export const Route = createFileRoute("/partition/")({
  beforeLoad: ({ search }) => {
    throw redirect({ to: "/partition/filetree", search });
  },
});
