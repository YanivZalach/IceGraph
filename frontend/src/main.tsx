import { StrictMode } from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { RouterProvider, createRouter } from "@tanstack/react-router";
import { env } from "./shared/lib/env";
import { parseSearch, stringifySearch } from "./shared/lib/searchParams";
import { routeTree } from "./routeTree.gen";
import "./index.css";

const router = createRouter({
  routeTree,
  basepath: env.basePath || "/",
  parseSearch,
  stringifySearch,
  defaultNotFoundComponent: () => null,
});

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
    },
  },
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}

const enableMocking = async (): Promise<void> => {
  if (!env.isMock) return;
  const { worker } = await import("./mocks/browser");
  await worker.start({
    onUnhandledRequest: "bypass",
    serviceWorker: { url: `${env.basePath}/mockServiceWorker.js` },
  });
};

void enableMocking().then(() => {
  const rootElement = document.getElementById("root");
  if (rootElement === null) {
    throw new Error("Root element #root not found");
  }
  ReactDOM.createRoot(rootElement).render(
    <StrictMode>
      <QueryClientProvider client={queryClient}>
        <RouterProvider router={router} />
      </QueryClientProvider>
    </StrictMode>,
  );
});
