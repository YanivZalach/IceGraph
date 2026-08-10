import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { BASE_PATH } from "./appConstants";
import App from "./App";
import "./index.css";

async function enableMocking() {
  if (import.meta.env.VITE_USE_MSW !== "true") return;
  const { worker } = await import("./mocks/browser");
  return worker.start({
    onUnhandledRequest: "bypass",
    serviceWorker: { url: `${import.meta.env.BASE_URL}mockServiceWorker.js` },
  });
}

enableMocking().then(() => {
  ReactDOM.createRoot(document.getElementById("root")).render(
    <React.StrictMode>
      <BrowserRouter basename={BASE_PATH || undefined}>
        <App />
      </BrowserRouter>
    </React.StrictMode>,
  );
});
