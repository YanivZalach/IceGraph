import { env } from "./shared/lib/env";

export const MOCK_TABLE = "default.events";
export const MOCK_LATEST_SNAPSHOT_ID = "3004708926140182071";
export const MOCK_TABLE_SEARCH = {
  table: MOCK_TABLE,
  start_snapshot_id: MOCK_LATEST_SNAPSHOT_ID,
  end_snapshot_id: MOCK_LATEST_SNAPSHOT_ID,
};
export const MOCK_HOME_ROUTE = `/table/timeline?${new URLSearchParams(MOCK_TABLE_SEARCH).toString()}`;
/** Vite `base` without trailing slash; empty string at site root. */
export const BASE_PATH = env.basePath;
export const MOCK_HOME = `${BASE_PATH}${MOCK_HOME_ROUTE}`;
export const IS_MOCK = env.isMock;
/** Release tag baked in at build time by CI; falls back to 'dev'. */
export const APP_VERSION = env.appVersion;
/** Query param used to hand off which graph node should be auto-selected on load. */
export const SELECT_NODE_ID_PARAM = "select_node_id";
