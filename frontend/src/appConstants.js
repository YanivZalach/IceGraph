import { env } from "./shared/lib/env";

export const MOCK_HOME_ROUTE = "/table/timeline?table=default.events";
export const MOCK_TABLE = "default.events";
/** Vite `base` without trailing slash; empty string at site root. */
export const BASE_PATH = env.basePath;
export const MOCK_HOME = `${BASE_PATH}${MOCK_HOME_ROUTE}`;
export const IS_MOCK = env.isMock;
/** Release tag baked in at build time by CI; falls back to 'dev'. */
export const APP_VERSION = env.appVersion;
/** Source for the exact deployed version when CI provides it. */
export const SOURCE_URL = env.sourceUrl;
export const LICENSE_URL =
  "https://github.com/YanivZalach/IceGraph/blob/master/LICENSE";
export const COPYRIGHT_NOTICE = "Copyright (c) 2026 Yaniv Zalach";
/** Query param used to hand off which graph node should be auto-selected on load. */
export const SELECT_NODE_ID_PARAM = "select_node_id";
