export const MOCK_HOME_ROUTE = '/table/timeline?table=default.events'
export const MOCK_TABLE = 'default.events'
/** Vite `base` without trailing slash; empty string at site root. */
export const BASE_PATH = import.meta.env.BASE_URL.replace(/\/$/, '')
export const MOCK_HOME = `${BASE_PATH}${MOCK_HOME_ROUTE}`
export const IS_MOCK = import.meta.env.VITE_USE_MSW === 'true'
/** Query param used to hand off which graph node should be auto-selected on load. */
export const SELECT_NODE_ID_PARAM = 'select_node_id'
