import { APP_VERSION } from "../../appConstants";

export const PIP_INSTALL_COMMAND =
  APP_VERSION === "dev"
    ? "pip install icegraph-client"
    : `pip install icegraph-client==${APP_VERSION.replace(/^v/, "")}`;
