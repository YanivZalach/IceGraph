`icegraph-client` is a Python client and CLI for the same backend API this UI uses - handy for scripting access to tables, snapshots, and the metadata graph.

### Install

`icegraph-client` is only guaranteed compatible with the exact same version of the IceGraph server it talks to - they're released together. Install that version:

```text
{{PIP_INSTALL_COMMAND}}
```

### Point it at your server

Pass `--base-url`, or set it once via the `ICEGRAPH_BASE_URL` environment variable.

If your server sits behind auth, pass `--token` (sent as an `Authorization: Bearer` header) or `--cookie`

- or set them via the
  `ICEGRAPH_TOKEN` / `ICEGRAPH_COOKIE` environment variables.

IceGraph itself doesn't have a login system, so you'll only need a token or cookie if your team has placed the server behind its own proxy. If you're already able to reach the IceGraph UI in a browser, that proxy has already authenticated your session - you can find the value it's using by opening DevTools → Application/Storage → Cookies (or the Network tab → any request → Request Headers) on the IceGraph page. If that doesn't apply, or you're not sure, it's best to check with whoever set up your IceGraph server - they'll know how their proxy handles authentication.

If your server uses a self-signed or otherwise untrusted TLS certificate, pass `--no-verify-ssl` to skip certificate verification, or set the `ICEGRAPH_NO_VERIFY_SSL` environment variable.

### Commands

```text
icegraph tables
icegraph snapshots <table>
icegraph graph <table> [--start-snapshot-id ID] [--end-snapshot-id ID]
```

Each command prints its result as JSON on stdout, so it pipes and redirects cleanly (e.g. into `jq`, Python, or a file). Status messages go to stderr, so they never end up mixed into the JSON output.
