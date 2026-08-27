import { PIP_INSTALL_COMMAND } from "../docsConstants";

const ClientCliSection = () => (
  <div className="space-y-5">
    <p>
      <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
        icegraph-client
      </code>{" "}
      is a Python client and CLI for the same backend API this UI uses - handy
      for scripting access to tables, snapshots, and the metadata graph.
    </p>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">Install</h3>
      <p>
        <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
          icegraph-client
        </code>{" "}
        is only guaranteed compatible with the exact same version of the
        IceGraph server it talks to - they're released together. Install that
        version:
      </p>
      <pre className="bg-surface-hover rounded-md p-3 text-sm text-[#7dd3fc] overflow-x-auto">
        {PIP_INSTALL_COMMAND}
      </pre>
    </div>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">Point it at your server</h3>
      <p>
        Pass{" "}
        <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
          --base-url
        </code>
        , or set it once via the{" "}
        <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
          ICEGRAPH_BASE_URL
        </code>{" "}
        environment variable.
      </p>
      <p>
        If your server sits behind auth, pass{" "}
        <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
          --token
        </code>{" "}
        (sent as an{" "}
        <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
          Authorization: Bearer
        </code>{" "}
        header) or{" "}
        <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
          --cookie
        </code>{" "}
        - or set them via the{" "}
        <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
          ICEGRAPH_TOKEN
        </code>{" "}
        /{" "}
        <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
          ICEGRAPH_COOKIE
        </code>{" "}
        environment variables.
      </p>
      <p>
        IceGraph itself doesn't have a login system, so you'll only need a token
        or cookie if your team has placed the server behind its own proxy. If
        you're already able to reach the IceGraph UI in a browser, that proxy
        has already authenticated your session - you can find the value it's
        using by opening DevTools → Application/Storage → Cookies (or the
        Network tab → any request → Request Headers) on the IceGraph page. If
        that doesn't apply, or you're not sure, it's best to check with whoever
        set up your IceGraph server - they'll know how their proxy handles
        authentication.
      </p>
      <p>
        If your server uses a self-signed or otherwise untrusted TLS
        certificate, pass{" "}
        <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
          --no-verify-ssl
        </code>{" "}
        to skip certificate verification, or set the{" "}
        <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
          ICEGRAPH_NO_VERIFY_SSL
        </code>{" "}
        environment variable.
      </p>
    </div>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">Commands</h3>
      <pre className="bg-surface-hover rounded-md p-3 text-sm text-[#7dd3fc] overflow-x-auto whitespace-pre-wrap">{`icegraph tables
icegraph snapshots <table>
icegraph graph <table> [--start-snapshot-id ID] [--end-snapshot-id ID]`}</pre>
    </div>
    <p>
      Each command prints its result as JSON on stdout, so it pipes and
      redirects cleanly (e.g. into{" "}
      <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
        jq
      </code>
      , Python, or a file). Status messages go to stderr, so they never end up
      mixed into the JSON output.
    </p>
  </div>
);

export default ClientCliSection;
