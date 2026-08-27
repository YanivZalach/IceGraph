import { BASE_PATH } from "../../../appConstants";

const CodingAgentSection = () => (
  <div className="space-y-5">
    <p>
      IceGraph ships an AI-assistant{" "}
      <strong className="text-white">skill</strong> so you can ask an agent to
      inspect and debug your tables directly - list tables, read snapshot
      history, pull the metadata graph, and get links back into this UI, all in
      plain language.
    </p>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">Claude Code</h3>
      <p>Ships as a plugin. Inside Claude Code, run:</p>
      <pre className="bg-surface-hover rounded-md p-3 text-sm text-[#7dd3fc] overflow-x-auto whitespace-pre-wrap">{`/plugin marketplace add YanivZalach/IceGraph
/plugin install icegraph`}</pre>
      <p>
        This is a <strong className="text-white">one-time install</strong> -
        once it's done, the plugin is available in every future Claude Code
        session on that machine, not just the current one.
      </p>
    </div>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">
        Other coding agents / Manual install
      </h3>
      <p>
        No plugin system needed - the skill is a single, self-contained
        instructions file. Grab it, then tell your agent to read and follow it:
      </p>
      <a
        href={`${BASE_PATH}/SKILL.md`}
        download="SKILL.md"
        target="_blank"
        rel="noopener noreferrer"
        className="inline-block text-accent hover:text-blue-400 transition font-mono text-sm underline"
      >
        Download SKILL.md
      </a>
      <pre className="bg-surface-hover rounded-md p-3 text-sm text-[#7dd3fc] overflow-x-auto whitespace-pre-wrap">{`Read the SKILL.md file I just downloaded and follow it as your instructions whenever you work with Iceberg tables and IceGraph.`}</pre>
    </div>
    <div className="space-y-2">
      <h3 className="text-white font-semibold">Requirements</h3>
      <p>
        Nothing to set up ahead of time - the agent checks for{" "}
        <code className="bg-surface-hover px-1.5 py-0.5 rounded text-[#7dd3fc] text-sm">
          icegraph-client
        </code>{" "}
        itself, gives you the right install command if it's missing, and asks
        for your server's address and any auth it needs as you go.
      </p>
    </div>
  </div>
);

export default CodingAgentSection;
