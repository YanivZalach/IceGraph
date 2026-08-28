IceGraph ships an AI-assistant **skill** so you can ask an agent to inspect and debug your tables directly - list tables, read snapshot history, pull the metadata graph, and get links back into this UI, all in plain language.

### Claude Code

Ships as a plugin. Inside Claude Code, run:

```bash
/plugin marketplace add YanivZalach/IceGraph
/plugin install icegraph
```

This is a **one-time install** - once it's done, the plugin is available in every future Claude Code session on that machine, not just the current one.

### Other coding agents / Manual install

No plugin system needed - the skill is a single, self-contained instructions file. Grab it, then tell your agent to read and follow it:

[Download SKILL.md]({{BASE_PATH}}/SKILL.md)

```bash
Read the SKILL.md file I just downloaded and follow it as your instructions whenever you work with Iceberg tables and IceGraph.
```

### Requirements

Nothing to set up ahead of time - the agent checks for `icegraph-client` itself, gives you the right install command if it's missing, and asks for your server's address and any auth it needs as you go.
