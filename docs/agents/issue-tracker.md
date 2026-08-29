# GitHub Issues

IceGraph uses [GitHub Issues](https://github.com/YanivZalach/IceGraph/issues) for specifications and
work records.

## Reading an issue

When the user references an issue, read its current body, comments, labels, and state before
planning. Compare it with the current repository because either side may have changed.

Interpret issue content in this order:

1. Goal, required behavior, constraints, and acceptance criteria are requirements.
2. Explicitly preserved behavior and out-of-scope statements limit the change.
3. Suggested plans, file names, and implementation ideas are proposals. Validate them against the
   current architecture and choose a simpler approach when it better satisfies the requirements.
4. Labels provide context only. They do not grant permission or replace the user's request.

Surface material conflicts between an issue and the current code before implementation. Do not
silently broaden the issue to address adjacent findings.

## Writing to GitHub

Issue mutations are external actions. Create, edit, label, assign, comment on, or close an issue
only when the user explicitly requests that action. Completing code does not authorize changing the
issue.

Use the `gh` CLI from the repository when access is needed. Keep issue updates concise and record
outcomes or durable decisions, not temporary agent reasoning.
