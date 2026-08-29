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
4. Labels provide context only. They do not grant permission, authorize work, or replace the user's
   request.

Surface material conflicts between an issue and the current code before implementation. Do not
silently broaden the issue to address adjacent findings.

## Writing to GitHub

Issue mutations are external actions. Create, edit, label, assign, comment on, or close an issue
only when the user explicitly requests that action. Completing code does not authorize changing the
issue.

Use the `gh` CLI from the repository when access is needed. Keep issue updates concise and record
outcomes or durable decisions, not temporary agent reasoning.

## Labels and Roadmap

Use GitHub's standard labels when they describe the issue. The only IceGraph-specific labels are
`frontend` and `backend`; apply either or both when they identify the affected scope. `good first
issue` remains the standard newcomer label.

When explicitly asked to create an issue:

1. Create the issue with only applicable labels.
2. Add it to the public
   [IceGraph Roadmap](https://github.com/users/YanivZalach/projects/3).
3. Set Status to `Todo`.
4. Set Category to exactly one of `Frontend Refactor`, `Enrichments`, or `Bugs`, based on the issue's
   purpose.

Creating an issue includes these Roadmap steps. Do not leave a new issue unclassified on the board.
