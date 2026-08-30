# CLA records branch

Valid CLA acceptance comments are stored as one JSON file per GitHub user and
CLA version on the `cla-records` branch. The branch is created automatically
when the first acceptance is recorded.

Before enabling the CLA workflow, configure a repository ruleset that targets
the `cla-records` branch name. GitHub rulesets can target a branch before it
exists. Configure the ruleset to:

- Block branch deletion and force pushes.
- Allow the GitHub Actions integration to bypass rules so the CLA workflow can
  append records directly.
- Restrict other direct updates to repository administrators.

The workflow uses atomic, non-force reference updates and retries concurrent
writes. An identical existing acceptance succeeds without another commit, and
a conflicting record fails for manual review.

If recording fails because of a transient GitHub error, rerun the failed
`CLA automation` workflow. Do not edit a signer record to correct it. Investigate
the conflict and preserve the original record as evidence.
