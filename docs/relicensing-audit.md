# Relicensing Audit

Audit date: 2026-08-20

This document records the repository review performed for the transition from
MIT to `AGPL-3.0-only`. It is a point-in-time engineering audit, not legal
advice.

## Dependency review

Reviewed inputs:

- `backend/pyproject.toml` and `backend/uv.lock`
- `icegraph-client/pyproject.toml` and `icegraph-client/uv.lock`
- `frontend/package.json` and `frontend/package-lock.json`
- Installed Python distribution metadata in the local virtual environments
- Installed npm package metadata in `frontend/node_modules`

No direct GPL, AGPL, LGPL, SSPL, Elastic License, or Commons Clause dependency
was found in the project manifests. No strong-copyleft package was found in the
frontend dependency lock or installed npm metadata.

The installed `setuptools` distribution vendors `autocommand 2.2.2`, whose
metadata reports LGPLv3. It is not a direct IceGraph dependency and does not
block the AGPL transition. Its presence in Python tooling and built container
environments must be considered separately before any future FSL-only
distribution.

Dependency licenses can change between releases. Release builds should retain
lockfiles and repeat this audit when dependencies are updated.

## Copyright and attribution inventory

The tracked source tree contained no per-file copyright headers or third-party
author attribution notices. The prior root license contained:

`Copyright (c) 2026 Yaniv Zalach`

That notice is preserved verbatim in `LICENSE-MIT`. No existing notice was
removed or altered.

Git history contains commits attributed to several human author identities in
addition to Yaniv Zalach, including A5HxD, Itay Adler, Itay Segev, Mangalam,
Pradeep Kambalapally, Raz, Razberrry, and Shahaf Elkayam. Commit authorship does
not by itself establish copyright ownership. Historical contributions were
submitted while the repository carried the MIT License. Retroactive CLAs from
material contributors remain advisable for acquisition diligence.

## License boundary

The final revision released under the prior MIT terms is:

`d6dd0a611fda5f987ca27df91d5d5cad3411dde9`

The repository history was not rewritten. `LICENSE-MIT` remains available for
historical versions.

## Public-tree and history review

The current tracked tree and all Git revisions were searched for filenames and
content indicating a private enterprise tier, paid-tier implementation,
premium-feature implementation, proprietary module, or commercial-license
implementation. No such product code was identified.

Matches in historical deployment commits came from generated frontend bundles
and third-party dependency data, not IceGraph enterprise functionality. Future
private or paid features should be created in a separate private repository
from their first commit.
