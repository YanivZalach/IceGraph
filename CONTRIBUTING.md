# Contributing to IceGraph

Thank you for contributing.

## Before opening a pull request

1. Read [ARCHITECTURE_PHILOSOPHY.md](ARCHITECTURE_PHILOSOPHY.md).
2. Open or reference an issue for substantial changes.
3. Read the [IceGraph Individual Contributor License Agreement](CLA.md).
4. Keep changes focused. Tests may be used temporarily during development, but test files must be
   removed before submission and must not be included in the contribution. Run the applicable
   format, lint, typecheck, build, and behavioral checks instead.
5. Commit with an email linked to your GitHub account, so the `CLA` check can
   identify you. Unlinked commits block the pull request.

## Accept the CLA

The CLA lets you keep ownership of your contribution while giving IceGraph
permission to use and license it. After opening a pull request, each pull
request author and commit author just needs to post this exact comment from
their own GitHub account:

> I have read and agree to the IceGraph Individual Contributor License Agreement, Harmony HA-CLA-I-ANY version 1.0, and I confirm that I have authority to submit my contribution.

Automation permanently records a valid acceptance on the repository's
`cla-records` branch. After it is recorded, later editing or deletion of the
original comment does not revoke the acceptance. The same recorded acceptance
covers later contributions under this CLA version.

After `CLA acceptance recording` succeeds, push another commit to trigger the
checks again, or ask a maintainer to rerun the failed `CLA verification`
workflow so it reads the permanent acceptance record.

## Third-party material

Required by Section 3(d) of the [CLA](CLA.md). If any part of your contribution
is not your own work, it must be under a license compatible with `AGPL-3.0-only`
(MIT, BSD, Apache-2.0 are fine; GPL, LGPL, MPL, SSPL and proprietary are not),
you must keep its original copyright and license notice, and you must name the
source and its license in the pull request description.

## Licensing of contributions

IceGraph is licensed under [`AGPL-3.0-only`](LICENSE). You license your
contribution under that license plus the terms of the [CLA](CLA.md), which lets
the project steward also offer IceGraph commercially. CLA Section 2.3 keeps your
contribution available under the license in effect when you submitted it.
