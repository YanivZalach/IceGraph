# Contributing to IceGraph

Thank you for contributing to IceGraph.

## Before submitting a contribution

1. Read [ARCHITECTURE_PHILOSOPHY.md](ARCHITECTURE_PHILOSOPHY.md).
2. Open or reference an issue for substantial changes.
3. Read the [Individual Contributor License Agreement, Harmony
   HA-CLA-I-ANY version 1.0](CLA.md).
4. Personally post the CLA acceptance comment on your pull request.

## Licensing of contributions

The public project is licensed under `AGPL-3.0-only`. If a contribution is
included in IceGraph, the CLA requires it to remain available under the project
license in effect on its submission date. The CLA also allows the project
steward to offer it under other licenses, including commercial,
source-available, and proprietary licenses.

Pull requests cannot be merged until every contributor has accepted the current
CLA. Every author and co-author accepts the same way, by personally posting this
comment on the pull request from their own account:

> I have read and agree to the IceGraph Individual Contributor License
> Agreement, Harmony HA-CLA-I-ANY version 1.0, and I confirm that I have
> authority to submit my contribution.

A comment is used rather than a checkbox because GitHub attributes a comment to
an authenticated account, while a pull request body can be edited by anyone with
write access and records no attribution.

The `CLA` status check verifies acceptance automatically. Required contributors
are the pull request author, every commit author, and every `Co-authored-by`
trailer on the pull request. The project steward and automated accounts are
excluded. No external signing service is required.

If any contributor identity cannot be mapped to a GitHub account, the check
fails and names it. A commit author must use an email associated with their
GitHub account. A `Co-authored-by` trailer must use that person's GitHub
`users.noreply.github.com` address. There is no label or manual bypass.

Configure the repository's `master` branch rules to require the `CLA` status
check before merging and to prevent bypassing that requirement. The script
reports status, but the branch rule is what enforces it at merge time.

If your contribution contains material whose copyright you do not own, identify
that material in the pull request and provide its copyright, license, and
attribution information. Do not submit it unless its license permits inclusion
in IceGraph. If an employer or another entity owns your contribution, obtain
its approval or contact the project steward about an entity agreement before
submitting it.

Every pull request requires a new acceptance. If the CLA changes, the project
steward increments its version and effective date and preserves the prior
version. The project steward also updates the acceptance statement and links it
to an immutable Git revision containing the new CLA version without altering
earlier pull request records.

Material proposed in an issue, discussion, email, or another channel is not
accepted for inclusion until its contributor personally accepts the CLA on the
pull request that includes it.

## Development checks

Follow the setup and validation commands in [README.md](README.md). Keep changes
focused, preserve existing copyright and attribution notices, and include tests
where practical.
