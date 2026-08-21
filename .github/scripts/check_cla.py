"""Require every contributor on a pull request to accept the IceGraph CLA."""

import json
import os
import re
import subprocess
import sys

CLA_VERSION = "1.0"
STATUS_CONTEXT = "CLA"
SIGNERS_PATH = ".github/cla-signers.json"
EXEMPT_LOGINS = {"yanivzalach"}
COMMENT_URL_RE = re.compile(
    r"^https://github\.com/([^/]+/[^/]+)/pull/(\d+)#issuecomment-(\d+)$",
    re.IGNORECASE,
)
SIGNER_FIELDS = {
    "github_login",
    "github_user_id",
    "cla_version",
    "accepted_at",
    "pull_request",
    "comment_url",
}


def statement(version):
    return (
        "I have read and agree to the IceGraph Individual Contributor License Agreement, "
        f"Harmony HA-CLA-I-ANY version {version}, "
        "and I confirm that I have authority to submit my contribution."
    )


def gh(args):
    result = subprocess.run(
        ["gh", "api", *args], capture_output=True, text=True, check=False
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout


def api_get(path):
    return json.loads(gh([path]))


def repo_get(repo, path):
    return api_get(f"repos/{repo}/{path}")


def repo_list(repo, path):
    output = gh(["--paginate", f"repos/{repo}/{path}", "--jq", ".[]"])
    return [json.loads(line) for line in output.splitlines() if line]


def signer_identity(account):
    if not account:
        return None
    login = account["login"].lower()
    if account.get("type") == "Bot":
        return None
    if login in EXEMPT_LOGINS:
        return None
    return account["id"], login


def contributors(pr, commits):
    required = {}
    unlinked = set()

    identity = signer_identity(pr["user"])
    if identity:
        required[identity[0]] = identity[1]

    for commit in commits:
        if commit.get("author"):
            identity = signer_identity(commit["author"])
            if identity:
                required[identity[0]] = identity[1]
        else:
            email = commit.get("commit", {}).get("author", {}).get("email")
            unlinked.add((email or "commit with no author email").lower())

    return required, unlinked


def verify_signer_record(repo, record):
    missing = SIGNER_FIELDS - record.keys()
    if missing:
        raise RuntimeError(
            f"Incomplete signer record, missing: {', '.join(sorted(missing))}"
        )

    match = COMMENT_URL_RE.fullmatch(record["comment_url"])
    if not match or match.group(1).lower() != repo.lower():
        raise RuntimeError(f"Invalid CLA comment URL: {record['comment_url']}")
    if int(match.group(2)) != record["pull_request"]:
        raise RuntimeError(f"CLA comment PR mismatch: {record['comment_url']}")

    comment = repo_get(repo, f"issues/comments/{match.group(3)}")
    expected_issue = (
        f"https://api.github.com/repos/{repo}/issues/{record['pull_request']}"
    )
    if comment["issue_url"].lower() != expected_issue.lower():
        raise RuntimeError(
            f"CLA comment belongs to another PR: {record['comment_url']}"
        )
    if comment["html_url"] != record["comment_url"]:
        raise RuntimeError(
            f"CLA comment URL does not match GitHub: {record['comment_url']}"
        )
    if comment["user"]["id"] != record["github_user_id"]:
        raise RuntimeError(f"CLA signer ID mismatch: {record['github_login']}")
    if comment["created_at"] != record["accepted_at"]:
        raise RuntimeError(f"CLA acceptance time mismatch: {record['github_login']}")
    if comment["body"].strip() != statement(record["cla_version"]):
        raise RuntimeError(f"CLA statement mismatch: {record['github_login']}")
    return record["github_user_id"]


def permanent_signers(repo):
    with open(SIGNERS_PATH) as file:
        registry = json.load(file)

    accepted = set()
    for record in registry.get("signers", []):
        if record.get("cla_version") != CLA_VERSION:
            continue
        try:
            accepted.add(verify_signer_record(repo, record))
        except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
            print(f"::warning::Ignoring signer record: {error}")
    return accepted


def set_status(repo, sha, state, description, target_url):
    gh(
        [
            "-X",
            "POST",
            f"repos/{repo}/statuses/{sha}",
            "-f",
            f"state={state}",
            "-f",
            f"context={STATUS_CONTEXT}",
            "-f",
            f"description={description[:140]}",
            "-f",
            f"target_url={target_url}",
        ]
    )


def verify(repo, number, target_url):
    pr = repo_get(repo, f"pulls/{number}")
    commits = repo_list(repo, f"pulls/{number}/commits")
    comments = repo_list(repo, f"issues/{number}/comments")

    required, unlinked = contributors(pr, commits)
    accepted = permanent_signers(repo) | {
        comment["user"]["id"]
        for comment in comments
        if comment["body"].strip() == statement(CLA_VERSION)
    }
    missing = [required[user_id] for user_id in sorted(required.keys() - accepted)]

    if unlinked:
        problem = f"Commit email not linked to a GitHub account: {', '.join(sorted(unlinked))}"
    elif missing:
        problem = f"CLA not accepted by: {', '.join(missing)}"
    else:
        message = f"All contributors accepted CLA v{CLA_VERSION}."
        set_status(repo, pr["head"]["sha"], "success", message, target_url)
        print(message)
        return 0

    set_status(repo, pr["head"]["sha"], "failure", problem, target_url)
    print(f"::error::{problem}")
    print(f"Required statement: {statement(CLA_VERSION)}")
    return 1


def main():
    repo = os.environ["REPO"]
    number = os.environ["PR_NUMBER"]
    target_url = (
        f"{os.environ['SERVER_URL']}/{repo}/actions/runs/{os.environ['RUN_ID']}"
    )
    try:
        return verify(repo, number, target_url)
    except (RuntimeError, OSError, ValueError, KeyError, TypeError) as error:
        try:
            sha = repo_get(repo, f"pulls/{number}")["head"]["sha"]
            set_status(
                repo, sha, "error", f"CLA check could not run: {error}", target_url
            )
        except (RuntimeError, OSError, ValueError, KeyError, TypeError):
            pass
        print(f"::error::CLA check could not run: {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
