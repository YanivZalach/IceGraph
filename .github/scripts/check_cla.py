"""Require every contributor on a pull request to accept the IceGraph CLA."""

import json
import os
import re
import subprocess
import sys

CLA_VERSION = "1.0"
CLA_STATEMENT = (
    "I have read and agree to the IceGraph Individual Contributor License Agreement, "
    f"Harmony HA-CLA-I-ANY version {CLA_VERSION}, "
    "and I confirm that I have authority to submit my contribution."
)
STATUS_CONTEXT = "CLA"
STEWARD_LOGIN = "yanivzalach"
IGNORED_LOGINS = {"web-flow"}
IGNORED_EMAILS = {
    "131461377+yanivzalach@users.noreply.github.com",
    "actions@github.com",
    "noreply@anthropic.com",
    "noreply@github.com",
    "t3code@users.noreply.github.com",
    "yanivzalach@yanivs-air.lan",
    "yanivzalach@yanivs-macbook-air.local",
    "yzal2318@gmail.com",
}
COAUTHOR_RE = re.compile(r"^\s*co-authored-by:[^<]*<([^>]+)>", re.IGNORECASE | re.MULTILINE)
NOREPLY_RE = re.compile(r"^(?:\d+\+)?([A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)@users\.noreply\.github\.com$")


def gh(args):
    result = subprocess.run(["gh", "api", *args], capture_output=True, text=True, check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout


def get(repo, path):
    return json.loads(gh([f"repos/{repo}/{path}"]))


def list_all(repo, path):
    output = gh(["--paginate", f"repos/{repo}/{path}", "--jq", ".[]"])
    return [json.loads(line) for line in output.splitlines() if line]


def required_login(account):
    if not account:
        return None
    login = account["login"]
    if account.get("type") == "Bot" or login.endswith("[bot]"):
        return None
    if login.lower() == STEWARD_LOGIN or login.lower() in IGNORED_LOGINS:
        return None
    return login


def add_email(required, unresolved, email):
    email = (email or "").strip().lower()
    if email in IGNORED_EMAILS:
        return
    match = NOREPLY_RE.fullmatch(email)
    if match:
        login = required_login({"login": match.group(1)})
        if login:
            required.add(login)
    else:
        unresolved.add(email or "commit author without an email")


def contributors(pr, commits):
    required = set()
    unresolved = set()

    login = required_login(pr["user"])
    if login:
        required.add(login)

    for commit in commits:
        login = required_login(commit.get("author"))
        if login:
            required.add(login)
        elif not commit.get("author"):
            add_email(required, unresolved, commit.get("commit", {}).get("author", {}).get("email"))

        for email in COAUTHOR_RE.findall(commit["commit"]["message"]):
            add_email(required, unresolved, email)

    return required, unresolved


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


def main():
    repo = os.environ["REPO"]
    number = os.environ["PR_NUMBER"]
    target_url = f"{os.environ['SERVER_URL']}/{repo}/actions/runs/{os.environ['RUN_ID']}"
    pr = get(repo, f"pulls/{number}")
    commits = list_all(repo, f"pulls/{number}/commits")
    comments = list_all(repo, f"issues/{number}/comments")

    required, unresolved = contributors(pr, commits)
    accepted = {
        comment["user"]["login"]
        for comment in comments
        if comment["body"].strip() == CLA_STATEMENT
    }
    missing = sorted(required - accepted)

    if unresolved:
        problem = f"Unmapped contributor(s): {', '.join(sorted(unresolved))}"
    elif missing:
        problem = f"CLA not accepted by: {', '.join(missing)}"
    else:
        message = f"All contributors accepted CLA v{CLA_VERSION}."
        set_status(repo, pr["head"]["sha"], "success", message, target_url)
        print(message)
        return 0

    set_status(repo, pr["head"]["sha"], "failure", problem, target_url)
    print(f"::error::{problem}")
    print(f"Required statement: {CLA_STATEMENT}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
