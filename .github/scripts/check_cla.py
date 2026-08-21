"""Verify that every contributor on a pull request has accepted the IceGraph CLA."""

import json
import os
import re
import subprocess
import sys

STEWARD_LOGINS = {"yanivzalach"}
STEWARD_EMAILS = {
    "131461377+yanivzalach@users.noreply.github.com",
    "yanivzalach@yanivs-air.lan",
    "yanivzalach@yanivs-macbook-air.local",
    "yzal2318@gmail.com",
}
IGNORED_LOGINS = {"web-flow"}
IGNORED_COAUTHOR_EMAILS = {
    "noreply@anthropic.com",
    "noreply@github.com",
    "actions@github.com",
    "t3code@users.noreply.github.com",
}
STATUS_CONTEXT = "CLA"
CLA_PATH = "CLA.md"

STATEMENT_TEMPLATE = (
    "I have read and agree to the IceGraph Individual Contributor License Agreement, "
    "Harmony HA-CLA-I-ANY version {version}, "
    "and I confirm that I have authority to submit my contribution."
)
VERSION_PATTERN = re.compile(r"^Harmony HA-CLA-I-ANY Version\s+(?P<version>\d+\.\d+)\s*$", re.MULTILINE)
COAUTHOR_PATTERN = re.compile(r"^\s*co-authored-by:[^<]*<(?P<email>[^>]+)>", re.IGNORECASE | re.MULTILINE)
NOREPLY_PATTERN = re.compile(r"^(?:\d+\+)?(?P<login>[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?)@users\.noreply\.github\.com$")


def normalize(text):
    return re.sub(r"\s+", " ", text).strip()


class GitHub:
    def __init__(self, repo):
        self.repo = repo

    def _run(self, args):
        result = subprocess.run(["gh", "api", *args], capture_output=True, text=True, check=False)
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip())
        return result.stdout

    def get(self, path):
        return json.loads(self._run([f"repos/{self.repo}/{path}"]))

    def list(self, path):
        output = self._run(["--paginate", f"repos/{self.repo}/{path}", "--jq", ".[]"])
        return [json.loads(line) for line in output.splitlines() if line.strip()]

    def post_status(self, sha, state, description, target_url):
        self._run(
            [
                "-X",
                "POST",
                f"repos/{self.repo}/statuses/{sha}",
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


class ClaCheck:
    def __init__(self, github, pr_number):
        self.github = github
        self.pull_request = github.get(f"pulls/{pr_number}")
        self.commits = github.list(f"pulls/{pr_number}/commits")
        self.comments = github.list(f"issues/{pr_number}/comments")
        self.number = pr_number

    @property
    def head_sha(self):
        return self.pull_request["head"]["sha"]

    @staticmethod
    def _is_excluded(account):
        if not account:
            return True
        login = account["login"]
        if account.get("type") == "Bot" or login.endswith("[bot]"):
            return True
        return login.lower() in STEWARD_LOGINS or login.lower() in IGNORED_LOGINS

    def _add(self, required, account):
        if not self._is_excluded(account):
            required.add(account["login"])

    def _add_identity(self, required, unresolved, account, identity):
        if account:
            self._add(required, account)
            return
        email = (identity or {}).get("email", "").strip().lower()
        if not email:
            unresolved.add("commit author without an email")
            return
        if email in STEWARD_EMAILS or email in IGNORED_COAUTHOR_EMAILS:
            return
        match = NOREPLY_PATTERN.match(email)
        if match:
            self._add(required, {"login": match.group("login")})
        else:
            unresolved.add(email)

    def required_signers(self):
        required = set()
        unresolved = set()
        self._add(required, self.pull_request["user"])
        for commit in self.commits:
            commit_data = commit.get("commit", {})
            self._add_identity(required, unresolved, commit.get("author"), commit_data.get("author"))
            for email in COAUTHOR_PATTERN.findall(commit["commit"]["message"]):
                email = email.strip().lower()
                if email in STEWARD_EMAILS or email in IGNORED_COAUTHOR_EMAILS:
                    continue
                match = NOREPLY_PATTERN.match(email)
                if match:
                    self._add(required, {"login": match.group("login")})
                else:
                    unresolved.add(email)
        return required, unresolved

    def accepted_signers(self, version):
        expected = normalize(STATEMENT_TEMPLATE.format(version=version))
        accepted = set()
        for comment in self.comments:
            if expected == normalize(comment["body"]):
                accepted.add(comment["user"]["login"])
        return accepted


def read_cla_version():
    if not os.path.exists(CLA_PATH):
        return None
    with open(CLA_PATH) as handle:
        match = VERSION_PATTERN.search(handle.read())
    return match.group("version") if match else None


def main():
    repo = os.environ["REPO"]
    pr_number = os.environ["PR_NUMBER"]
    target_url = f"{os.environ['SERVER_URL']}/{repo}/actions/runs/{os.environ['RUN_ID']}"

    github = GitHub(repo)
    version = read_cla_version()
    if version is None:
        print(f"No {CLA_PATH} on the base branch; nothing to verify.")
        return 0

    check = ClaCheck(github, pr_number)
    required, unresolved = check.required_signers()
    accepted = check.accepted_signers(version)
    missing = sorted(required - accepted)

    print(f"CLA version:     {version}")
    print(f"Required:        {sorted(required) or '(none)'}")
    print(f"Accepted:        {sorted(accepted) or '(none)'}")
    print(f"Unresolved:      {sorted(unresolved) or '(none)'}")

    if unresolved:
        problem = f"Unmapped contributor(s): {', '.join(sorted(unresolved))}"
        remedy = "Map each commit identity to a GitHub account; this check has no manual bypass."
    elif missing:
        problem = f"CLA not accepted by: {', '.join(missing)}"
        remedy = "Each must comment the acceptance statement from their own account."
    else:
        github.post_status(check.head_sha, "success", f"All contributors accepted CLA v{version}.", target_url)
        print("All required contributors have accepted the CLA.")
        return 0

    github.post_status(check.head_sha, "failure", problem, target_url)
    print(f"::error::{problem} {remedy}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
