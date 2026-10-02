# GitHub issue tracker

The user supplied `ffff2004/codex-remote-android` as this repository’s tracker. It is already configured; no user setup is needed. Issue #3 is the accepted LNTP adoption specification, also preserved at `docs/implementation/LNTP_ADOPTION.md`.

Read the originating specification:

```sh
gh issue view 3 --repo ffff2004/codex-remote-android --json title,body,url,comments
```

Create or edit tracker issues when the task authorizes it (preserve multiline bodies with files):

```sh
gh issue create --repo ffff2004/codex-remote-android --title 'Title' --body-file /tmp/issue-body.md
gh issue edit 3 --repo ffff2004/codex-remote-android --body-file /tmp/issue-body.md
```

For review, compare the code to the issue and repository implementation specification; do not infer scope from commit messages alone. Each review round reports its base/head SHAs, full and incremental diff commands, gates and independent Standards/Spec conclusions in a PR comment. Reviewers use codexctl-as-subagent with the user-confirmed `--approve-for-me` mode and remain read-only.
