# Codex Remote for Android

[![Android CI](https://github.com/ffff2004/codex-remote-android/actions/workflows/android.yml/badge.svg)](https://github.com/ffff2004/codex-remote-android/actions/workflows/android.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

An Android client for Codex hosts reached over SSH. A saved connection represents
one host; after connecting, the app imports every resumable remote Codex
conversation and groups projects from each thread's working directory. The app
does not run a local agent: it starts the shared remote app-server daemon with
`codex app-server daemon start`, then reaches that daemon's Unix control socket
through `codex app-server proxy --sock`, carrying RFC 6455 WebSocket frames over
the SSH channel.

Implementation notes and the audited Codex source boundary are documented in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).
The current Desktop parity matrix and known gaps are tracked in
[`docs/FEATURE_PARITY.md`](docs/FEATURE_PARITY.md).

## Remote host requirements

- A POSIX host (Linux or macOS) with OpenSSH access using a password or
  PEM/OpenSSH private key.
- Codex installed with the standalone installer from
  <https://chatgpt.com/codex/install.sh>, so `codex` is available from the remote
  login shell in the layout the app-server daemon manages
  (`$CODEX_HOME/packages/standalone/current/codex`). npm-installed Codex and
  Windows remotes are not supported.
- A Codex account on the remote host. Existing API-key/ChatGPT auth is reused;
  when required, the app can start Codex's remote ChatGPT device-code login.
- A trusted, least-privilege remote user.

The remote app-server is a shared, persistent daemon. `codex app-server daemon
start` runs on every connect and is idempotent; Android never stops, restarts or
bootstraps it, and remote turns keep running while the phone is disconnected.
The app connects to the daemon's `0600` Unix control socket through
`codex app-server proxy`; SSH is the trust boundary and no TCP app-server
listener is exposed.

No project path is configured on Android. Existing projects and conversations
come from the remote Codex history returned by `thread/list`.
The first page is shown as soon as the connection is ready; remaining history
loads in the background and is merged into the project list. The sidebar shows
loading progress and offers a retry if a later page fails, keeping already
loaded conversations available.
Opening a conversation resumes its live app-server subscription while loading
only the latest five full turns; older turns are fetched as the chat is scrolled
to the top, with a legacy full-history fallback for older Codex hosts.

Connect enables foreground connection maintenance by default. The connection
belongs to the app process, so Activity rotation or removal keeps SSH open.
The notification and the in-app maintenance bar both offer Disconnect; deleting
the active host also stops maintenance. Notification permission denial does not
prevent connection maintenance: use the in-app Disconnect control if Android
hides the notification.

Automatic recovery pauses without a default network or during Doze, and stops
after ten failed dials with capped exponential backoff. Running tasks and pending
approvals defer proactive network handoff; genuine transport loss restores the
connection and cached task subscriptions. It never retries messages, commands,
reviews, forks, or approval responses. Approval delivery uncertainty requires
manual checking on the server. Authentication, host-key, installation, protocol,
and capacity failures require manual Connect after correcting the cause.

Only the desired saved connection ID is persisted for system service recreation.
Drafts and full-access grants remain in memory and are scoped to the exact saved
host, username, port, pinned fingerprint and task. A new process restores safe
permissions. Android may delay or prevent recreation after killing the process;
force-stop requires opening the app and connecting again.

The model and reasoning pickers are also remote data. They are loaded from
`model/list`, so the choices follow the Codex version and account configured on
that SSH host rather than a hard-coded Android catalog.

The composer uses the same remote app-server surfaces for Plan mode, service
tiers, permission profiles, image input, running-turn steering, Goals, context
compaction, forks, code review, MCP status, remote skills and installed plugins.
Task pins are stored on the remote Codex thread rather than only on Android.

## Install

Download the signed APK from the latest
[GitHub release](https://github.com/ffff2004/codex-remote-android/releases/latest).
Android may ask you to allow installation from your browser or file manager.

The project is an unofficial community client. It is not affiliated with or
endorsed by OpenAI. Codex and OpenAI are trademarks of their respective owner.

## Build

Open this directory in Android Studio, or run `./gradlew assembleDebug` with
JDK 17 and Android SDK 35 installed.

Run the injected UI/session/owner regression suite on a connected emulator or Android device:

```text
./gradlew connectedDebugAndroidTest -Pandroid.testInstrumentationRunnerArguments.class=com.codex.remote.ConnectionHistoryDeviceTest,com.codex.remote.TaskSessionsDeviceTest,com.codex.remote.WorkspaceDeviceTest,com.codex.remote.ConnectionRecoveryDeviceTest
```

The full connected suite also includes `SshRecoveryDeviceTest`, which requires
configured loopback SSH fixtures. See the device validation recipe in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md#device-validation).

The first SSH handshake asks you to verify its SHA-256 host-key fingerprint
before any password or private key is sent. Subsequent key changes are blocked
until the saved fingerprint is explicitly cleared by editing the connection.

To produce a signed release build, provide these environment variables before
running `./gradlew assembleRelease`:

```text
CODEX_REMOTE_KEYSTORE_PATH
CODEX_REMOTE_KEYSTORE_PASSWORD
CODEX_REMOTE_KEY_ALIAS
CODEX_REMOTE_KEY_PASSWORD
```

Signing material must remain outside the repository. APKs, keystores, local SDK
configuration, build caches, and QA captures are excluded by `.gitignore`.

## Release

Pushing a version tag triggers [Android Release](.github/workflows/release.yml).
The workflow checks that the tag matches `versionName`, runs unit tests and lint,
builds a signed APK, and verifies its signing certificate. It then creates a
draft GitHub Release, uploads the APK and `SHA256SUMS`, and publishes the release
only after both uploads succeed. Tags with a suffix such as `v0.1.3-rc.1` create
pre-releases; the APK's `versionName` must also include that suffix.

### One-time signing setup

Reuse the keystore used by `~/.local/sbin/codex-remote-build release` so existing
installations can upgrade. In the publishing repository's **Settings → Secrets
and variables → Actions**, configure these repository secrets:

| Secret | Value |
| --- | --- |
| `CODEX_REMOTE_KEYSTORE_BASE64` | Base64-encoded existing `release.jks` |
| `CODEX_REMOTE_KEYSTORE_PASSWORD` | Existing keystore password |
| `CODEX_REMOTE_KEY_ALIAS` | Existing signing key alias |
| `CODEX_REMOTE_KEY_PASSWORD` | Existing key password |

For example, upload the keystore without printing its contents:

```bash
base64 -w 0 ~/.local/share/codex-remote-keys/release.jks |
  gh secret set CODEX_REMOTE_KEYSTORE_BASE64 --repo ffff2004/codex-remote-android
```

Use the GitHub settings page or `gh secret set NAME --repo
ffff2004/codex-remote-android` to enter the other secrets interactively. The local
build script uses the same password for the keystore and key; set both password
secrets to that value if using the same signing material.

Also configure the repository **variable** `CODEX_REMOTE_SIGNING_CERT_SHA256`:
the existing APK signer's certificate SHA-256 digest, as 64 hexadecimal
characters without colons. Obtain it from a locally signed APK:

```bash
~/.local/sbin/codex-remote-build release
~/Android/Sdk/build-tools/35.0.0/apksigner verify --print-certs \
  app/build/outputs/apk/release/app-release.apk
```

Copy the value on the `Signer #1 certificate SHA-256 digest:` line. This is the
certificate fingerprint, not the APK file checksum. Signing material stays in
the runner's temporary directory and is removed even if a later step fails.
Release uploads use `GITHUB_TOKEN` with `contents: write`; no extra PAT is needed.

### Publish a version

1. Update `versionName` and increment `versionCode` in `app/build.gradle.kts`.
   Commit and push the change, including the release workflow on first setup,
   and wait for Android CI to pass.
2. Tag that commit and push the tag. For example, after setting `versionName` to
   `0.1.3` and `versionCode` to `4`:

   ```bash
   git switch main
   git pull --ff-only
   git tag -a v0.1.3 -m "Release v0.1.3"
   git push origin v0.1.3
   ```

3. Follow Android Release in the Actions tab. The resulting release contains
   `codex-remote-v0.1.3.apk` and `SHA256SUMS`.

If a test, build, or signature check fails, no release is created. If uploading
fails, the release remains a draft; fix the external cause and rerun the failed
workflow. An existing draft is reused and its assets can be replaced. Already
published releases are never overwritten. A rerun uses the original tagged
code: source or workflow fixes need a new commit and version tag. Each new
installable version, including a pre-release, should increment `versionCode`.

To check the publishing logic locally without contacting GitHub:

```bash
bash .github/scripts/test-publish-release.sh
```

## License

Licensed under the [Apache License 2.0](LICENSE).
