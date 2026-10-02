# Architecture

## Source audit

This client follows the public Codex app-server contract rather than embedding
an agent on Android.

- Codex source inspected at `openai/codex` commit
  `61a44880a85d2fd0d8770908dea5733495e571c8` (2026-07-26).
- Protocol definitions come from `codex-rs/app-server-protocol`.
- Server behavior comes from `codex-rs/app-server`.
- Desktop behavior and command registration were audited from Codex Desktop
  `26.721.4979.0`; Android keeps only actions that can operate on the SSH host.
- `codex-rs/app-server-daemon/README.md` explicitly describes SSH-launched
  app-server instances used by desktop and mobile remote clients.
- `codex-rs/app-server-protocol/src/protocol/v2/thread.rs` defines host-wide
  `thread/list` discovery, `cwd` filtering and opaque cursor pagination.
- `codex-rs/app-server/tests/suite/v2/thread_list.rs` verifies `nextCursor`
  pagination and shows that `cwd: None` does not restrict results to one project.
- Desktop sends an empty `sourceKinds` filter for its user-facing thread list;
  Android mirrors that behavior and follows every `nextCursor` page.

## Runtime flow

```text
Android UI
   |
   | SSH handshake (verify/pin SHA-256 host key)
   v
Remote login shell
   |
   | codex app-server daemon start        (idempotent, one lifecycle JSON line)
   v
socketPath: 0600 Unix control socket (no TCP listener)
   |
   | codex app-server proxy --sock <socketPath>
   v
WebSocket-over-stdio on the SSH channel (RFC 6455)
   |
   +-- initialize / initialized
   +-- thread/list (all cursor pages, no cwd filter)
   +-- thread/start, thread/resume, thread/fork, thread/compact/start
   +-- thread/archive, thread/unarchive, thread/delete
   +-- thread/goal/get, thread/goal/set, thread/goal/clear
   +-- thread/metadata/update (remote pin state)
   +-- turn/start, turn/steer, turn/interrupt
   +-- review/start, collaborationMode/list
   +-- account/read, account/login/start
   +-- model/list (all cursor pages)
   +-- skills/list, plugin/installed, mcpServerStatus/list
   +-- mcpServer/oauth/login, config/mcpServer/reload
   +-- feedback/upload (remote thread id and remote logs)
   +-- item and turn streaming notifications
   +-- command, file-change, permission and user-input approvals
```

`daemon start` is invoked on every connect and is idempotent. The lifecycle line
is parsed strictly for the `status` and absolute POSIX `socketPath` that the
proxy command needs, and tolerantly for additive fields. The daemon lifecycle
JSON contract is experimental and may change; the client ignores unknown fields
so additive changes are safe. The socket path is POSIX-shell-quoted before it is
embedded in the remote command, and the client never interpolates raw remote
JSON into a shell string.

The proxied stdio stream is an RFC 6455 WebSocket: the client sends an HTTP
`GET ws://localhost/` Upgrade with `Sec-WebSocket-Key` and
`Sec-WebSocket-Version: 13`, validates `101 Switching Protocols` plus
`Sec-WebSocket-Accept`, then exchanges JSON-RPC 2.0 messages (with the `jsonrpc`
header omitted) as WebSocket text frames. Client frames are masked; server frames
are unmasked. No compression and no subprotocol are requested.

Agent execution, repository access, authentication, tools and approvals remain
owned by the remote Codex installation, and the daemon is shared and persistent:
turns keep running while Android is disconnected. Android has no local agent
runtime and never stops or restarts the daemon implicitly.

## Host-wide discovery

A saved connection stores only SSH host and authentication details.
On connection, the client requests every page of non-archived interactive
threads with the same empty `sourceKinds` filter as Desktop. It deliberately
omits the optional `cwd` field from `thread/list`, follows `nextCursor` until it
is null, and deduplicates thread IDs across overlapping pages.

Projects are a UI projection of the returned threads grouped by normalized
`Thread.cwd`; they are not stored in the SSH connection. Resuming a conversation
uses that thread's own `cwd`. Starting a conversation uses the selected imported
project's path.

Connection readiness requires protocol initialization, account/model data,
initial collaboration/permission options and the first thread page. The client
publishes that page immediately, then follows
the remaining cursors in the background using the same filesystem-backed query
to preserve history completeness. Each accumulated page is deduplicated and
merged without invalidating the current conversation. Later-page failures keep
the connection usable and expose a separate sidebar retry. Thread refreshes
coalesce during an active listing; local edits and archive tombstones remain
authoritative for that pass. Disconnecting or changing hosts cancels the old
listing and prevents its results from updating the new connection.

The model picker is populated exclusively from the remote `model/list` catalog,
including each model's reasoning choices, input modalities, service tiers and
defaults. Plan mode comes from `collaborationMode/list`; permissions come from
`permissionProfile/list`. Text, structured skill/plugin mentions and image data
are sent as official `UserInput` objects. Follow-up messages sent while a turn
is active use `turn/steer` with the active `expectedTurnId`.

Existing remote API-key and ChatGPT accounts are read through `account/read`.
When the remote host requires ChatGPT authentication, the Android client starts
the official device-code flow through `account/login/start`; credentials remain
owned by the remote Codex installation.

Archived conversations are fetched on demand with the same host-wide,
cursor-paginated `thread/list` query and `archived: true`. Restoring and
permanently deleting them use `thread/unarchive` and `thread/delete`; deletion
is always guarded by a confirmation dialog. Context compaction is available
both as `/compact` and as an explicit current-task menu action.

The MCP status view can reload the remote MCP configuration and start the same
OAuth request used by Desktop. Android opens the returned authorization URL in
the system browser and waits for `mcpServer/oauthLogin/completed`; SSH hosts
whose OAuth provider redirects to loopback must configure a reachable
`mcp_oauth_callback_url` or an SSH tunnel on the remote host.

## Security model

- Passwords, private keys and passphrases are encrypted with an AES-GCM key
  generated inside Android Keystore.
- Android backup and device transfer are disabled for all app data domains.
- A new SSH host is rejected before authentication and its SHA-256 fingerprint
  is shown for explicit confirmation. A changed key is always blocked.
- The app-server daemon's control socket is a `0600` Unix domain socket with no
  TCP listener. The client reaches it only through `codex app-server proxy` on
  the SSH channel, so SSH remains the trust boundary.
- The remote thread starts with `workspace-write` sandboxing and `on-request`
  approval by default. Named permission profiles are loaded from the host.
  Explicit full access maps to the app-server `dangerFullAccess` policy.

## Compatibility boundary

The app uses stable app-server methods and tolerant JSON parsing. Unknown item
types are ignored, while unknown server-initiated requests are surfaced rather
than automatically approved. Because schemas are tied to the installed Codex
version, the Android protocol layer should be tested whenever the remote Codex
installation is upgraded across major protocol changes.

The runtime is single-path by design: POSIX remote hosts running a Codex
installation created by `install.sh`. Windows remotes, npm-installed Codex, and
the removed JSONL stdio transport are unsupported. There is no external
`--remote-socket` mode. `AppServerSession` never reconnects itself; the process
owner may create a fresh session under the recovery policy described below while
the shared remote daemon keeps running.

Current SSH connection setup supports direct password and private-key hosts.
OpenSSH config expansion, ProxyJump, hardware-backed SSH agents and managed
Remote Control relay pairing are not implemented.

SSHJ's `curve25519` key-exchange factories are excluded on Android because the
platform JCA does not expose the `X25519` key-pair generator expected by SSHJ.
The client retains the interoperable ECDH and DH group14 families rather than
failing before host-key verification.

## Task supervision and approval ownership

A bounded `SessionRegistry` retains independent task timelines, goals, drafts,
settings, running state and unread indicators. Events require exact thread/turn/item
ownership. Selected, running, pending-write, approval and unfinished-goal owners
are protected from eviction. Partial thread pages never imply deletion. Capacity
failure closes the transport and blocks automatic recovery until manual Connect.

Resume buffers notifications by connection/task epoch and drains in wire order
only after installing the current snapshot. Late snapshots cannot replace a new
selection or newer live state. Client user-message IDs reconcile phone messages
with their server items; equal Desktop/phone text alone never merges messages.
Client IDs do not permit replay. Full-access confirmation rechecks the captured
exact trusted host and task; new tasks and new processes use safe permissions.

Approvals use a global FIFO queue with typed RPC IDs and captured owner/epoch
keys. Command/cwd/diff/rename/scope context is frozen and bounded for complete
review. Missing or unknown authorization semantics disable Allow. The RPC client
checks the frozen authoritative record again at dispatch. Resume history is never
live approval evidence. Failed or uncertain response delivery is never retried.
A recovered connection invalidates old RPC identities and keeps a visible manual
handling warning for pending or uncertain approvals.

## Process ownership and recovery

`CodexRemoteApplication` owns one `ProcessConnectionOwner` and `AppViewModel`.
`MainActivity` only renders this process state. `SshConnectionService` starts its
foreground notification in `onCreate`, before SSH/authentication; it forwards
default-network and Doze callbacks to that same owner and handles notification
Disconnect. It has no independent SSH client or retry loop. The SDK35 manifest
uses `specialUse` with its declared subtype and foreground-service permissions.
Denied API33 notification permission leaves foreground maintenance available;
Android may hide its notification, so the app also exposes Disconnect and status.

Maintenance stores only the desired saved connection ID. Manual Disconnect or
active-host deletion synchronously clears that intent before stopping the service.
Sticky recreation can resolve the ID through the existing encrypted connection
store and restore remote state, without restoring drafts or grants. Activity
creation alone does not auto-connect a last-used host after a manual disconnect.
System recreation is best effort, with no immediate-restart or force-stop guarantee.

The pure recovery policy tracks actual default-network identity and ignores stale
loss callbacks. Its first availability callback establishes a baseline without
cancelling the initial dial. Doze/offline observations do not consume the ten
failed-dial budget. Retry delays are 1/2/4/8/16 seconds, then 30 seconds; exhaustion
keeps the monitor visible and leaves Connect/Disconnect under user control. A
manual Connect or changed default network resets the transient budget. Fatal
host-key/auth/install/protocol/capacity failures remain blocked across callbacks
until explicit manual action.

Proactive handoff waits while any cached task is running or any approval is pending.
Other pending RPCs receive five seconds of grace. Genuine transport loss bypasses
that deferral, captures selected and retained task IDs before closing, then opens
a fresh session. The first thread page still publishes CONNECTED immediately;
protected cached metadata and the selected task remain visible. Read-only recovery
resumes selected first and then every other retained subscription sequentially,
without changing selection or replaying any write. Cache preservation requires an
exact saved ID, username, host, port and pinned fingerprint match, including edits
saved under the same connection ID.

Production SSH setup has a 45-second deadline. Dial cancellation and operation
deadlines close the TCP socket before waiting for blocking IO to unwind; a new
attempt joins the old one. Stream close never waits behind a stalled WebSocket
write lock. Once a WebSocket frame starts, caller cancellation waits for that
frame within its 15-second deadline; cancelling an obsolete catalog read cannot
truncate a frame and close the shared session. Waiting for the write mutex remains
cancellable, and owner Disconnect still closes the socket immediately.
RPC responses have a 60-second upper bound.
Maintenance health is a 10-second authoritative read every 30 seconds, skipped in
Doze/offline and while another RPC is pending. Cached resubscription has a
30-second deadline per owner. No health check sends a turn or approval response.

## Device validation

`ConnectionHistoryDeviceTest`, `TaskSessionsDeviceTest`, `WorkspaceDeviceTest`,
and `ConnectionRecoveryDeviceTest` use injected app-server sessions or UI state.
They cover progressive pages, exact event routing, approval races, task grants,
resume ordering, finite recovery, cancellation, trust-context edits, deletion and
ID-only restoration. They are not physical network-handoff proof.

For production SSH testing, run the fixture with a temporary strong password and
pinned host key, binding only loopback. Use `adb reverse` for both its SSH and
optional forced-transport-loss control port:

```sh
uv run --with paramiko tools/mock_ssh_app_server.py --host 127.0.0.1 --port 22222 \
  --control-port 22224 --history-page-delay 2 --username android-fixture \
  --password "$FIXTURE_PASSWORD" --state-dir /tmp/codex-ssh-fixture
adb reverse tcp:22222 tcp:22222
adb reverse tcp:22224 tcp:22224
./gradlew connectedDebugAndroidTest \
  -Pandroid.testInstrumentationRunnerArguments.sshFixture=mock \
  -Pandroid.testInstrumentationRunnerArguments.sshPort=22222 \
  -Pandroid.testInstrumentationRunnerArguments.sshControlPort=22224 \
  -Pandroid.testInstrumentationRunnerArguments.sshUsername=android-fixture \
  -Pandroid.testInstrumentationRunnerArguments.sshPassword="$FIXTURE_PASSWORD" \
  -Pandroid.testInstrumentationRunnerArguments.sshFingerprint="$FIXTURE_FINGERPRINT"
```

Read the fingerprint from the fixture's `ready` event. Omit a `class` filter to
run the full connected suite. The two SSH tests verify delayed first-page readiness,
full history, live items, exact-owner approval denial, forced transport recovery, Activity removal/recreation,
foreground notification Disconnect and Activity rotation. For actual API33+ denial,
create a private JSON config containing the same `ssh*` arguments, build both debug
APKs, and run:

```sh
uv run tools/run_ssh_permission_test.py --serial "$ANDROID_SERIAL" --config /tmp/private-ssh-test.json
```

This runner installs the APKs, denies POST_NOTIFICATIONS before instrumentation,
sets a temporary user-fixed flag to avoid a permission prompt obscuring Activity
lifecycle, verifies foreground maintenance and in-app Disconnect, then restores
the prior grant and user-fixed state. Changing this permission from a running
instrumentation test can kill that process; AppOps alone does not establish an
API33 runtime-permission denial. The control
endpoint only closes fixture-owned SSH transports; it never stops the daemon.
Restore device permission/AppOps settings and remove only the reverse ports used
by the fixture after testing. Keep fixture credentials/logs outside the repo.

`tools/real_ssh_codex_bridge.py` replaces the double with the installed managed
Codex daemon/proxy and audits client RPC method names without recording prompt
contents. Use `sshFixture=real`, a small dedicated test thread's `sshThreadId`, and
a loopback `sshEventPort` endpoint responding to `EVENT` with `OK` after a second
genuine daemon client starts a test turn on that thread. This external
client supplies real live-event evidence after Android resubscription. The real
SSH fixture test also forces the Android platform into Doze and restores it with
`unforce`, verifying the actual idle broadcast/status without claiming physical
radio suspension. Android sends no writes in real mode. Archive the dedicated thread afterward; the shared
daemon remains running.

Waydroid Android13/API33 covers this foreground-service and SSH path, not actual
cellular/Wi-Fi/VPN handoff, vendor power restrictions, physical wireless sleep, or
API34/35+ foreground-service enforcement. Policy injection does not establish
physical handoff reliability. Process-owner reconstruction tests do not prove
Android will restart a killed or force-stopped app on any particular device.
