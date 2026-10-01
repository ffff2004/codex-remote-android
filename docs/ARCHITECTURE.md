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
`--remote-socket` mode and no automatic reconnect: on connection loss the client
surfaces a failure and waits for a manual reconnect while the remote daemon keeps
running.

Current SSH connection setup supports direct password and private-key hosts.
OpenSSH config expansion, ProxyJump, hardware-backed SSH agents and managed
Remote Control relay pairing are not implemented.

SSHJ's `curve25519` key-exchange factories are excluded on Android because the
platform JCA does not expose the `X25519` key-pair generator expected by SSHJ.
The client retains the interoperable ECDH and DH group14 families rather than
failing before host-key verification.
