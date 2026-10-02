# Issue #3: confirmed LNTP workflow adoption

Tracker: https://github.com/ffff2004/codex-remote-android/issues/3

The following is the accepted issue specification. Unchecked criteria describe the full delivery scope, not a claim that all three implementation stages are complete.

# 目标

基于当前 main `fc7e696e3ce1b02af50c10643ec66ca6de05c8de`，吸收 `lntp-k/codex-remote-android` main `4c7b89a6418920067cb8ca2e2ede05f094cd0e0f` 的全部六项工作流改进：审批队列与完整审查、多会话状态与事件路由、resume 事件顺序与消息身份、后台连接与网络恢复、task 级权限范围，以及这些能力的行为回归测试。

保留本仓库 #1 的单一 managed daemon/proxy runtime 和 #2 的首屏渐进历史加载。交付包括实现、文档、PR，以及 Standards / Spec 两轴 review。

## 必须保留的行为

- 每次连接 `daemon start`，校验 lifecycle JSON，使用绝对且 shell-quoted 的 socketPath，`proxy --sock`，WebSocket-over-stdio。
- 不引入 JSONL stdio fallback、平台选择、独立版本预检或隐式 daemon bootstrap/stop/restart。
- AppServerSession 自身永不自动重连；自动恢复由上层创建新的 session。
- 首个 thread/list 页面完成即可 CONNECTED；后续页面后台合并、去重、排序；失败保留已加载内容并可重试。
- 旧连接结果不能污染新连接；保留部分列表导入期间的 rename/pin/archive 等本地修改与当前选择。

## 功能与验收标准

### 1. 审批队列、归属和完整审查

- [ ] 连续到达的审批不会互相覆盖；回复 A 期间到达 B、迟到的 A completion 均不清除 B。
- [ ] 区分 string/number RPC ID；绑定 connection epoch、request、thread、turn/item；重复请求不导致重复批准。
- [ ] 后台 task 审批按到达顺序呈现，显示准确 owner；Allow 要求选中确切 owner，无法确认归属时不能授权。
- [ ] 根据服务端支持的 decisions 显示可选授权；保留结构化 user-input 的 description / Other 语义并校验完整回答。
- [ ] 文件审批冻结可审查的 command/cwd/diff/rename/scope 快照；信息缺失、不完整或超过预算时不得 Allow。
- [ ] response 发送状态和失败/交付不确定性明确；不自动重发审批。

### 2. 多会话状态与事件路由

- [ ] 同一个 SSH host 的 A/B task 有独立 timeline/history/goal/运行状态/draft/settings/approval/unread；切去 B 时 A 仍接收后台事件。
- [ ] sidebar 显示 running / approval / failed / unread；准确 thread+turn+item 路由，未知或过期 owner 不猜成当前 task。
- [ ] 覆盖 notification-before-response、completion-before-response、旧 turn completion，以及 task 切换后的迟到事件。
- [ ] 有界 retention 和原子容量判断；selected/running/approval/active-goal 受保护，eligible inactive session 才可淘汰。
- [ ] 渐进列表中的“未出现在当前页”不视为删除；目录导入不覆盖正在运行或已 resume 的 task 状态。

### 3. Resume 顺序与跨客户端消息身份

- [ ] resume snapshot 期间按 connection/task epoch 缓冲事件；安装 snapshot 后按 wire order drain，RPC reader 不因 resume 锁而阻塞。
- [ ] 切换、归档、fork、断开或重连使旧 resume 失效；队列溢出明确失败，不静默丢事件。
- [ ] 使用 clientUserMessageId / server item clientId 对齐 optimistic message；Desktop 与手机发送相同文本不会合错。
- [ ] client ID 不被当作请求重放许可；不自动重发 turn/start、steer 或其他写操作。

### 4. 后台连接与网络恢复

- [ ] 进程级 connection owner；Activity 旋转/移除不自动销毁 SSH；foreground service 和通知 Disconnect 使用同一个连接 owner。
- [ ] 网络 handoff、Doze、退避重试、实际拨号预算有明确策略；stale callbacks 不干扰新连接。
- [ ] running turn / approval 期间避免主动 handoff；其他 pending RPC 有有限 grace；真实断线后只恢复连接与 authoritative subscription/read。
- [ ] 手动 Disconnect、删除连接停止 maintenance 并清除目标；不重放消息、命令、review、fork 或审批。
- [ ] 重连继续保留首屏渐进加载，不退回全量列表阻塞。

### 5. Task 级授权

- [ ] Full access 明确确认，按准确 host+thread 生效；确认时再次校验选择，避免对话框打开后切任务造成授权错位。
- [ ] 运行中更改提示下个 turn 生效；新/未缓存 task 默认安全授权，不继承其他 task 的 broad permission。
- [ ] 不自动持久化并恢复 full-access grants；进程重建不能无明确用户授权重新授予宽权限。

## 已确认的设计与默认策略

用户已确认采用设计 A：适配 fork 的纯 session/approval/recovery modules，约束可变状态入口；保留当前 AppServerRuntime/AppServerSession、首屏 bootstrap 与 UI projection，避免完整 AppViewModel/transport cherry-pick。

未选方案 B：重构为 WorkspaceSessions + ConnectionCoordinator 两个 deep modules，进一步减少 ViewModel orchestration，但会扩大本次 UI 与调用路径迁移范围。

用户已确认的默认策略：

- Connect 后默认启用 foreground maintenance；通知 Disconnect 即停止。
- 进程重建只恢复期望连接与远端状态；不持久化 credential/draft/full-access grants。系统允许时恢复连接，不承诺系统杀进程后立即恢复。
- 10 次失败实际拨号上限、最大 30 秒退避；网络变化或手动 Connect 可重新开始预算；Doze 暂停不消耗预算。
- 采用 fork 的有界 session/resume 起始预算，原子拒绝和可见错误；不能安全保留 authoritative state 时结束该连接并要求人工处理，避免自动恢复循环。
- 同一进程中的权限按准确 host+thread 隔离；新 task/host 或进程重建默认安全权限。

## 已确认的实现与 review 安排

三个 worker 严格串行，后者基于前者的已提交结果：

1. 审批 domain/RPC/queue/完整审查与 UI，行为 tests。
2. session registry/routing、resume/client ID、task settings 与 UI projection，保留渐进列表，行为 tests。
3. Application ownership、foreground service/recovery、整体验证、文档与设备验证配方。

每阶段使用内建 sub-agent worker 建立可构建提交，禁止回退他人改动。整体完成后执行 gates、确认 clean，再通过 `codexctl-as-subagent --approve-for-me` 启动两个独立 reviewer，分别检查 Standards / Spec；后续轮次复用同一 reviewer thread。reviewer 只读审查，不修改代码或 refs，尽管运行模式使用用户指定的 approve-for-me。

每轮 review 的完整 diff 区间（基线 SHA、当轮 HEAD SHA 和 diff 命令）、复用 reviewer 的增量区间、已执行 gates、两个轴各自的结论和 findings 处理情况均作为 PR 评论发布。默认最多两轮 implement-review；第二轮未通过时报告并等待确认。

分支：`feat/3-lntp-task-supervision`。用户已授权继续设计、实现、创建 PR 和 review；PR 合并不在本任务范围内。

## 验证

- [ ] `JAVA_HOME=$HOME/.local/share/nix-jdk17 sh ./gradlew testDebugUnitTest lintDebug assembleDebug assembleDebugAndroidTest`。
- [ ] 保留并通过现有 daemon lifecycle / shell compatibility / WebSocket / session 与渐进 thread/list 回归测试。
- [ ] 新增真实行为 tests：审批竞争、后台 task events、容量原子性、resume races、相同文本不同 client IDs、网络/Doze/retry budget、never replay。
- [ ] 使用本机 Waydroid（已确认运行且 ADB 可连接，Android 13 / API 33 / x86_64）执行 connectedDebugAndroidTest；验证审批、多 task、resume/client ID、首屏加载、通知/Disconnect、Activity 重建与后台连接。
- [ ] 配合本机 SSH fixture / real_ssh_codex_bridge.py 与真实 Codex daemon 做协议与同步验证；模拟网络中断和 Doze，记录哪些是注入/模拟场景。
- [ ] 明确 Waydroid 验证边界：不覆盖真实蜂窝/Wi-Fi/VPN handoff、厂商省电/后台限制、真实无线休眠，以及 API 34/35+ 的 foreground service 行为；这些项目未实测时在 PR 中保留缺口。
- [ ] PR 描述说明最终行为、验证结果、真机验证缺口和兼容性限制。

## 非目标

不整体 merge fork；不移植 Android 16/AGP/Kotlin/Gradle 升级、release/signing 改动或日志自动导出。不实现多 host supervisor、dashboard/grouped inbox、terminal、worktree、完整进程重建 drafts、持久授权或自建远端 daemon runtime。不在本 ticket 合并 PR。

## 参考

- #1 / #2。
- https://github.com/lntp-k/codex-remote-android/commit/c2b5e60 （审批/RPC 加固）
- https://github.com/lntp-k/codex-remote-android/commit/d9cd9f1 （多 session）
- https://github.com/lntp-k/codex-remote-android/commit/a330b01 （后台连接/网络恢复）
- https://github.com/lntp-k/codex-remote-android/commit/1908c07 （resume/live sync/task full access）

## Stage 1 implementation contract

- `domain/Approvals.kt` owns FIFO queue identity and complete review/answer/owner validation. `ApprovalQueueKey.queueInstanceId` is a connection epoch; keys also contain sequence, typed RPC ID, thread, turn and item. Always use the captured key, never whichever request is currently displayed.
- `rpc/ApprovalParsing.kt` validates known authorization semantics. Explicitly inert top-level metadata (`metadata`, `traceId`, `requestTimestamp`) may be additive. Unknown permission, policy, file operation or decision semantics remain deny-only.
- `rpc/OutstandingApprovalRequests.kt` suppresses exact replay, remembers resolved IDs for the connection lifetime, and rejects conflicting ownership/context. No approval response is retried after an uncertain send. Tracking exhaustion closes the connection instead of evicting authoritative identities.
- RPC dispatch compares the review with its own frozen authoritative record. UI copies cannot replace command, cwd, permissions, choices or diffs. V2 file review only binds an exact live thread/turn/item, never conversation history. Relative file review also requires a frozen cwd supplied by the request or an authoritative read of its exact owner; the bounded RPC cwd cache holds 256 owners. `ApprovalReviewUpdated` installs a newly validated cwd without changing queue identity. A changed, incomplete or completed live item invalidates pending authorization. Legacy file approvals carry complete file details in the request.
- Initial budgets: 64 queued approvals / 4 MiB retained queue; 256 tracked request IDs/tombstones / 1 MiB identity storage / 4 MiB payload; individual approval envelope 512 KiB; at most 3 questions and 32 options each. Complete review budgets: 200 file targets, 200 diff lines, 32,768 diff characters, 4,096 characters per target, 32,768 target characters total; other displayed authorization context at most 200 lines and 32,768 characters. Live snapshot caches are bounded to 200 items / 4 MiB each. Exceeding a display budget disables Allow; queue/protocol capacity exhaustion disconnects.
- File session grants require a nonblank explicit grantRoot; without it only a complete once patch can be accepted. Successful local send means sent, not server-confirmed delivery. Failed/uncertain responses stay disabled with a visible explanation and acknowledgement. A request without a supported denial response exposes Disconnect, never an invented RPC decision.
- Stage 2 must integrate `approvalQueue`/`approvalFileItems` with session routing without substituting resumed timeline data for live review, and preserve the exact key/owner check at dispatch. Stage 3 must preserve queue epoch invalidation and never replay approvals on recovery.

Approval parser/queue/tracker regression tests selectively adapt the reference fork tests. Runtime and progressive pagination tests remain the original repository gates. Additional public-client tests cover lossless numeric/string routing and validated dispatch. Device approval tests use injected Compose state; they do not claim end-to-end real daemon approval validation.

## Stage 1 validation (2026-10-02)

- Required JDK17 command passed: `sh ./gradlew testDebugUnitTest lintDebug assembleDebug assembleDebugAndroidTest` (169 JVM tests). Existing daemon lifecycle, shell compatibility, WebSocket/session and progressive pagination regressions remain passing.
- Waydroid Android13/API33/x86_64: `connectedDebugAndroidTest` with the three new WorkspaceDeviceTest approval methods passed, 3 tests / 0 failures. Methods: `backgroundApprovalShowsExactOwnerAndOnlyAllowsDeny`, `approvalACompletionKeepsBVisibleWithItsOwnCallbackIdentity`, `missingFileReviewCannotBeAuthorizedDespiteConversationChanges`. Run with `ANDROID_SERIAL=192.168.240.112:5555` and instrumentation `class` filter listing those methods.
- These are injected UI tests plus in-memory public RPC-reader tests. Stage 1 did not run real SSH/daemon approval fixtures or unrelated credential-dependent instrumentation suites. Stage 3 owns real protocol/synchronization and lifecycle/recovery verification and the device limitations listed above.
