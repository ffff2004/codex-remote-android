package com.codex.remote.data.rpc

import com.codex.remote.domain.*
import kotlinx.serialization.json.*


internal const val MAX_APPROVAL_MESSAGE_CHARS = 512L * 1024L
internal const val MAX_APPROVAL_QUESTIONS = 3
internal const val MAX_APPROVAL_OPTIONS_PER_QUESTION = 32
internal const val MAX_TRACKED_APPROVAL_REQUESTS = 256
internal const val MAX_TRACKED_APPROVAL_ID_CHARS = 1L * 1024L * 1024L
internal const val MAX_TRACKED_APPROVAL_RETAINED_CHARS = 4L * 1024L * 1024L

private val NEW_COMMAND_APPROVAL_KEYS = setOf(
    "additionalPermissions",
    "approvalId",
    "availableDecisions",
    "command",
    "commandActions",
    "cwd",
    "environmentId",
    "itemId",
    "networkApprovalContext",
    "proposedExecpolicyAmendment",
    "proposedNetworkPolicyAmendments",
    "reason",
    "startedAtMs",
    "threadId",
    "turnId",
)

private val LEGACY_COMMAND_APPROVAL_KEYS = setOf(
    "approvalId",
    "callId",
    "command",
    "conversationId",
    "cwd",
    "parsedCmd",
    "reason",
)

private val NEW_FILE_APPROVAL_KEYS = setOf(
    "availableDecisions",
    "cwd",
    "grantRoot",
    "itemId",
    "reason",
    "startedAtMs",
    "threadId",
    "turnId",
)

private val LEGACY_FILE_APPROVAL_KEYS = setOf(
    "cwd",
    "callId",
    "conversationId",
    "fileChanges",
    "grantRoot",
    "reason",
)

private val PERMISSION_APPROVAL_KEYS = setOf(
    "availableDecisions",
    "cwd",
    "environmentId",
    "itemId",
    "permissions",
    "reason",
    "startedAtMs",
    "threadId",
    "turnId",
)

private val USER_INPUT_REQUEST_KEYS = setOf(
    "autoResolutionMs",
    "isBlocking",
    "itemId",
    "questions",
    "threadId",
    "turnId",
)

private val SUPPORTED_APPROVAL_DECISIONS = setOf("accept", "acceptForSession", "decline", "cancel")

internal fun parseCommandApprovalRequest(
    id: RpcRequestId,
    method: String,
    params: JsonObject,
    rawParams: String,
): ApprovalRequest {
    val legacy = method == "execCommandApproval"
    val allowedKeys = if (legacy) LEGACY_COMMAND_APPROVAL_KEYS else NEW_COMMAND_APPROVAL_KEYS
    val unknown = params.unknownFields(allowedKeys)
    val threadId = params.strictNonBlankString(if (legacy) "conversationId" else "threadId")
    val turnId = params.strictNonBlankString("turnId")
    val itemId = params.strictString(if (legacy) "callId" else "itemId")
    val cwd = params.strictString("cwd")
    val command = params["command"].approvalDisplayValue()
    val requiredContextPresent = if (legacy) {
        !threadId.isNullOrBlank() && !itemId.isNullOrBlank() && !cwd.isNullOrBlank() &&
            params["command"].isStringArray() && params["parsedCmd"].isValidLegacyParsedCommands()
    } else {
        !threadId.isNullOrBlank() && !turnId.isNullOrBlank() && !itemId.isNullOrBlank() &&
            (params["startedAtMs"] == null || params["startedAtMs"].isJsonLong()) && !params.strictString("command").isNullOrBlank() &&
            !cwd.isNullOrBlank()
    }
    val permissionsValid = params["additionalPermissions"].isNullOrValidPermissionProfile()
    val availableDecisionsValid = params["availableDecisions"].isNullOrValidAvailableDecisions()
    val commandActionsValid = legacy || params["commandActions"].isNullOrValidCommandActions()
    val networkContextValid = legacy || params["networkApprovalContext"].isNullOrValidNetworkApprovalContext()
    val execPolicyValid = legacy || params["proposedExecpolicyAmendment"].isNullOrStringArray()
    val networkPolicyValid = legacy || params["proposedNetworkPolicyAmendments"].isNullOrValidNetworkPolicyAmendments()
    val securityContextComplete = requiredContextPresent && permissionsValid &&
        availableDecisionsValid && commandActionsValid && networkContextValid && execPolicyValid &&
        networkPolicyValid && params.hasValidOptionalStrings(
            if (legacy) setOf("approvalId", "reason")
            else setOf("approvalId", "command", "cwd", "environmentId", "reason"),
        ) && unknown == null
    val context = buildList {
        add(ApprovalContextField("Working directory", cwd ?: "Not provided"))
        addContext("Reason", params.string("reason"))
        addContext("Thread", threadId)
        addContext("Turn", turnId)
        addContext("Item", itemId)
        addContext("Approval callback", params.string("approvalId"))
        addContext("Started at (ms)", (params["startedAtMs"] as? JsonPrimitive)?.longOrNull?.toString())
        addContext("Environment", params.string("environmentId"))
        addJsonContext("Parsed command actions", params[if (legacy) "parsedCmd" else "commandActions"])
        addJsonContext("Available decisions", params["availableDecisions"])
        addJsonContext("Additional permissions", params["additionalPermissions"])
        addJsonContext("Network request", params["networkApprovalContext"])
        addJsonContext("Exec policy amendment", params["proposedExecpolicyAmendment"])
        addJsonContext("Network policy amendments", params["proposedNetworkPolicyAmendments"])
        addJsonContext("Unrecognized request data", unknown)
        if (!securityContextComplete) {
            add(ApprovalContextField("Validation warning", APPROVAL_VALIDATION_WARNING))
        }
    }
    return ApprovalRequest(
        requestId = id,
        kind = ApprovalKind.COMMAND,
        title = "Allow command execution?",
        detail = command ?: params.string("reason") ?: "Remote Codex requests command execution",
        rawMethod = method,
        rawParams = rawParams,
        threadId = threadId,
        turnId = turnId,
        itemId = itemId,
        approvalId = params.strictString("approvalId"),
        startedAtMs = (params["startedAtMs"] as? JsonPrimitive)?.longOrNull,
        cwd = cwd,
        context = context,
        availableDecisions = params.approvalDecisions(ApprovalKind.COMMAND, securityContextComplete),
        securityContextComplete = securityContextComplete,
    )
}

internal fun parseFileApprovalRequest(
    id: RpcRequestId,
    method: String,
    params: JsonObject,
    rawParams: String,
): ApprovalRequest {
    val legacy = method == "applyPatchApproval"
    val allowedKeys = if (legacy) LEGACY_FILE_APPROVAL_KEYS else NEW_FILE_APPROVAL_KEYS
    val unknown = params.unknownFields(allowedKeys)
    val threadId = params.strictNonBlankString(if (legacy) "conversationId" else "threadId")
    val turnId = params.strictNonBlankString("turnId")
    val itemId = params.strictString(if (legacy) "callId" else "itemId")
    val (fileChanges, fileChangesValid) = if (legacy) params.parseLegacyFileChanges() else emptyList<FileChangeSummary>() to true
    val requiredContextPresent = if (legacy) {
        !threadId.isNullOrBlank() && !itemId.isNullOrBlank() && params["fileChanges"] is JsonObject
    } else {
        !threadId.isNullOrBlank() && !turnId.isNullOrBlank() && !itemId.isNullOrBlank() &&
            (params["startedAtMs"] == null || params["startedAtMs"].isJsonLong())
    }
    val securityContextComplete = requiredContextPresent && fileChangesValid &&
        params.hasValidOptionalStrings(setOf("grantRoot", "reason", "cwd")) &&
        params["availableDecisions"].isNullOrValidAvailableDecisions() && unknown == null
    val context = buildList {
        addContext("Reason", params.string("reason"))
        addContext("Working directory", params.strictString("cwd"))
        addContext("Session write root", params.string("grantRoot"))
        addContext("Approval scope", "Allow once authorizes the listed patch; Allow session grants ongoing writes under the session root.")
        addJsonContext("Available decisions", params["availableDecisions"])
        addContext("Thread", threadId)
        addContext("Turn", turnId)
        addContext("Item", itemId)
        addContext("Started at (ms)", (params["startedAtMs"] as? JsonPrimitive)?.longOrNull?.toString())
        addJsonContext("Unrecognized request data", unknown)
        if (!securityContextComplete) {
            add(ApprovalContextField("Validation warning", APPROVAL_VALIDATION_WARNING))
        }
    }
    return ApprovalRequest(
        requestId = id,
        kind = ApprovalKind.FILE_CHANGE,
        title = "Allow file changes?",
        detail = params.string("reason") ?: params.string("grantRoot") ?: "Remote Codex requests write access to the project",
        rawMethod = method,
        rawParams = rawParams,
        threadId = threadId,
        turnId = turnId,
        itemId = itemId,
        startedAtMs = (params["startedAtMs"] as? JsonPrimitive)?.longOrNull,
        context = context,
        cwd = params.strictString("cwd"),
        fileChanges = fileChanges,
        availableDecisions = params.approvalDecisions(ApprovalKind.FILE_CHANGE, securityContextComplete)
            .filter { it != "acceptForSession" || !params.strictString("grantRoot").isNullOrBlank() },
        securityContextComplete = securityContextComplete,
    )
}

internal fun parsePermissionApprovalRequest(
    id: RpcRequestId,
    method: String,
    params: JsonObject,
    rawParams: String,
): ApprovalRequest {
    val unknown = params.unknownFields(PERMISSION_APPROVAL_KEYS)
    val threadId = params.strictNonBlankString("threadId")
    val turnId = params.strictNonBlankString("turnId")
    val itemId = params.strictString("itemId")
    val cwd = params.strictString("cwd")
    val permissions = params["permissions"]
    val securityContextComplete = !threadId.isNullOrBlank() && !turnId.isNullOrBlank() &&
        !itemId.isNullOrBlank() && !cwd.isNullOrBlank() &&
        (params["startedAtMs"] == null || params["startedAtMs"].isJsonLong()) &&
        permissions is JsonObject && permissions.isValidPermissionProfile() &&
        params.hasValidOptionalStrings(setOf("environmentId", "reason")) &&
        params["availableDecisions"].isNullOrValidAvailableDecisions() && unknown == null
    val context = buildList {
        addContext("Working directory", cwd)
        addContext("Reason", params.string("reason"))
        addContext("Thread", threadId)
        addContext("Turn", turnId)
        addContext("Item", itemId)
        addContext("Started at (ms)", (params["startedAtMs"] as? JsonPrimitive)?.longOrNull?.toString())
        addContext("Environment", params.string("environmentId"))
        addJsonContext("Requested permissions", permissions)
        addJsonContext("Available decisions", params["availableDecisions"])
        addJsonContext("Unrecognized request data", unknown)
        if (!securityContextComplete) {
            add(ApprovalContextField("Validation warning", APPROVAL_VALIDATION_WARNING))
        }
    }
    return ApprovalRequest(
        requestId = id,
        kind = ApprovalKind.PERMISSION,
        title = "Allow additional permissions?",
        detail = params.string("reason") ?: "Remote Codex requests additional file or network permissions",
        rawMethod = method,
        rawParams = rawParams,
        threadId = threadId,
        turnId = turnId,
        itemId = itemId,
        startedAtMs = (params["startedAtMs"] as? JsonPrimitive)?.longOrNull,
        cwd = cwd,
        context = context,
        availableDecisions = params.approvalDecisions(ApprovalKind.PERMISSION, securityContextComplete),
        securityContextComplete = securityContextComplete,
    )
}

internal fun parseUserInputRequest(
    id: RpcRequestId,
    method: String,
    params: JsonObject,
    rawParams: String,
): ApprovalRequest {
    val unknown = params.unknownFields(USER_INPUT_REQUEST_KEYS)
    val questionElements = params["questions"] as? JsonArray
    val boundedQuestionElements = questionElements?.takeIf { it.size in 1..MAX_APPROVAL_QUESTIONS }
    val questions = boundedQuestionElements.orEmpty().mapNotNull(::parseApprovalQuestion)
    val isBlockingValid = (params["isBlocking"] as? JsonPrimitive)
        ?.takeUnless(JsonPrimitive::isString)
        ?.contentOrNull
        ?.toBooleanStrictOrNull() != null
    val autoResolution = params["autoResolutionMs"]
    val autoResolutionValid = autoResolution == null || autoResolution is JsonNull ||
        autoResolution.isJsonLong() && autoResolution.jsonPrimitive.longOrNull!! >= 0L
    val securityContextComplete = boundedQuestionElements != null && questions.isNotEmpty() &&
        questions.size == boundedQuestionElements.size &&
        questions.map(ApprovalQuestion::id).distinct().size == questions.size &&
        isBlockingValid && autoResolutionValid && unknown == null &&
        params.strictNonBlankString("threadId") != null &&
        params.strictNonBlankString("turnId") != null && !params.strictString("itemId").isNullOrBlank()
    return ApprovalRequest(
        requestId = id,
        kind = ApprovalKind.USER_INPUT,
        title = questions.firstOrNull()?.header?.ifBlank { null } ?: "Codex needs your input",
        detail = questions.firstOrNull()?.question ?: "Enter a reply",
        rawMethod = method,
        rawParams = rawParams,
        questions = questions,
        threadId = params.strictNonBlankString("threadId"),
        turnId = params.strictNonBlankString("turnId"),
        itemId = params.strictString("itemId"),
        availableDecisions = if (securityContextComplete) listOf("accept") else emptyList(),
        securityContextComplete = securityContextComplete,
    )
}

internal fun approvalDecisionElement(method: String, decision: String): JsonElement {
    if (method != "execCommandApproval" && method != "applyPatchApproval") return JsonPrimitive(decision)
    return when (decision) {
        "accept" -> JsonPrimitive("approved")
        "acceptForSession" -> JsonPrimitive("approved_for_session")
        "decline" -> buildJsonObject {
            put("denied", buildJsonObject { put("rejection", "Denied by user") })
        }
        "cancel" -> JsonPrimitive("abort")
        else -> JsonPrimitive(decision)
    }
}

private fun JsonObject.approvalDecisions(
    kind: ApprovalKind,
    securityContextComplete: Boolean,
): List<String> {
    val advertised = this["availableDecisions"]
    val decisions = if (advertised == null || advertised is JsonNull) {
        com.codex.remote.domain.defaultApprovalDecisions(kind)
    } else {
        (advertised as? JsonArray).orEmpty()
            .mapNotNull { (it as? JsonPrimitive)?.takeIf(JsonPrimitive::isString)?.contentOrNull }
            .filter { it in SUPPORTED_APPROVAL_DECISIONS }
            .distinct()
    }
    return if (securityContextComplete) decisions else decisions.filterNot { it.startsWith("accept") }
}

private fun JsonObject.parseLegacyFileChanges(): Pair<List<FileChangeSummary>, Boolean> {
    val changes = obj("fileChanges") ?: return emptyList<FileChangeSummary>() to false
    if (changes.isEmpty()) return emptyList<FileChangeSummary>() to false
    val parsed = mutableListOf<FileChangeSummary>()
    for ((path, element) in changes) {
        if (path.isBlank()) return emptyList<FileChangeSummary>() to false
        val change = element.asObject() ?: return emptyList<FileChangeSummary>() to false
        val kind = change.strictString("type") ?: return emptyList<FileChangeSummary>() to false
        val (diff, movePath) = when (kind) {
            "update" -> {
                if (change.keys.any { it !in setOf("type", "unified_diff", "move_path") }) {
                    return emptyList<FileChangeSummary>() to false
                }
                val diff = change.strictString("unified_diff")
                    ?: return emptyList<FileChangeSummary>() to false
                val rawMovePath = change["move_path"]
                val movePath = when (rawMovePath) {
                    null, JsonNull -> null
                    is JsonPrimitive -> rawMovePath.takeIf(JsonPrimitive::isString)?.contentOrNull
                        ?.takeIf(String::isNotBlank)
                        ?: return emptyList<FileChangeSummary>() to false
                    else -> return emptyList<FileChangeSummary>() to false
                }
                diff to movePath
            }
            "add", "delete" -> {
                if (change.keys != setOf("type", "content")) {
                    return emptyList<FileChangeSummary>() to false
                }
                val diff = change.strictString("content")
                    ?: return emptyList<FileChangeSummary>() to false
                diff to null
            }
            else -> return emptyList<FileChangeSummary>() to false
        }
        parsed += FileChangeSummary(path = path, kind = kind, diff = diff, movePath = movePath)
    }
    return parsed to true
}

internal fun parseTimelineFileChange(element: JsonElement): FileChangeSummary? {
    val change = element.asObject() ?: return null
    if (change.keys != setOf("diff", "kind", "path")) return null
    val path = change.strictString("path")?.takeIf(String::isNotBlank) ?: return null
    val diff = change.strictString("diff") ?: return null
    val kind = change.obj("kind") ?: return null
    val kindType = kind.strictString("type") ?: return null
    val movePath = when (kindType) {
        "add", "delete" -> {
            if (kind.keys != setOf("type")) return null
            null
        }
        "update" -> {
            if (kind.keys.any { it !in setOf("type", "move_path") }) return null
            val rawMovePath = kind["move_path"]
            when (rawMovePath) {
                null, JsonNull -> null
                is JsonPrimitive -> rawMovePath.takeIf(JsonPrimitive::isString)?.contentOrNull
                    ?.takeIf(String::isNotBlank)
                    ?: return null
                else -> return null
            }
        }
        else -> return null
    }
    return FileChangeSummary(path = path, kind = kindType, diff = diff, movePath = movePath)
}

private fun JsonElement?.isStringArray(): Boolean =
    this is JsonArray && all { it is JsonPrimitive && it.isString }

private fun JsonElement?.isNullOrStringArray(): Boolean =
    this == null || this is JsonNull || isStringArray()

private fun JsonElement?.isNullOrValidCommandActions(): Boolean = when (this) {
    null, JsonNull -> true
    is JsonArray -> all { element ->
        val command = element.asObject() ?: return@all false
        val type = command.strictString("type") ?: return@all false
        if (command.strictString("command") == null) return@all false
        when (type) {
            "read" -> command.keys == setOf("command", "name", "path", "type") &&
                command.strictString("name") != null && command.strictString("path") != null
            "listFiles" -> command.keys.all { it in setOf("command", "path", "type") } &&
                command.hasValidOptionalStrings(setOf("path"))
            "search" -> command.keys.all { it in setOf("command", "path", "query", "type") } &&
                command.hasValidOptionalStrings(setOf("path", "query"))
            "unknown" -> command.keys == setOf("command", "type")
            else -> false
        }
    }
    else -> false
}

private fun JsonElement?.isNullOrValidNetworkApprovalContext(): Boolean = when (this) {
    null, JsonNull -> true
    is JsonObject -> keys == setOf("host", "protocol") &&
        !strictString("host").isNullOrBlank() &&
        strictString("protocol") in setOf("http", "https", "socks5Tcp", "socks5Udp")
    else -> false
}

private fun JsonElement?.isNullOrValidNetworkPolicyAmendments(): Boolean = when (this) {
    null, JsonNull -> true
    is JsonArray -> all { element ->
        val amendment = element.asObject() ?: return@all false
        amendment.keys == setOf("action", "host") &&
            amendment.strictString("action") in setOf("allow", "deny") &&
            !amendment.strictString("host").isNullOrBlank()
    }
    else -> false
}

private fun JsonElement?.isValidLegacyParsedCommands(): Boolean {
    val commands = this as? JsonArray ?: return false
    return commands.all { element ->
        val command = element.asObject() ?: return@all false
        val type = command.strictString("type") ?: return@all false
        if (command.strictString("cmd") == null) return@all false
        when (type) {
            "read" -> command.keys == setOf("cmd", "name", "path", "type") &&
                command.strictString("name") != null && command.strictString("path") != null
            "list_files" -> command.keys.all { it in setOf("cmd", "path", "type") } &&
                command.hasValidOptionalStrings(setOf("path"))
            "search" -> command.keys.all { it in setOf("cmd", "path", "query", "type") } &&
                command.hasValidOptionalStrings(setOf("path", "query"))
            "unknown" -> command.keys == setOf("cmd", "type")
            else -> false
        }
    }
}

private fun JsonElement?.isNullOrValidAvailableDecisions(): Boolean = when (this) {
    null, JsonNull -> true
    is JsonArray -> all(JsonElement::isValidAvailableDecision)
    else -> false
}

private fun JsonElement.isValidAvailableDecision(): Boolean = when (this) {
    is JsonPrimitive -> isString && contentOrNull in SUPPORTED_APPROVAL_DECISIONS
    is JsonObject -> when {
        keys == setOf("acceptWithExecpolicyAmendment") -> {
            val wrapper = obj("acceptWithExecpolicyAmendment")
            val amendment = wrapper?.get("execpolicy_amendment")
            wrapper?.keys == setOf("execpolicy_amendment") && amendment.isStringArray()
        }
        keys == setOf("applyNetworkPolicyAmendment") -> {
            val wrapper = obj("applyNetworkPolicyAmendment")
            val amendment = wrapper?.obj("network_policy_amendment")
            wrapper?.keys == setOf("network_policy_amendment") &&
                amendment?.keys == setOf("action", "host") &&
                amendment.strictString("action") in setOf("allow", "deny") &&
                amendment.strictString("host") != null
        }
        else -> false
    }
    else -> false
}

private fun parseApprovalQuestion(element: JsonElement): ApprovalQuestion? {
    val question = element.asObject() ?: return null
    if (question.keys.any { it !in setOf("header", "id", "isOther", "isSecret", "options", "question") }) {
        return null
    }
    val id = question.strictString("id")?.takeIf(String::isNotBlank) ?: return null
    val header = question.strictString("header") ?: return null
    val prompt = question.strictString("question")?.takeIf(String::isNotBlank) ?: return null
    val isOther = question.strictBoolean("isOther") ?: return null
    val isSecret = question.strictBoolean("isSecret") ?: return null
    if (isSecret) return null
    if (!question.containsKey("options")) return null
    val optionsElement = question["options"]
    val options = when (optionsElement) {
        null, JsonNull -> emptyList()
        is JsonArray -> {
            if (optionsElement.size > MAX_APPROVAL_OPTIONS_PER_QUESTION) return null
            optionsElement.mapNotNull { optionElement ->
                val option = optionElement.asObject() ?: return null
                if (option.keys != setOf("description", "label")) return null
                ApprovalOption(
                    description = option.strictString("description") ?: return null,
                    label = option.strictString("label")?.takeIf(String::isNotBlank) ?: return null,
                )
            }
        }
        else -> return null
    }
    if (options.map(ApprovalOption::label).distinct().size != options.size) return null
    return ApprovalQuestion(id = id, header = header, question = prompt, isOther = isOther, options = options)
}

private fun JsonObject.strictBoolean(key: String): Boolean? = (this[key] as? JsonPrimitive)
    ?.takeUnless(JsonPrimitive::isString)
    ?.contentOrNull
    ?.toBooleanStrictOrNull()

private fun JsonObject.unknownFields(allowedKeys: Set<String>): JsonObject? {
    val values = entries.filter { it.key !in allowedKeys && it.key !in setOf("metadata", "traceId", "requestTimestamp") }.associate { it.toPair() }
    return if (values.isEmpty()) null else JsonObject(values)
}

private fun JsonElement?.approvalDisplayValue(): String? = when (this) {
    null, JsonNull -> null
    is JsonPrimitive -> contentOrNull
    is JsonArray -> toString()
    else -> toString()
}

private fun MutableList<ApprovalContextField>.addContext(label: String, value: String?) {
    value?.takeIf(String::isNotBlank)?.let { add(ApprovalContextField(label, it)) }
}

private fun MutableList<ApprovalContextField>.addJsonContext(label: String, value: JsonElement?) {
    if (value != null && value !is JsonNull) add(ApprovalContextField(label, value.toString()))
}

private fun JsonElement?.isNullOrValidPermissionProfile(): Boolean =
    this == null || this is JsonNull || (this as? JsonObject)?.isValidPermissionProfile() == true

private fun JsonObject.isValidPermissionProfile(): Boolean {
    if (keys.any { it !in setOf("fileSystem", "network") }) return false
    val fileSystem = this["fileSystem"]
    if (fileSystem != null && fileSystem !is JsonNull &&
        (fileSystem as? JsonObject)?.isValidFileSystemPermissions() != true
    ) return false
    val network = this["network"]
    if (network != null && network !is JsonNull &&
        (network as? JsonObject)?.isValidNetworkPermissions() != true
    ) return false
    return true
}

private fun JsonObject.isValidFileSystemPermissions(): Boolean {
    if (keys.any { it !in setOf("entries", "globScanMaxDepth", "read", "write") }) return false
    val entries = this["entries"]
    if (entries != null && entries !is JsonNull) {
        val array = entries as? JsonArray ?: return false
        if (array.any { (it as? JsonObject)?.isValidFileSystemEntry() != true }) return false
    }
    for (key in listOf("read", "write")) {
        val value = this[key]
        if (value != null && value !is JsonNull) {
            val array = value as? JsonArray ?: return false
            if (array.any { it !is JsonPrimitive || !it.isString }) return false
        }
    }
    val maxDepth = this["globScanMaxDepth"]
    if (maxDepth != null && maxDepth !is JsonNull &&
        (maxDepth as? JsonPrimitive)?.takeUnless(JsonPrimitive::isString)
            ?.longOrNull?.let { it in 1..UInt.MAX_VALUE.toLong() } != true
    ) return false
    return true
}

private fun JsonObject.isValidNetworkPermissions(): Boolean {
    if (keys.any { it != "enabled" }) return false
    val enabled = this["enabled"] ?: return true
    if (enabled is JsonNull) return true
    return enabled is JsonPrimitive && !enabled.isString &&
        enabled.contentOrNull?.toBooleanStrictOrNull() != null
}

private fun JsonObject.isValidFileSystemEntry(): Boolean {
    if (keys != setOf("access", "path")) return false
    if (strictString("access") !in setOf("read", "write", "deny")) return false
    return (this["path"] as? JsonObject)?.isValidFileSystemPath() == true
}

private fun JsonObject.isValidFileSystemPath(): Boolean = when (strictString("type")) {
    "path" -> keys == setOf("type", "path") && !strictString("path").isNullOrBlank()
    "glob_pattern" -> keys == setOf("type", "pattern") && !strictString("pattern").isNullOrBlank()
    "special" -> keys == setOf("type", "value") &&
        (this["value"] as? JsonObject)?.isValidSpecialFileSystemPath() == true
    else -> false
}

private fun JsonObject.isValidSpecialFileSystemPath(): Boolean = when (strictString("kind")) {
    "root", "minimal", "tmpdir", "slash_tmp" -> keys == setOf("kind")
    "project_roots" -> keys.all { it in setOf("kind", "subpath") } &&
        (this["subpath"] == null || this["subpath"] is JsonNull || strictString("subpath") != null)
    "unknown" -> keys.all { it in setOf("kind", "path", "subpath") } && !strictString("path").isNullOrBlank() &&
        (this["subpath"] == null || this["subpath"] is JsonNull || strictString("subpath") != null)
    else -> false
}

private const val APPROVAL_VALIDATION_WARNING =
    "Request contains missing, malformed, or unrecognized security fields. Only denial is allowed."

private fun JsonElement.asObject(): JsonObject? = this as? JsonObject
private fun JsonObject.obj(key: String): JsonObject? = this[key] as? JsonObject
private fun JsonObject.array(key: String): JsonArray = this[key] as? JsonArray ?: JsonArray(emptyList())
private fun JsonObject.string(key: String): String? = (this[key] as? JsonPrimitive)?.contentOrNull
private fun JsonObject.strictString(key: String): String? =
    (this[key] as? JsonPrimitive)?.takeIf(JsonPrimitive::isString)?.contentOrNull
internal fun JsonObject.strictNonBlankString(key: String): String? =
    strictString(key)?.takeIf(String::isNotBlank)
private fun JsonObject.hasValidOptionalStrings(keys: Set<String>): Boolean = keys.all { key ->
    val value = this[key]
    value == null || value is JsonNull || value is JsonPrimitive && value.isString
}
private fun JsonElement?.isJsonLong(): Boolean =
    this is JsonPrimitive && !isString && longOrNull != null
private fun JsonObject.boolean(key: String): Boolean = (this[key] as? JsonPrimitive)?.contentOrNull?.toBooleanStrictOrNull() ?: false
private fun JsonObject.booleanOrDefault(key: String, default: Boolean): Boolean =
    (this[key] as? JsonPrimitive)?.contentOrNull?.toBooleanStrictOrNull() ?: default
