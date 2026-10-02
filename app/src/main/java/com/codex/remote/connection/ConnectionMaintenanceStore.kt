package com.codex.remote.connection

import android.content.Context

/** Only the desired saved ID is durable. No drafts, grants or additional credentials. */
class ConnectionMaintenanceStore(context: Context) {
    private val preferences = context.applicationContext.getSharedPreferences("connection_maintenance", Context.MODE_PRIVATE)
    fun desiredConnectionId(): String? = preferences.getString("desired_connection_id", null)?.takeIf { it.isNotBlank() }
    fun remember(id: String) { check(preferences.edit().putString("desired_connection_id", id).commit()) { "Could not save connection maintenance target" } }
    fun clear() { check(preferences.edit().remove("desired_connection_id").commit()) { "Could not clear connection maintenance target" } }
}
