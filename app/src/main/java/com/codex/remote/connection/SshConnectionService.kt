package com.codex.remote.connection

import android.app.NotificationChannel
import android.app.NotificationManager
import android.app.PendingIntent
import android.app.Service
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.ServiceInfo
import android.net.ConnectivityManager
import android.net.Network
import android.os.Build
import android.os.Handler
import android.os.IBinder
import android.os.Looper
import android.os.PowerManager
import androidx.core.app.NotificationCompat
import androidx.core.app.ServiceCompat
import androidx.core.content.ContextCompat
import com.codex.remote.CodexRemoteApplication
import com.codex.remote.MainActivity
import com.codex.remote.R
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch

/** Platform callback/notification adapter; the Application owns the only connection. */
class SshConnectionService : Service() {
    private val owner get() = (application as CodexRemoteApplication).connectionOwner
    private val notificationScope = CoroutineScope(SupervisorJob() + Dispatchers.Main.immediate)
    private lateinit var notificationBuilder: NotificationCompat.Builder
    private var observesStatus = false
    private val main = Handler(Looper.getMainLooper())
    private var destroyed = false
    private var networkRegistered = false
    private var idleRegistered = false
    private var reportReceived = false
    private val connectivity by lazy { getSystemService(ConnectivityManager::class.java) }
    private val power by lazy { getSystemService(PowerManager::class.java) }
    private val networkCallback = object : ConnectivityManager.NetworkCallback() {
        override fun onAvailable(network: Network) { post { reportReceived = true; owner.networkAvailable(network.networkHandle) } }
        override fun onLost(network: Network) { post { reportReceived = true; owner.networkLost(network.networkHandle) } }
    }
    private val idleReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context?, intent: Intent?) { owner.deviceIdle(power.isDeviceIdleMode) }
    }
    private fun post(action: () -> Unit) { main.post { if (!destroyed) action() } }

    override fun onCreate() {
        super.onCreate()
        getSystemService(NotificationManager::class.java).createNotificationChannel(
            NotificationChannel(CHANNEL_ID, "SSH connection", NotificationManager.IMPORTANCE_LOW).apply { setShowBadge(false) },
        )
        val open = PendingIntent.getActivity(this, 1, Intent(this, MainActivity::class.java), PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)
        val disconnect = PendingIntent.getService(this, 2, Intent(this, SshConnectionService::class.java).setAction(ACTION_DISCONNECT), PendingIntent.FLAG_IMMUTABLE or PendingIntent.FLAG_UPDATE_CURRENT)
        notificationBuilder = NotificationCompat.Builder(this, CHANNEL_ID)
            .setSmallIcon(R.drawable.ic_launcher_foreground)
            .setContentTitle("Codex SSH connection")
            .setContentText("Connection maintained; open the app for recovery status")
            .setContentIntent(open).setOngoing(true).setOnlyAlertOnce(true)
            .addAction(android.R.drawable.ic_menu_close_clear_cancel, "Disconnect", disconnect)
        // Prompt even if notification permission was denied: Android still requires foreground entry.
        ServiceCompat.startForeground(this, NOTIFICATION_ID, notificationBuilder.build(),
            if (Build.VERSION.SDK_INT >= 34) ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE else 0)
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        if (intent?.action == ACTION_DISCONNECT) {
            owner.viewModel.disconnect()
            stopForeground(STOP_FOREGROUND_REMOVE); stopSelf()
            return START_NOT_STICKY
        }
        if (ConnectionMaintenanceStore(this).desiredConnectionId() == null) {
            stopForeground(STOP_FOREGROUND_REMOVE); stopSelf(); return START_NOT_STICKY
        }
        owner.restoreDesiredConnection()
        if (!observesStatus) {
            observesStatus = true
            notificationScope.launch {
                owner.viewModel.state.map { it.maintenanceStatus ?: "Preparing SSH connection" }
                    .distinctUntilChanged().collect { status ->
                        if (!destroyed && ConnectionMaintenanceStore(this@SshConnectionService).desiredConnectionId() != null) {
                            runCatching { getSystemService(NotificationManager::class.java).notify(
                                NOTIFICATION_ID, notificationBuilder.setContentText(status).build()) }
                        }
                    }
            }
        }
        if (!networkRegistered) {
            connectivity.registerDefaultNetworkCallback(networkCallback)
            networkRegistered = true
            // Baseline after already queued availability callbacks; avoids inventing a stale loss.
            main.postDelayed({ if (!destroyed && !reportReceived) {
                connectivity.activeNetwork?.let { owner.networkAvailable(it.networkHandle) } ?: owner.noNetwork()
            } }, 500)
        }
        if (!idleRegistered) {
            ContextCompat.registerReceiver(this, idleReceiver, IntentFilter(PowerManager.ACTION_DEVICE_IDLE_MODE_CHANGED), ContextCompat.RECEIVER_NOT_EXPORTED)
            idleRegistered = true
        }
        owner.deviceIdle(power.isDeviceIdleMode)
        return START_STICKY
    }

    override fun onDestroy() {
        destroyed = true
        notificationScope.cancel()
        if (networkRegistered) connectivity.unregisterNetworkCallback(networkCallback)
        if (idleRegistered) unregisterReceiver(idleReceiver)
        main.removeCallbacksAndMessages(null)
        super.onDestroy()
    }
    override fun onBind(intent: Intent?): IBinder? = null
    companion object {
        const val ACTION_DISCONNECT = "com.codex.remote.DISCONNECT"
        const val CHANNEL_ID = "ssh_connection"
        const val NOTIFICATION_ID = 3
    }
}
