package com.codex.remote

import android.app.Application
import com.codex.remote.connection.ProcessConnectionOwner

class CodexRemoteApplication : Application() {
    val connectionOwner: ProcessConnectionOwner by lazy { ProcessConnectionOwner(this) }
    val appViewModel: AppViewModel get() = connectionOwner.viewModel
}
