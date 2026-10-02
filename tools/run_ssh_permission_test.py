#!/usr/bin/env python3
"""Run a production-SSH permission/lifecycle test with notification denial set before instrumentation.

Build both debug APKs first. Config is a private JSON file containing the ssh*
instrumentation arguments documented in ARCHITECTURE.md; it is never printed.
Android may kill an app when its permission is revoked, so do this outside tests.
"""

import argparse
import json
import re
import shlex
import subprocess
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--serial", required=True)
    parser.add_argument("--config", type=Path, required=True)
    args = parser.parse_args()
    adb = ["adb", "-s", args.serial]

    def shell(*command: str) -> str:
        return subprocess.check_output(adb + ["shell", shlex.join(command)], text=True)

    subprocess.run(adb + ["install", "-r", "-t", "app/build/outputs/apk/debug/app-debug.apk"], check=True)
    subprocess.run(adb + ["install", "-r", "-t", "app/build/outputs/apk/androidTest/debug/app-debug-androidTest.apk"], check=True)
    previous = shell("dumpsys", "package", "com.codex.remote")
    permission = re.search(r"android.permission.POST_NOTIFICATIONS: granted=(true|false), flags=\[([^]]*)]", previous)
    if permission is None:
        raise RuntimeError("Expected Android API33+ notification permission record")
    was_granted = permission.group(1) == "true"
    was_fixed = "USER_FIXED" in permission.group(2)
    config = json.loads(args.config.read_text())
    config["sshNotificationPermission"] = "denied"
    command = ["am", "instrument", "-w", "-r", "-e", "class",
               "com.codex.remote.SshRecoveryDeviceTest#configuredNotificationPermissionMaintainsAcrossRotationAndAppDisconnect"]
    for key, value in config.items():
        if not key.startswith("ssh"):
            raise ValueError("Only ssh* fixture arguments are accepted")
        command += ["-e", key, str(value)]
    command += ["com.codex.remote.test/androidx.test.runner.AndroidJUnitRunner"]
    try:
        shell("pm", "revoke", "com.codex.remote", "android.permission.POST_NOTIFICATIONS")
        shell("pm", "set-permission-flags", "com.codex.remote", "android.permission.POST_NOTIFICATIONS", "user-fixed")
        output = shell(*command)
        print(output)
        if "OK (1 test)" not in output:
            raise RuntimeError("Notification-denied SSH instrumentation failed")
    finally:
        if not was_fixed:
            shell("pm", "clear-permission-flags", "com.codex.remote", "android.permission.POST_NOTIFICATIONS", "user-fixed")
        if was_granted:
            shell("pm", "grant", "com.codex.remote", "android.permission.POST_NOTIFICATIONS")


if __name__ == "__main__":
    main()
