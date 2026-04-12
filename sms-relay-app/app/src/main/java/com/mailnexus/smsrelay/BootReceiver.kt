package com.mailnexus.smsrelay

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.util.Log

/**
 * Starts SMS relay service on device boot if it was previously enabled.
 */
class BootReceiver : BroadcastReceiver() {
    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action == Intent.ACTION_BOOT_COMPLETED) {
            Log.i("BootReceiver", "Device booted — SMS Relay is ${if (CodeSender.isEnabled(context)) "enabled" else "disabled"}")
            // SmsReceiver is statically registered in manifest, so it will auto-receive SMS
            // No need to start a service — BroadcastReceiver handles everything
        }
    }
}
