package com.mailnexus.smsrelay

import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.provider.Telephony
import android.util.Log

/**
 * BroadcastReceiver that intercepts incoming SMS messages,
 * extracts Google verification codes, and forwards them to the backend.
 */
class SmsReceiver : BroadcastReceiver() {

    companion object {
        private const val TAG = "SmsReceiver"

        // Regex patterns to extract verification codes from SMS
        private val CODE_PATTERNS = listOf(
            // Google G-XXXXXX format
            Regex("""G-(\d{4,8})"""),
            // "Your verification code is XXXXXX"
            Regex("""(?:verification|verify|security)\s*code\s*(?:is|:)?\s*(\d{4,8})""", RegexOption.IGNORE_CASE),
            // "XXXXXX is your verification code"
            Regex("""(\d{4,8})\s*is your\s*(?:verification|verify|security)\s*code""", RegexOption.IGNORE_CASE),
            // "Code: XXXXXX" or "code: XXXXXX"
            Regex("""(?:code|Code|CODE)\s*[:=]\s*(\d{4,8})"""),
            // "Enter XXXXXX" or "enter XXXXXX"
            Regex("""(?:enter|Enter|ENTER)\s+(\d{4,8})"""),
            // Generic: standalone 6-digit number (last resort)
            Regex("""\b(\d{6})\b"""),
        )

        /**
         * Extract verification code from SMS body.
         * Returns the code string or null if no code found.
         */
        fun extractCode(message: String): String? {
            for (pattern in CODE_PATTERNS) {
                val match = pattern.find(message)
                if (match != null && match.groupValues.size > 1) {
                    val code = match.groupValues[1]
                    Log.d(TAG, "Extracted code '$code' using pattern: ${pattern.pattern}")
                    return code
                }
            }
            return null
        }
    }

    override fun onReceive(context: Context, intent: Intent) {
        if (intent.action != Telephony.Sms.Intents.SMS_RECEIVED_ACTION) return

        // Check if relay is enabled
        if (!CodeSender.isEnabled(context)) {
            Log.d(TAG, "SMS Relay is disabled, ignoring SMS")
            return
        }

        val messages = Telephony.Sms.Intents.getMessagesFromIntent(intent)
        if (messages.isNullOrEmpty()) return

        // Combine multi-part SMS
        val sender = messages[0].displayOriginatingAddress ?: "unknown"
        val fullMessage = messages.joinToString("") { it.displayMessageBody ?: "" }

        Log.d(TAG, "SMS received from $sender: ${fullMessage.take(50)}...")

        // Extract verification code
        val code = extractCode(fullMessage)
        if (code != null) {
            Log.i(TAG, "Verification code found: $code from $sender")
            CodeSender.sendCode(context, code, sender, fullMessage)

            // Notify UI if activity is active
            try {
                val updateIntent = Intent("com.mailnexus.smsrelay.CODE_RECEIVED").apply {
                    putExtra("code", code)
                    putExtra("sender", sender)
                    putExtra("message", fullMessage)
                }
                context.sendBroadcast(updateIntent)
            } catch (e: Exception) {
                Log.e(TAG, "Could not send UI broadcast: ${e.message}")
            }
        } else {
            Log.d(TAG, "No verification code found in SMS from $sender")
        }
    }
}
