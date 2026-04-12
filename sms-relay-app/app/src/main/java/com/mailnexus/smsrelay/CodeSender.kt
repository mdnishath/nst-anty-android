package com.mailnexus.smsrelay

import android.content.Context
import android.content.SharedPreferences
import kotlinx.coroutines.*
import java.io.OutputStreamWriter
import java.net.HttpURLConnection
import java.net.URL
import org.json.JSONObject

/**
 * Sends extracted SMS verification codes to the Flask backend via HTTP POST.
 */
object CodeSender {

    private const val PREFS_NAME = "sms_relay_prefs"
    private const val KEY_SERVER_IP = "server_ip"
    private const val KEY_SERVER_PORT = "server_port"
    private const val KEY_ENABLED = "relay_enabled"

    private val scope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    // Callback for UI updates
    var onCodeSent: ((code: String, success: Boolean, message: String) -> Unit)? = null
    var onStatusChange: ((connected: Boolean) -> Unit)? = null

    fun getPrefs(context: Context): SharedPreferences {
        return context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
    }

    fun getServerIp(context: Context): String {
        return getPrefs(context).getString(KEY_SERVER_IP, "") ?: ""
    }

    fun getServerPort(context: Context): Int {
        return getPrefs(context).getInt(KEY_SERVER_PORT, 5000)
    }

    fun isEnabled(context: Context): Boolean {
        return getPrefs(context).getBoolean(KEY_ENABLED, false)
    }

    fun setEnabled(context: Context, enabled: Boolean) {
        getPrefs(context).edit().putBoolean(KEY_ENABLED, enabled).apply()
    }

    fun saveServerConfig(context: Context, ip: String, port: Int) {
        getPrefs(context).edit()
            .putString(KEY_SERVER_IP, ip)
            .putInt(KEY_SERVER_PORT, port)
            .apply()
    }

    fun getBaseUrl(context: Context): String {
        val ip = getServerIp(context)
        val port = getServerPort(context)
        return "http://$ip:$port"
    }

    /**
     * Send verification code to the Flask backend.
     */
    fun sendCode(context: Context, code: String, sender: String, fullMessage: String) {
        if (!isEnabled(context)) return

        val baseUrl = getBaseUrl(context)
        if (baseUrl.contains("://:" ) || getServerIp(context).isBlank()) return

        scope.launch {
            try {
                val url = URL("$baseUrl/api/sms-code")
                val conn = url.openConnection() as HttpURLConnection
                conn.requestMethod = "POST"
                conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8")
                conn.doOutput = true
                conn.connectTimeout = 5000
                conn.readTimeout = 5000

                val payload = JSONObject().apply {
                    put("code", code)
                    put("sender", sender)
                    put("full_message", fullMessage)
                    put("timestamp", System.currentTimeMillis() / 1000)
                }

                val writer = OutputStreamWriter(conn.outputStream, "UTF-8")
                writer.write(payload.toString())
                writer.flush()
                writer.close()

                val responseCode = conn.responseCode
                val success = responseCode == 200

                withContext(Dispatchers.Main) {
                    onCodeSent?.invoke(code, success,
                        if (success) "Code sent successfully" else "Server error: $responseCode")
                }

                conn.disconnect()
            } catch (e: Exception) {
                withContext(Dispatchers.Main) {
                    onCodeSent?.invoke(code, false, "Connection failed: ${e.message}")
                }
            }
        }
    }

    /**
     * Test connection to the Flask backend.
     */
    fun testConnection(context: Context, callback: (success: Boolean, message: String) -> Unit) {
        val baseUrl = getBaseUrl(context)
        if (getServerIp(context).isBlank()) {
            callback(false, "Server IP is empty")
            return
        }

        scope.launch {
            try {
                val url = URL("$baseUrl/api/health")
                val conn = url.openConnection() as HttpURLConnection
                conn.requestMethod = "GET"
                conn.connectTimeout = 5000
                conn.readTimeout = 5000

                val responseCode = conn.responseCode
                val success = responseCode == 200

                withContext(Dispatchers.Main) {
                    onStatusChange?.invoke(success)
                    callback(success,
                        if (success) "Connected to MailNexus Pro!" else "Server responded with $responseCode")
                }

                conn.disconnect()
            } catch (e: Exception) {
                withContext(Dispatchers.Main) {
                    onStatusChange?.invoke(false)
                    callback(false, "Connection failed: ${e.message}")
                }
            }
        }
    }
}
