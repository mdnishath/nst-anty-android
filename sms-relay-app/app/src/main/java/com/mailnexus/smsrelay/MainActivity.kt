package com.mailnexus.smsrelay

import android.Manifest
import android.content.BroadcastReceiver
import android.content.Context
import android.content.Intent
import android.content.IntentFilter
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import android.widget.*
import androidx.appcompat.app.AppCompatActivity
import androidx.core.app.ActivityCompat
import androidx.core.content.ContextCompat
import java.text.SimpleDateFormat
import java.util.*

class MainActivity : AppCompatActivity() {

    private lateinit var etServerIp: EditText
    private lateinit var etServerPort: EditText
    private lateinit var btnTestConnection: Button
    private lateinit var btnToggleRelay: Button
    private lateinit var tvStatus: TextView
    private lateinit var tvLastCode: TextView
    private lateinit var tvLog: TextView
    private lateinit var btnClearLog: Button

    private val logLines = mutableListOf<String>()
    private val dateFormat = SimpleDateFormat("HH:mm:ss", Locale.getDefault())

    // Receiver for UI updates when codes are extracted
    private val codeReceiver = object : BroadcastReceiver() {
        override fun onReceive(context: Context, intent: Intent) {
            val code = intent.getStringExtra("code") ?: return
            val sender = intent.getStringExtra("sender") ?: "unknown"
            tvLastCode.text = "Last Code: $code (from $sender)"
            addLog("Code extracted: $code from $sender")
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_main)

        // Bind views
        etServerIp = findViewById(R.id.etServerIp)
        etServerPort = findViewById(R.id.etServerPort)
        btnTestConnection = findViewById(R.id.btnTestConnection)
        btnToggleRelay = findViewById(R.id.btnToggleRelay)
        tvStatus = findViewById(R.id.tvStatus)
        tvLastCode = findViewById(R.id.tvLastCode)
        tvLog = findViewById(R.id.tvLog)
        btnClearLog = findViewById(R.id.btnClearLog)

        // Load saved config
        etServerIp.setText(CodeSender.getServerIp(this))
        etServerPort.setText(CodeSender.getServerPort(this).toString())
        updateToggleButton()

        // Request SMS permissions
        requestPermissions()

        // Setup callbacks
        CodeSender.onCodeSent = { code, success, message ->
            runOnUiThread {
                if (success) {
                    addLog("Sent code $code to server")
                } else {
                    addLog("FAILED to send code $code: $message")
                }
            }
        }

        CodeSender.onStatusChange = { connected ->
            runOnUiThread {
                tvStatus.text = if (connected) "Connected" else "Disconnected"
                tvStatus.setTextColor(
                    ContextCompat.getColor(this,
                        if (connected) android.R.color.holo_green_light
                        else android.R.color.holo_red_light)
                )
            }
        }

        // Test Connection button
        btnTestConnection.setOnClickListener {
            val ip = etServerIp.text.toString().trim()
            val port = etServerPort.text.toString().trim().toIntOrNull() ?: 5000

            if (ip.isBlank()) {
                Toast.makeText(this, "Enter server IP address", Toast.LENGTH_SHORT).show()
                return@setOnClickListener
            }

            CodeSender.saveServerConfig(this, ip, port)
            addLog("Testing connection to $ip:$port...")
            btnTestConnection.isEnabled = false

            CodeSender.testConnection(this) { success, message ->
                runOnUiThread {
                    btnTestConnection.isEnabled = true
                    addLog(if (success) "Connection OK!" else "Connection failed: $message")
                    Toast.makeText(this, message, Toast.LENGTH_SHORT).show()
                }
            }
        }

        // Toggle Relay button
        btnToggleRelay.setOnClickListener {
            val ip = etServerIp.text.toString().trim()
            val port = etServerPort.text.toString().trim().toIntOrNull() ?: 5000

            if (ip.isBlank()) {
                Toast.makeText(this, "Enter server IP first", Toast.LENGTH_SHORT).show()
                return@setOnClickListener
            }

            CodeSender.saveServerConfig(this, ip, port)

            val nowEnabled = !CodeSender.isEnabled(this)
            CodeSender.setEnabled(this, nowEnabled)
            updateToggleButton()

            addLog(if (nowEnabled) "SMS Relay STARTED" else "SMS Relay STOPPED")
            Toast.makeText(this,
                if (nowEnabled) "SMS Relay Active - codes will be forwarded"
                else "SMS Relay Stopped",
                Toast.LENGTH_SHORT).show()
        }

        // Clear Log button
        btnClearLog.setOnClickListener {
            logLines.clear()
            tvLog.text = "Log cleared."
        }

        // Register code receiver
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            registerReceiver(codeReceiver, IntentFilter("com.mailnexus.smsrelay.CODE_RECEIVED"),
                RECEIVER_NOT_EXPORTED)
        } else {
            registerReceiver(codeReceiver, IntentFilter("com.mailnexus.smsrelay.CODE_RECEIVED"))
        }

        addLog("SMS Relay app started")
    }

    override fun onDestroy() {
        super.onDestroy()
        try { unregisterReceiver(codeReceiver) } catch (_: Exception) {}
    }

    private fun updateToggleButton() {
        val enabled = CodeSender.isEnabled(this)
        btnToggleRelay.text = if (enabled) "STOP RELAY" else "START RELAY"
        btnToggleRelay.setBackgroundColor(
            ContextCompat.getColor(this,
                if (enabled) android.R.color.holo_red_dark
                else android.R.color.holo_green_dark)
        )
    }

    private fun addLog(message: String) {
        val timestamp = dateFormat.format(Date())
        val line = "[$timestamp] $message"
        logLines.add(line)
        // Keep only last 50 lines
        if (logLines.size > 50) logLines.removeAt(0)
        tvLog.text = logLines.joinToString("\n")
    }

    private fun requestPermissions() {
        val permissions = mutableListOf(
            Manifest.permission.RECEIVE_SMS,
            Manifest.permission.READ_SMS,
        )
        // Android 13+ needs POST_NOTIFICATIONS
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            permissions.add(Manifest.permission.POST_NOTIFICATIONS)
        }

        val needed = permissions.filter {
            ContextCompat.checkSelfPermission(this, it) != PackageManager.PERMISSION_GRANTED
        }

        if (needed.isNotEmpty()) {
            ActivityCompat.requestPermissions(this, needed.toTypedArray(), 100)
        }
    }

    override fun onRequestPermissionsResult(
        requestCode: Int, permissions: Array<out String>, grantResults: IntArray
    ) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults)
        if (requestCode == 100) {
            val denied = permissions.zip(grantResults.toTypedArray())
                .filter { it.second != PackageManager.PERMISSION_GRANTED }
                .map { it.first }

            if (denied.isNotEmpty()) {
                addLog("WARNING: Permissions denied: ${denied.joinToString()}")
                Toast.makeText(this, "SMS permission required for relay to work!", Toast.LENGTH_LONG).show()
            } else {
                addLog("All permissions granted")
            }
        }
    }
}
