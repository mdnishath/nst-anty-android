/**
 * server.js — Backend health check, detection, monitoring, online/offline state.
 */
(function (App) {
    'use strict';

    // Poll server health
    App.checkBackendStatus = async function () {
        try {
            const response = await fetch('http://localhost:5000/api/health', {
                method: 'GET',
                headers: { 'Content-Type': 'application/json' }
            });

            if (response.ok) {
                if (!App.state.serverOnline) App.setServerOnline();
                return true;
            } else {
                if (App.state.serverOnline) App.setServerOffline();
                return false;
            }
        } catch (e) {
            if (App.state.serverOnline) App.setServerOffline();
            return false;
        }
    };

    App.autoDetectServer = async function () {
        // Listen for IPC notification from main.js that backend process was spawned
        if (window.electronAPI && window.electronAPI.onBackendReady) {
            window.electronAPI.onBackendReady(() => {
                console.log('[UI] Received backend-ready IPC from main process');
                App.startAggressiveDetection();
            });
        }

        // Listen for backend stdout/stderr — show in live log for debugging
        if (window.electronAPI && window.electronAPI.onBackendLog) {
            window.electronAPI.onBackendLog((msg) => {
                console.log('[Backend]', msg);
                if (/\d+\.\d+\.\d+\.\d+\s+-\s+-\s+\[/.test(msg)) return;
                if (/\* (Serving|Running|Debugger|Restarting)/.test(msg)) return;
                if (msg.includes('[SETUP]') || msg.includes('[ERR]') || msg.includes('Server started')) {
                    App.log(msg, msg.includes('[ERR]') ? 'error' : 'info');
                }
            });
        }

        // Initial fast detection: try up to 60s (30 x 2s)
        await App.startAggressiveDetection();

        // Start continuous background health monitor
        App.startContinuousHealthMonitor();
    };

    App.startAggressiveDetection = async function () {
        const MAX_ATTEMPTS = 60;
        const RETRY_DELAY = 500;
        const statusText = document.getElementById('backendStatusText');
        const toggleBtn = document.getElementById('serverToggleBtn');

        for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            try {
                const res = await fetch('http://localhost:5000/api/health', { method: 'GET' });
                if (res.ok) {
                    App.state.userStartedServer = true;
                    App.setServerOnline();
                    return;
                }
            } catch (e) { /* not ready yet */ }

            if (attempt === 1) {
                if (statusText) statusText.innerText = 'Backend: Starting\u2026';
                if (toggleBtn) toggleBtn.disabled = true;
            }

            if (attempt % 5 === 0 && statusText) {
                statusText.innerText = `Backend: Starting\u2026 (${attempt}/${MAX_ATTEMPTS})`;
            }

            await new Promise(r => setTimeout(r, RETRY_DELAY));
        }

        if (toggleBtn) toggleBtn.disabled = false;
        if (statusText) statusText.innerText = 'Backend: Offline';
    };

    App.startContinuousHealthMonitor = function () {
        setInterval(async () => {
            try {
                const res = await fetch('http://localhost:5000/api/health', { method: 'GET' });
                if (res.ok) {
                    if (!App.state.serverOnline) {
                        App.state.userStartedServer = true;
                        App.setServerOnline();
                    }
                } else {
                    if (App.state.serverOnline) App.setServerOffline();
                }
            } catch (e) {
                if (App.state.serverOnline) App.setServerOffline();
            }
        }, 5000);
    };

    App.setServerOnline = function () {
        const wasOffline = !App.state.serverOnline;
        App.state.serverOnline = true;
        const dot = document.getElementById('backendStatusDot');
        const text = document.getElementById('backendStatusText');
        const btn = document.getElementById('serverToggleBtn');

        if (dot) dot.classList.add('online');
        if (text) text.innerText = 'Backend: Online';
        if (wasOffline && App.toast) App.toast('Backend server is online', 'success');
        if (btn) {
            btn.className = 'state-stop';
            btn.innerHTML = '<i class="fas fa-stop"></i> Stop Backend';
            btn.disabled = false;
        }

        // (Re)connect SSE log stream
        if (App.connectLogStream) App.connectLogStream();

        // Auto-load saved settings
        if (App.loadConfig) App.loadConfig();
        if (App.loadProxy) App.loadProxy();
        if (App.loadFingerprint) App.loadFingerprint();
        if (App.loadNstConfig) App.loadNstConfig();
    };

    App.setServerOffline = function () {
        App.state.serverOnline = false;
        App.state.processing = false;
        const dot = document.getElementById('backendStatusDot');
        const text = document.getElementById('backendStatusText');
        const btn = document.getElementById('serverToggleBtn');
        const startBtn = document.getElementById('startBtn');
        const stopBtn = document.getElementById('stopBtn');

        if (dot) dot.classList.remove('online');
        if (text) text.innerText = 'Backend: Offline';
        if (btn) {
            btn.className = 'state-start';
            btn.innerHTML = '<i class="fas fa-play"></i> Start Backend';
        }
        if (startBtn) startBtn.disabled = true;
        if (stopBtn) stopBtn.disabled = true;

        // Close SSE stream
        if (App.disconnectLogStream) App.disconnectLogStream();
    };

    App.setupServerToggle = function () {
        const btn = document.getElementById('serverToggleBtn');
        if (!btn) return;

        btn.addEventListener('click', async () => {
            if (!App.state.serverOnline) {
                // Start server
                App.state.userStartedServer = true;
                btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Starting...';
                try {
                    if (window.electronAPI && window.electronAPI.startBackend) {
                        await window.electronAPI.startBackend();
                    }

                    let attempts = 0;
                    const checkInterval = setInterval(async () => {
                        attempts++;
                        const online = await App.checkBackendStatus();
                        if (online || attempts > 5) {
                            clearInterval(checkInterval);
                            if (!online) {
                                btn.innerHTML = '<i class="fas fa-play"></i> Start Backend';
                                App.log('Failed to start backend server natively.', 'error');
                                App.state.userStartedServer = false;
                            }
                        }
                    }, 1000);
                } catch (err) {
                    console.error(err);
                    App.log('Error spawning backend proc: ' + err, 'error');
                    App.state.userStartedServer = false;
                }
            } else {
                // Stop server
                btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Stopping...';
                btn.disabled = true;
                App.log('Backend Server Shutdown Requested.', 'warning');

                try {
                    const controller = new AbortController();
                    const timeoutId = setTimeout(() => controller.abort(), 2000);
                    await App.apiFetch('/api/shutdown', {
                        method: 'POST',
                        signal: controller.signal
                    });
                    clearTimeout(timeoutId);
                } catch (e) {
                    console.log('Shutdown request sent (connection closed as expected)');
                }

                try {
                    if (window.electronAPI && window.electronAPI.stopBackend) {
                        await window.electronAPI.stopBackend();
                    }
                } catch (err) {
                    console.error('Error stopping backend via Electron:', err);
                }

                setTimeout(() => {
                    App.setServerOffline();
                    btn.disabled = false;
                    App.state.userStartedServer = false;
                    App.log('Backend Server Stopped.', 'info');
                }, 1000);
            }
        });
    };

})(window.App || (window.App = {}));
