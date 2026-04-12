/**
 * debug.js — Debug browser panel.
 */
(function (App) {
    'use strict';

    let _debugRunning = false;
    let _debugPollTimer = null;

    function _getDebugUrl() {
        const preset = document.getElementById('debugUrlPreset');
        if (!preset) return 'https://ipinfo.io';
        if (preset.value === 'custom') {
            const custom = document.getElementById('debugCustomUrl');
            return (custom && custom.value.trim()) ? custom.value.trim() : 'https://ipinfo.io';
        }
        return preset.value;
    }

    function _setDebugStatus(state, text) {
        const dot = document.getElementById('debugStatusDot');
        const span = document.getElementById('debugStatusText');
        const closeBtn = document.getElementById('debugCloseBtn');
        const launchBtn = document.getElementById('debugLaunchBtn');

        if (dot) dot.className = 'debug-status-dot ' + state;
        if (span) span.textContent = text;

        _debugRunning = (state === 'running');
        if (closeBtn) closeBtn.disabled = !_debugRunning;
        if (launchBtn) launchBtn.disabled = _debugRunning;
    }

    async function _launchDebugBrowsers() {
        if (!App.state.serverOnline) {
            App.log('Start the backend server first before launching debug browsers.', 'error');
            return;
        }

        const numEl = document.getElementById('debugNumBrowsers');
        const num = numEl ? Math.max(1, Math.min(parseInt(numEl.value) || 1, 10)) : 1;
        const url = _getDebugUrl();

        _setDebugStatus('running', `Launching ${num} debug browser(s)\u2026`);

        try {
            const res = await App.apiFetch('/api/debug/launch', {
                method: 'POST',
                body: JSON.stringify({ num_browsers: num, test_url: url }),
            });
            const data = await res.json();

            if (data.success) {
                App.log(data.message, 'success');
                _setDebugStatus('running', `${num} debug browser(s) open \u2014 inspect and close manually or click Close All.`);
                _startDebugPoll();
            } else {
                App.log('Debug launch failed: ' + data.message, 'error');
                _setDebugStatus('idle', 'Launch failed \u2014 check log.');
            }
        } catch (e) {
            App.log('Network error launching debug browsers: ' + e, 'error');
            _setDebugStatus('idle', 'Network error.');
        }
    }

    async function _closeDebugBrowsers() {
        try {
            const res = await App.apiFetch('/api/debug/close', { method: 'POST' });
            const data = await res.json();
            if (data.success) {
                App.log(data.message, 'info');
                _setDebugStatus('closed', 'Debug browsers closed.');
                _stopDebugPoll();
                setTimeout(() => _setDebugStatus('idle', 'No debug browsers running'), 3000);
            } else {
                App.log('Close error: ' + data.message, 'error');
            }
        } catch (e) {
            App.log('Network error closing debug browsers: ' + e, 'error');
        }
    }

    function _startDebugPoll() {
        _stopDebugPoll();
        _debugPollTimer = setInterval(_pollDebugStatus, 2000);
    }

    function _stopDebugPoll() {
        if (_debugPollTimer) { clearInterval(_debugPollTimer); _debugPollTimer = null; }
    }

    async function _pollDebugStatus() {
        if (!App.state.serverOnline) { _stopDebugPoll(); return; }
        try {
            const res = await App.apiFetch('/api/debug/status');
            const data = await res.json();
            if (data.success) {
                if (!data.running && data.open === 0) {
                    _setDebugStatus('closed', 'All debug browsers were closed.');
                    _stopDebugPoll();
                    setTimeout(() => _setDebugStatus('idle', 'No debug browsers running'), 3000);
                } else if (data.open < data.total && data.total > 0) {
                    _setDebugStatus('running', `${data.open}/${data.total} debug browser(s) still open.`);
                }
            }
        } catch (e) {
            _stopDebugPoll();
        }
    }

    App.setupDebugPanel = function () {
        const preset = document.getElementById('debugUrlPreset');
        const customRow = document.getElementById('debugCustomUrlRow');
        if (preset) {
            preset.addEventListener('change', () => {
                if (customRow) customRow.classList.toggle('hidden', preset.value !== 'custom');
            });
        }

        const launchBtn = document.getElementById('debugLaunchBtn');
        const closeBtn = document.getElementById('debugCloseBtn');
        if (launchBtn) launchBtn.addEventListener('click', _launchDebugBrowsers);
        if (closeBtn) closeBtn.addEventListener('click', _closeDebugBrowsers);
    };

})(window.App || (window.App = {}));
