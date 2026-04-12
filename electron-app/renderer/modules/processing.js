/**
 * processing.js — Start, stop, progress polling, log stream, process buttons.
 *
 * GLITCH FIX: Dashboard progress updates are throttled via _lastProgressUpdate
 * to prevent flickering when multiple pollers (processing.js + profiles.js opPanel)
 * write to the same DOM elements. Only values that actually changed get written.
 */
(function (App) {
    'use strict';

    let logEventSource = null;
    let pollingInterval = null;
    let _lastProgress = {};  // cache to avoid redundant DOM writes

    // ── Recovery email/phone list helpers ─────────────────────────────────

    function _collectRecoveryValues(className) {
        const vals = [];
        document.querySelectorAll('.' + className).forEach(inp => {
            const v = inp.value.trim();
            if (v) vals.push(v);
        });
        return vals.join(',');
    }

    function _addRecoveryItem(listId, className, placeholder, max) {
        const list = document.getElementById(listId);
        if (!list) return;
        if (list.querySelectorAll('.recovery-item').length >= max) {
            alert('Maximum ' + max + ' allowed (Google limit).');
            return;
        }
        const div = document.createElement('div');
        div.className = 'recovery-item';
        div.innerHTML = '<input type="' + (className.includes('email') ? 'email' : 'tel') + '" placeholder="' + placeholder + '" class="' + className + '">' +
            '<button type="button" class="btn-remove-recovery" title="Remove"><i class="fas fa-times"></i></button>';
        list.appendChild(div);
        div.querySelector('.btn-remove-recovery').addEventListener('click', () => {
            div.remove();
            _updateRemoveBtns(listId);
        });
        _updateRemoveBtns(listId);
    }

    function _updateRemoveBtns(listId) {
        const list = document.getElementById(listId);
        if (!list) return;
        const items = list.querySelectorAll('.recovery-item');
        items.forEach(item => {
            const btn = item.querySelector('.btn-remove-recovery');
            if (btn) btn.style.display = items.length > 1 ? 'inline-block' : 'none';
        });
    }

    function _setupRecoveryLists() {
        const addEmailBtn = document.getElementById('addRecoveryEmail');
        if (addEmailBtn) addEmailBtn.addEventListener('click', () => _addRecoveryItem('recoveryEmailList', 'recovery-email-input', 'e.g. secure@domain.com', 10));
        const addPhoneBtn = document.getElementById('addRecoveryPhone');
        if (addPhoneBtn) addPhoneBtn.addEventListener('click', () => _addRecoveryItem('recoveryPhoneList', 'recovery-phone-input', 'e.g. +1234567890', 10));
        document.querySelectorAll('#recoveryEmailList .btn-remove-recovery').forEach(btn =>
            btn.addEventListener('click', () => { btn.closest('.recovery-item').remove(); _updateRemoveBtns('recoveryEmailList'); }));
        document.querySelectorAll('#recoveryPhoneList .btn-remove-recovery').forEach(btn =>
            btn.addEventListener('click', () => { btn.closest('.recovery-item').remove(); _updateRemoveBtns('recoveryPhoneList'); }));
    }

    // ── SSE Log Stream ──────────────────────────────────────────────────

    App.connectLogStream = function () {
        if (logEventSource) { logEventSource.close(); logEventSource = null; }
        let sseUrl = 'http://localhost:5000/api/log-stream';
        if (App.state.apiToken) sseUrl += '?token=' + encodeURIComponent(App.state.apiToken);
        logEventSource = new EventSource(sseUrl);

        logEventSource.onmessage = (event) => {
            try {
                const log = JSON.parse(event.data);
                if (log.id > App.state.lastLogId) {
                    App.appendLogDOM(log.message, log.type || 'info');
                    App.state.lastLogId = log.id;
                    const liveLog = document.getElementById('liveLog');
                    if (liveLog) liveLog.scrollTop = liveLog.scrollHeight;
                }
            } catch (e) { /* ignore */ }
        };

        logEventSource.onerror = () => {
            if (!App.state.serverOnline) { logEventSource.close(); logEventSource = null; }
        };
    };

    App.disconnectLogStream = function () {
        if (logEventSource) { logEventSource.close(); logEventSource = null; }
    };

    // ── Smooth DOM setters (only write if value changed) ────────────────

    function _setEl(id, val) {
        if (_lastProgress[id] === val) return;
        _lastProgress[id] = val;
        const el = document.getElementById(id);
        if (el) el.innerText = val;
    }

    function _setLabel(valueId, label, iconClass) {
        const key = valueId + '_lbl';
        if (_lastProgress[key] === label) return;
        _lastProgress[key] = label;
        const el = document.getElementById(valueId);
        if (!el) return;
        const card = el.closest('.stat-card');
        if (!card) return;
        const labelEl = card.querySelector('.stat-label');
        if (labelEl) labelEl.innerText = label;
        const iconEl = card.querySelector('.stat-icon i');
        if (iconEl) iconEl.className = 'fas ' + iconClass;
    }

    function _setBarSmooth(pct) {
        if (_lastProgress._barPct === pct) return;
        _lastProgress._barPct = pct;
        const bar = document.getElementById('progressBar');
        const pctEl = document.getElementById('progressPercentage');
        if (bar) bar.style.width = pct + '%';
        if (pctEl) pctEl.innerText = pct + '%';
    }

    // ── Progress Polling ────────────────────────────────────────────────

    App.startProgressPolling = function () {
        if (pollingInterval) clearInterval(pollingInterval);
        App.connectLogStream();

        pollingInterval = setInterval(async () => {
            if (!App.state.serverOnline) return;

            // Note: opProgressPanel also updates dashboard elements — that's fine,
            // both sources write the same values so no conflict

            try {
                const res = await App.apiFetch('/api/progress');
                const data = await res.json();

                if (data.success && data.progress) {
                    const p = data.progress;
                    const isBatchLogin = p.job_type === 'batch_login';
                    const isBulkRelogin = p.job_type === 'bulk_relogin';

                    // Current account label
                    const currentAccount = document.getElementById('currentAccount');
                    if (currentAccount) {
                        let text;
                        if (isBatchLogin && p.status === 'processing') text = p.current_account ? `Logging in: ${p.current_account}` : 'BATCH LOGIN RUNNING...';
                        else if (isBatchLogin && p.status === 'completed') text = 'BATCH LOGIN COMPLETE';
                        else if (isBulkRelogin && p.status === 'processing') text = p.current_account ? `Re-logging in: ${p.current_account}` : 'BULK RE-LOGIN RUNNING...';
                        else if (isBulkRelogin && p.status === 'completed') text = 'BULK RE-LOGIN COMPLETE';
                        else text = p.current_account || (App.state.processing ? 'FETCHING_NEXT...' : 'WAITING_FOR_JOB');
                        if (currentAccount.innerText !== text) currentAccount.innerText = text;
                    }

                    // Step indicator
                    const stepEl = document.getElementById('stepIndicator');
                    if (stepEl) {
                        let stepText = '';
                        if (isBatchLogin) stepText = 'BATCH LOGIN';
                        else if (isBulkRelogin) stepText = 'BULK RE-LOGIN';
                        else if (p.step_label) stepText = p.step_label;
                        else if (!App.state.processing) stepText = '';
                        if (stepEl.innerText !== stepText) stepEl.innerText = stepText;
                    }

                    // Progress bar (smooth)
                    {
                        const pct = p.total > 0 ? Math.floor((p.current / p.total) * 100)
                                  : p.status === 'completed' ? 100 : 0;
                        _setBarSmooth(pct);
                        const txtEl = document.getElementById('progressText');
                        const txt = `Progress: ${p.current} / ${p.total}`;
                        if (txtEl && txtEl.innerText !== txt) txtEl.innerText = txt;
                    }

                    // HUD counters (only write on change)
                    if (p.total   !== undefined) _setEl('totalAccounts', p.total);
                    if (p.success !== undefined) _setEl('totalSuccess', p.success);
                    if (p.failed  !== undefined) _setEl('totalFailed', p.failed);
                    if (p.pending !== undefined) _setEl('totalPending', p.pending);

                    // Labels
                    if (isBatchLogin) {
                        _setLabel('totalAccounts', 'Total Accounts', 'fa-users');
                        _setLabel('totalSuccess', 'Logged In', 'fa-check-circle');
                        _setLabel('totalFailed', 'Failed', 'fa-times-circle');
                        _setLabel('totalPending', 'Remaining', 'fa-hourglass-half');
                    } else if (isBulkRelogin) {
                        _setLabel('totalAccounts', 'Total Profiles', 'fa-users');
                        _setLabel('totalSuccess', 'Re-Logged In', 'fa-check-circle');
                        _setLabel('totalFailed', 'Failed', 'fa-times-circle');
                        _setLabel('totalPending', 'Remaining', 'fa-hourglass-half');
                    } else if (p.live_mode) {
                        _setLabel('totalSuccess', 'Live', 'fa-check-circle');
                        _setLabel('totalFailed', 'Missing', 'fa-times-circle');
                    } else {
                        _setLabel('totalSuccess', 'Successful', 'fa-check-circle');
                        _setLabel('totalFailed', 'Failed', 'fa-exclamation-circle');
                        _setLabel('totalPending', 'Pending / Skipped', 'fa-hourglass-half');
                    }

                    // State transitions
                    if (p.status === 'processing' && !App.state.processing) {
                        if (!isBatchLogin && !isBulkRelogin) {
                            App.state.processing = true;
                            App.updateProcessButtons();
                        }
                    } else if (p.status !== 'processing' && App.state.processing) {
                        App.state.processing = false;
                        App.updateProcessButtons();
                        if (p.status === 'completed') {
                            App.log('Processing complete', 'success');
                            App.toast('Processing complete', 'success');
                        }
                        if (p.status === 'stopped') {
                            App.log('Job stopped by user', 'warning');
                            App.toast('Processing stopped', 'warning');
                        }
                    }
                }
            } catch (e) { /* ignore */ }
        }, 1500);  // 1.5s instead of 1s — less flickering, still responsive
    };

    // ── Process Buttons ─────────────────────────────────────────────────

    App.updateProcessButtons = function () {
        const startBtn = document.getElementById('startBtn');
        const stopBtn = document.getElementById('stopBtn');
        const selectFileBtn = document.getElementById('selectFileBtn');
        if (!App.state.serverOnline) {
            if (startBtn) startBtn.disabled = true;
            if (stopBtn) stopBtn.disabled = true;
            return;
        }
        if (App.state.processing) {
            if (startBtn) startBtn.disabled = true;
            if (stopBtn) stopBtn.disabled = false;
            if (selectFileBtn) selectFileBtn.disabled = true;
        } else {
            if (startBtn) startBtn.disabled = !App.state.currentFilePath;
            if (stopBtn) stopBtn.disabled = true;
            if (selectFileBtn) selectFileBtn.disabled = false;
        }
    };

    App.setupProcessControls = function () {
        const startBtn = document.getElementById('startBtn');
        const stopBtn = document.getElementById('stopBtn');
        const resetBtn = document.getElementById('resetBtn');
        if (startBtn) startBtn.addEventListener('click', _startProcessing);
        if (stopBtn) stopBtn.addEventListener('click', _stopProcessing);
        if (resetBtn) resetBtn.addEventListener('click', _resetDashboard);
        _setupRecoveryLists();

        const processGenPassBtn = document.getElementById('processGenPassBtn');
        if (processGenPassBtn) {
            processGenPassBtn.addEventListener('click', () => {
                const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789!@#$%&*_+-=?';
                let pwd = '';
                for (let i = 0; i < 16; i++) pwd += chars[Math.floor(Math.random() * chars.length)];
                document.getElementById('newPassword').value = pwd;
            });
        }
    };

    async function _startProcessing() {
        if (!App.state.currentFilePath) return alert('Select an Excel file first.');
        const selectedSteps = App.getSelectedSteps();
        if (selectedSteps.length === 0) return alert('Select at least one step.');

        const linkCheckbox = document.getElementById('linkCheckbox');
        const linked = linkCheckbox && linkCheckbox.checked && selectedSteps.length >= 2;
        const opsMap = {};
        let allOps = [];
        const stepSels = {
            1: '#operationsListStep1 input[type="checkbox"]:checked',
            2: '#operationsList input[type="checkbox"]:checked',
            3: '#operationsListStep3 input[type="checkbox"]:checked',
            4: '#operationsListStep4 input[type="checkbox"]:checked',
        };
        for (const step of selectedSteps) {
            const ops = [];
            document.querySelectorAll(stepSels[step]).forEach(el => ops.push(el.value));
            if (!ops.length) return alert(`Select at least one operation for Step ${step}.`);
            opsMap[String(step)] = ops.join(',');
            allOps = allOps.concat(ops);
        }

        try {
            const res = await App.apiFetch('/api/start-processing', {
                method: 'POST',
                body: JSON.stringify({
                    file_path: App.state.currentFilePath,
                    operations: allOps.join(','),
                    new_password: document.getElementById('newPassword')?.value || '',
                    recovery_email: _collectRecoveryValues('recovery-email-input'),
                    recovery_phone: _collectRecoveryValues('recovery-phone-input'),
                    num_workers: parseInt(document.getElementById('numWorkers')?.value) || 5,
                    bot_step: selectedSteps[0],
                    bot_steps: selectedSteps,
                    linked, ops_per_step: opsMap,
                })
            });
            const data = await res.json();
            if (data.success) {
                App.state.processing = true;
                App.updateProcessButtons();
                App.log('Processing started...', 'info');
            } else {
                alert('Failed: ' + data.message);
            }
        } catch (e) {
            App.log('Network error: ' + e, 'error');
        }
    }

    async function _stopProcessing() {
        if (!confirm('Stop processing?')) return;
        try {
            await App.apiFetch('/api/stop-processing', { method: 'POST' });
            App.log('Stop signal sent...', 'warning');
        } catch (e) { console.error('Stop error', e); }
    }

    async function _resetDashboard() {
        if (App.state.processing) return alert('Stop the job first.');
        App.state.currentFilePath = '';
        _lastProgress = {};
        const ids = ['filePath', 'newPassword'];
        ids.forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
        ['infoRows', 'infoPending'].forEach(id => _setEl(id, '-'));
        ['totalAccounts', 'totalSuccess', 'totalFailed', 'totalPending'].forEach(id => { _lastProgress[id] = undefined; _setEl(id, '0'); });

        const progressText = document.getElementById('progressText');
        if (progressText) progressText.innerText = 'Idle';
        _setBarSmooth(0);
        const currentAccount = document.getElementById('currentAccount');
        if (currentAccount) currentAccount.innerText = 'WAITING_FOR_JOB';

        const liveLog = document.getElementById('liveLog');
        if (liveLog) liveLog.innerHTML = '';
        App.state.lastLogId = 0;

        ['#operationsListStep1', '#operationsList', '#operationsListStep3', '#operationsListStep4']
            .forEach(sel => document.querySelectorAll(`${sel} input[type="checkbox"]`).forEach(cb => cb.checked = false));
        ['recoveryEmailList', 'recoveryPhoneList'].forEach(listId => {
            const list = document.getElementById(listId);
            if (!list) return;
            const items = list.querySelectorAll('.recovery-item');
            for (let i = 1; i < items.length; i++) items[i].remove();
            const first = list.querySelector('input');
            if (first) first.value = '';
            _updateRemoveBtns(listId);
        });

        try { await App.apiFetch('/api/logs/clear', { method: 'POST' }); } catch (e) { /* ok */ }
        App.log('Dashboard reset.', 'info');
        App.updateProcessButtons();
    }

    // ── File Selection ──────────────────────────────────────────────────

    App.setupFileInput = function () {
        const btn = document.getElementById('selectFileBtn');
        if (!btn) return;
        btn.addEventListener('click', async () => {
            if (!window.electronAPI) return alert('Electron API not available.');
            const filePath = await window.electronAPI.selectFile();
            if (filePath) {
                App.state.currentFilePath = filePath;
                const display = document.getElementById('filePath');
                if (display) display.value = filePath;
                if (App.state.serverOnline) _fetchFileInfo(filePath);
            }
        });
    };

    async function _fetchFileInfo(filePath) {
        try {
            const response = await App.apiFetch('/api/file-info', {
                method: 'POST', headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ file_path: filePath })
            });
            const data = await response.json();
            if (data.success) {
                _setEl('infoRows', data.data.total);
                _setEl('infoPending', data.data.pending);
                _setEl('totalAccounts', data.data.total);
                _setEl('totalPending', data.data.pending);
                _setEl('totalSuccess', data.data.success);
                _setEl('totalFailed', data.data.failed);
                App.updateProcessButtons();
                App.log(`Loaded: ${data.data.total} records.`, 'info');
            } else {
                App.log('File read failed: ' + data.message, 'error');
            }
        } catch (e) { App.log('Network error: ' + e, 'error'); }
    }

    // ── Clear Log ───────────────────────────────────────────────────────

    App.setupClearLog = function () {
        const btn = document.getElementById('clearLogBtn');
        if (!btn) return;
        btn.addEventListener('click', async () => {
            if (App.state.serverOnline) {
                try {
                    const res = await App.apiFetch('/api/logs/clear', { method: 'POST' });
                    const data = await res.json();
                    if (data.clear_id) App.state.lastLogId = data.clear_id;
                } catch (e) { /* ok */ }
            }
            const liveLog = document.getElementById('liveLog');
            if (liveLog) liveLog.innerHTML = '';
            App.log('Log cleared.', 'info');
        });
    };

    // ── SMS Relay Panel ─────────────────────────────────────────────────

    App.setupSmsRelayPanel = function () {
        const testBtn = document.getElementById('smsTestBtn');
        const checkBtn = document.getElementById('smsCheckBtn');
        if (!testBtn) return;
        const lastCode = document.getElementById('smsLastCode');
        const lastSender = document.getElementById('smsLastSender');
        const relayStatus = document.getElementById('smsRelayStatus');
        const testResult = document.getElementById('smsTestResult');

        testBtn.addEventListener('click', async () => {
            testResult.innerText = 'Sending...';
            try {
                const res = await App.apiFetch('/api/sms-code', {
                    method: 'POST', headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ code: String(Math.floor(100000 + Math.random() * 900000)), sender: '+880TEST', full_message: 'Test from UI', timestamp: Date.now() / 1000 })
                });
                const data = await res.json();
                testResult.innerText = data.success ? 'Test code sent!' : 'Failed';
                testResult.style.color = data.success ? '#2ecc71' : '#e74c3c';
            } catch (e) { testResult.innerText = 'Backend offline'; testResult.style.color = '#e74c3c'; }
            setTimeout(() => { testResult.innerText = ''; }, 5000);
        });

        checkBtn.addEventListener('click', async () => {
            try {
                const res = await App.apiFetch('/api/sms-code?max_age=9999&peek=1');
                const data = await res.json();
                if (data.success && data.code) {
                    lastCode.innerText = data.code;
                    lastSender.innerText = data.sender || 'unknown';
                    relayStatus.innerText = 'Code available'; relayStatus.style.color = '#2ecc71';
                } else {
                    lastCode.innerText = '--';
                    relayStatus.innerText = 'No codes'; relayStatus.style.color = '#f39c12';
                }
            } catch (e) { relayStatus.innerText = 'Offline'; relayStatus.style.color = '#e74c3c'; }
        });

        setInterval(async () => {
            if (!document.getElementById('dashboard')?.classList.contains('active')) return;
            if (!App.state.serverOnline) return;
            try {
                const res = await App.apiFetch('/api/sms-code?max_age=300&peek=1');
                const data = await res.json();
                if (data.success && data.code) {
                    lastCode.innerText = data.code; lastSender.innerText = data.sender || '';
                    relayStatus.innerText = 'Code available'; relayStatus.style.color = '#2ecc71';
                }
            } catch (e) { /* silent */ }
        }, 5000);
    };

})(window.App || (window.App = {}));
