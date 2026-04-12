/**
 * config.js — App Configuration + NST + Fingerprint panels
 *
 * Handles app settings, NST browser config, Nexus API key,
 * and fingerprint panel. Proxy management removed.
 */
(function (App) {
    'use strict';

    async function _apiJson(res) {
        const ct = res.headers.get('content-type') || '';
        if (!ct.includes('application/json')) {
            throw new Error(`Server returned HTTP ${res.status} (non-JSON). Restart backend.`);
        }
        return res.json();
    }

    function setVal(id, val) {
        const el = document.getElementById(id);
        if (el && val !== undefined && val !== null) el.value = val;
    }

    // ══════════════════════════════════════════════════════════════════════
    // NST BROWSER CONFIG stub placeholder — proxy removed
    // ══════════════════════════════════════════════════════════════════════

    // No-op stub so any lingering references don't crash
    App.setupProxyPanel = function () {};

    // ══════════════════════════════════════════════════════════════════════
    // (proxy parse helper kept for potential reuse by other modules)
    // ══════════════════════════════════════════════════════════════════════

    function _parseProxy(line) {
        const t = line.trim();
        if (!t || t.startsWith('#')) return null;

        let host, port, user = '', pass = '';

        // Format: socks5://user:pass@host:port or http://user:pass@host:port
        const urlMatch = t.match(/^(socks5|http|https):\/\/(?:([^:]+):([^@]+)@)?([^:]+):(\d+)/i);
        if (urlMatch) {
            const proto = urlMatch[1];
            user = urlMatch[2] || '';
            pass = urlMatch[3] || '';
            host = urlMatch[4];
            port = urlMatch[5];
            return {
                server: `${proto}://${user && pass ? user + ':' + pass + '@' : ''}${host}:${port}`,
                username: user, password: pass,
                display: `${host}:${port}` + (user ? ` (${user.substring(0, 8)}...)` : ''),
            };
        }

        // Format: host:port:user:pass (NSTProxy / common)
        const parts = t.split(':');
        if (parts.length === 4) {
            host = parts[0]; port = parts[1]; user = parts[2]; pass = parts[3];
        } else if (parts.length === 2) {
            host = parts[0]; port = parts[1];
        } else if (parts.length >= 5) {
            // host:port:long:user:name:pass — join user parts
            host = parts[0]; port = parts[1];
            user = parts.slice(2, -1).join(':'); pass = parts[parts.length - 1];
        }

        // Format: user:pass@host:port
        const atMatch = t.match(/^([^@]+)@([^:]+):(\d+)/);
        if (atMatch && !host) {
            const authParts = atMatch[1].split(':');
            user = authParts[0]; pass = authParts.slice(1).join(':');
            host = atMatch[2]; port = atMatch[3];
        }

        if (!host || !port) return null;

        const server = user && pass
            ? `http://${user}:${pass}@${host}:${port}`
            : `http://${host}:${port}`;

        return {
            server, username: user, password: pass,
            display: `${host}:${port}` + (user ? ` (${user.substring(0, 12)}...)` : ''),
        };
    }

    // Expose parser for other modules (kept for potential reuse)
    App.parseProxy = _parseProxy;

    // ══════════════════════════════════════════════════════════════════════
    // NST BROWSER CONFIG (API key + base URL)
    // ══════════════════════════════════════════════════════════════════════

    App.loadNstConfig = async function () {
        if (!App.state.serverOnline) return;
        try {
            const res = await App.apiFetch('/api/nst/config');
            const data = await _apiJson(res);
            if (data.success) {
                setVal('cfg_nst_api_key', data.nst_api_key || '');
                setVal('cfg_nst_api_base', data.nst_api_base || 'http://localhost:8848/api/v2');
            }
        } catch (e) { /* silent */ }
        // Also check status
        App.checkNstStatus();
    };

    App.saveNstConfig = async function () {
        const key = (document.getElementById('cfg_nst_api_key')?.value || '').trim();
        const base = (document.getElementById('cfg_nst_api_base')?.value || '').trim();

        if (!key) {
            App.toast('Enter your NST API key first', 'error');
            return;
        }

        try {
            const res = await App.apiFetch('/api/nst/config', {
                method: 'POST',
                body: JSON.stringify({ nst_api_key: key, nst_api_base: base || 'http://localhost:8848/api/v2' }),
            });
            const data = await _apiJson(res);
            if (data.success) {
                App.toast('NST config saved' + (data.connected ? ' — connected!' : ' — check NST client'), data.connected ? 'success' : 'warning');
                App.checkNstStatus();
            } else {
                App.toast('Save failed: ' + (data.message || 'Unknown'), 'error');
            }
        } catch (e) {
            App.toast('NST config save error', 'error');
        }
    };

    App.checkNstStatus = async function () {
        const dot = document.getElementById('nstStatusDot');
        const txt = document.getElementById('nstStatusText');
        if (txt) txt.textContent = 'Checking...';
        if (dot) dot.style.background = '#f59e0b';

        try {
            const res = await App.apiFetch('/api/nst/status');
            const data = await _apiJson(res);
            if (data.connected) {
                if (dot) dot.style.background = '#10b981';
                if (txt) txt.textContent = 'Connected';
            } else {
                if (dot) dot.style.background = '#ef4444';
                if (txt) txt.textContent = 'Not reachable — start NST Browser';
            }
        } catch (e) {
            if (dot) dot.style.background = '#ef4444';
            if (txt) txt.textContent = 'Error checking status';
        }
    };

    App.setupNstPanel = function () {
        const saveBtn = document.getElementById('saveNstConfigBtn');
        const checkBtn = document.getElementById('checkNstBtn');
        const toggleBtn = document.getElementById('toggleNstKeyBtn');
        const keyInput = document.getElementById('cfg_nst_api_key');

        if (saveBtn) saveBtn.addEventListener('click', App.saveNstConfig);
        if (checkBtn) checkBtn.addEventListener('click', App.checkNstStatus);
        if (toggleBtn && keyInput) {
            toggleBtn.addEventListener('click', () => {
                const isPassword = keyInput.type === 'password';
                keyInput.type = isPassword ? 'text' : 'password';
                toggleBtn.querySelector('i').className = isPassword ? 'fas fa-eye-slash' : 'fas fa-eye';
            });
        }
    };

    // ══════════════════════════════════════════════════════════════════════
    // NEXUS API KEY CONFIG
    // ══════════════════════════════════════════════════════════════════════

    App.loadNexusApiKey = async function () {
        if (!App.state.serverOnline) return;
        try {
            const res = await App.apiFetch('/api/nst/config');
            const data = await _apiJson(res);
            if (data.success) {
                // Load nexus_api_key from the same browser.json endpoint
                const bres = await App.apiFetch('/api/nexus-api-key');
                const bdata = await _apiJson(bres);
                if (bdata.success) {
                    setVal('cfg_nexus_api_key', bdata.nexus_api_key || '');
                }
            }
        } catch (e) { /* silent — endpoint may not exist yet */ }
    };

    App.saveNexusApiKey = async function () {
        const key = (document.getElementById('cfg_nexus_api_key')?.value || '').trim();
        try {
            const res = await App.apiFetch('/api/nexus-api-key', {
                method: 'POST',
                body: JSON.stringify({ nexus_api_key: key }),
            });
            const data = await _apiJson(res);
            if (data.success) {
                App.toast('Nexus API key saved', 'success');
            } else {
                App.toast('Save failed: ' + (data.message || 'Unknown'), 'error');
            }
        } catch (e) {
            App.toast('Nexus API key save error', 'error');
        }
    };

    App.setupNexusApiKeyPanel = function () {
        const saveBtn = document.getElementById('saveNexusApiKeyBtn');
        const toggleBtn = document.getElementById('toggleNexusKeyBtn');
        const genBtn = document.getElementById('generateNexusKeyBtn');
        const keyInput = document.getElementById('cfg_nexus_api_key');

        if (saveBtn) saveBtn.addEventListener('click', App.saveNexusApiKey);
        if (toggleBtn && keyInput) {
            toggleBtn.addEventListener('click', () => {
                const isPassword = keyInput.type === 'password';
                keyInput.type = isPassword ? 'text' : 'password';
                toggleBtn.querySelector('i').className = isPassword ? 'fas fa-eye-slash' : 'fas fa-eye';
            });
        }
        if (genBtn && keyInput) {
            genBtn.addEventListener('click', () => {
                // Generate a random hex key
                const arr = new Uint8Array(16);
                crypto.getRandomValues(arr);
                keyInput.value = Array.from(arr, b => b.toString(16).padStart(2, '0')).join('');
                keyInput.type = 'text';
                if (toggleBtn) toggleBtn.querySelector('i').className = 'fas fa-eye-slash';
            });
        }
    };

    // ══════════════════════════════════════════════════════════════════════
    // FINGERPRINT SETTINGS (minimal — just OS preference for auto-generation)
    // ══════════════════════════════════════════════════════════════════════

    let _fpOsType = 'random';

    App.loadFingerprint = async function () {
        if (!App.state.serverOnline) return;
        try {
            const res = await App.apiFetch('/api/fingerprint');
            const data = await _apiJson(res);
            if (data.success && data.fingerprint) {
                _fpOsType = data.fingerprint.os_type || 'random';
                document.querySelectorAll('.fp-os-btn').forEach(btn => {
                    btn.classList.toggle('active', btn.dataset.os === _fpOsType);
                });
            }
        } catch (e) { /* silent */ }
    };

    App.saveFingerprint = async function () {
        try {
            const res = await App.apiFetch('/api/fingerprint', {
                method: 'POST',
                body: JSON.stringify({ os_type: _fpOsType, auto_timezone: true }),
            });
            const data = await _apiJson(res);
            if (data.success) App.toast('Fingerprint preference saved', 'success');
        } catch (e) {
            App.toast('Save error', 'error');
        }
    };

    App.setupFingerprintPanel = function () {
        document.querySelectorAll('.fp-os-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                _fpOsType = btn.dataset.os;
                document.querySelectorAll('.fp-os-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
            });
        });
        const saveBtn = document.getElementById('saveFingerprintBtn');
        const reloadBtn = document.getElementById('reloadFingerprintBtn');
        if (saveBtn) saveBtn.addEventListener('click', App.saveFingerprint);
        if (reloadBtn) reloadBtn.addEventListener('click', App.loadFingerprint);
    };

    // ══════════════════════════════════════════════════════════════════════
    // APP CONFIG (settings, URLs, delays)
    // ══════════════════════════════════════════════════════════════════════

    App.loadConfig = async function () {
        if (!App.state.serverOnline) return;
        try {
            const res = await App.apiFetch('/api/config');
            const data = await res.json();
            if (!data.success) return;

            const s = data.settings;
            const u = data.urls;

            if (s.browser) {
                const hEl = document.getElementById('cfg_headless');
                if (hEl) hEl.value = String(s.browser.headless ?? false);
                setVal('cfg_slow_mo', s.browser.slow_mo);
                setVal('cfg_timeout', s.browser.timeout);
            }
            if (s.processing) {
                setVal('cfg_workers', s.processing.parallel_accounts);
                setVal('cfg_retries', s.processing.max_retries);
                setVal('cfg_retry_delay', s.processing.retry_delay);
            }

            const urlContainer = document.getElementById('urlFields');
            if (urlContainer && u.urls) {
                urlContainer.innerHTML = '';
                Object.entries(u.urls).forEach(([key, val]) => {
                    const grp = document.createElement('div');
                    grp.className = 'form-group';
                    grp.style.gridColumn = '1 / -1';
                    grp.innerHTML = `<label>${key.replace(/_/g, ' ')}</label>
                        <input type="text" data-url-key="${App.escapeHtml(key)}" value="${App.escapeHtml(val)}" style="font-size:12px;">`;
                    urlContainer.appendChild(grp);
                });
            }

            if (u.delays) {
                setVal('cfg_delay_page_load', u.delays.page_load);
                setVal('cfg_delay_typing', u.delays.typing_delay);
                setVal('cfg_delay_action', u.delays.action_delay);
                setVal('cfg_delay_between', u.delays.between_accounts);
            }
        } catch (e) {
            console.error('Config load error', e);
        }
    };

    App.saveConfig = async function () {
        try {
            const settings = {
                browser: {
                    headless: document.getElementById('cfg_headless')?.value === 'true',
                    slow_mo: parseInt(document.getElementById('cfg_slow_mo')?.value) || 500,
                    timeout: parseInt(document.getElementById('cfg_timeout')?.value) || 60000,
                    viewport: { width: 1280, height: 720 }
                },
                processing: {
                    parallel_accounts: parseInt(document.getElementById('cfg_workers')?.value) || 5,
                    max_retries: parseInt(document.getElementById('cfg_retries')?.value) || 3,
                    retry_delay: parseInt(document.getElementById('cfg_retry_delay')?.value) || 5,
                    continue_on_error: true
                },
                logging: { level: "INFO", file: "logs/bot.log", rotation: "10 MB", retention: "7 days" },
                excel: { input_file: "input/accounts.xlsx", sheet_name: "Accounts",
                    required_columns: ["email","password","recovery_email","recovery_phone","totp_secret","backup_code"] },
                output: { success_folder: "output/success", failed_folder: "output/failed", reports_folder: "output/reports" }
            };

            const urlInputs = document.querySelectorAll('[data-url-key]');
            const urlsMap = {};
            urlInputs.forEach(el => { urlsMap[el.dataset.urlKey] = el.value; });

            const urls = {
                comment: "Direct Google Account URLs",
                urls: urlsMap,
                delays: {
                    page_load: parseFloat(document.getElementById('cfg_delay_page_load')?.value) || 3,
                    typing_delay: parseInt(document.getElementById('cfg_delay_typing')?.value) || 100,
                    action_delay: parseFloat(document.getElementById('cfg_delay_action')?.value) || 1,
                    between_accounts: parseInt(document.getElementById('cfg_delay_between')?.value) || 5
                }
            };

            const res = await App.apiFetch('/api/config', {
                method: 'POST',
                body: JSON.stringify({ settings, urls })
            });
            const data = await res.json();
            if (data.success) App.toast('Configuration saved', 'success');
            else App.toast('Config save failed', 'error');
        } catch (e) {
            App.toast('Network error saving config', 'error');
        }
    };

    App.setupConfigPanel = function () {
        const saveBtn = document.getElementById('saveConfigBtn');
        const reloadBtn = document.getElementById('reloadConfigBtn');
        if (saveBtn) saveBtn.addEventListener('click', App.saveConfig);
        if (reloadBtn) reloadBtn.addEventListener('click', App.loadConfig);
    };

})(window.App || (window.App = {}));
