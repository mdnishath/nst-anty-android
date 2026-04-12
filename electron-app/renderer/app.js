/**
 * app.js — Main coordinator.
 *
 * Initializes shared state, wires up navigation, step selection,
 * and delegates to focused modules loaded before this file:
 *   modules/logs.js        — Log DOM, escapeHtml, tag detection
 *   modules/server.js      — Backend health, detection, online/offline
 *   modules/processing.js  — Start/stop, progress, log stream, file input
 *   modules/config.js      — Config, proxy, fingerprint panels
 *   modules/reports.js     — Reports, templates
 *   modules/debug.js       — Debug browser panel
 *   modules/totp.js        — TOTP generator widget
 */
(function (App) {
    'use strict';

    // ── Shared State ────────────────────────────────────────────────────
    App.state = App.state || {};
    App.state.serverOnline = false;
    App.state.processing = false;
    App.state.currentFilePath = '';
    App.state.lastLogId = 0;
    App.state.userStartedServer = false;

    // ── Navigation ──────────────────────────────────────────────────────
    function setupNavigation() {
        document.querySelectorAll('.nav-item').forEach(item => {
            item.addEventListener('click', (e) => {
                e.preventDefault();
                document.querySelectorAll('.nav-item').forEach(nav => nav.classList.remove('active'));
                item.classList.add('active');

                const targetPage = item.getAttribute('data-page');
                document.querySelectorAll('.page').forEach(page => page.classList.remove('active'));
                document.getElementById(targetPage).classList.add('active');

                if (targetPage === 'results' && App.loadReports) App.loadReports();
                if (targetPage === 'config') {
                    if (App.loadConfig) App.loadConfig();
                    if (App.loadNstConfig) App.loadNstConfig();
                    if (App.loadNexusApiKey) App.loadNexusApiKey();
                }
                if (targetPage === 'tools' && App.loadToolsPage) App.loadToolsPage();
                if (targetPage === 'profiles' && App.loadProfiles) App.loadProfiles();
            });
        });
    }

    // ── Step Selection ──────────────────────────────────────────────────
    const stepInputs = document.querySelectorAll('input[name="botStep"]');
    const stepCards = document.querySelectorAll('.step-card');

    App.getSelectedSteps = function () {
        const selected = [];
        stepInputs.forEach(input => {
            if (input.checked) selected.push(parseInt(input.value));
        });
        return selected.sort((a, b) => a - b);
    };

    function applyStepUI(selectedSteps) {
        const opsListS1 = document.getElementById('operationsListStep1');
        const opsList = document.getElementById('operationsList');
        const opsListS3 = document.getElementById('operationsListStep3');
        const opsListS4 = document.getElementById('operationsListStep4');
        const opsToolS1 = document.getElementById('opsToolbarStep1');
        const opsToolS2 = document.getElementById('opsToolbarStep2');
        const opsToolS3 = document.getElementById('opsToolbarStep3');
        const opsToolS4 = document.getElementById('opsToolbarStep4');
        const pwdGroup = document.getElementById('newPassword')?.closest('.form-group');
        const emailGroup = document.getElementById('recoveryEmailList')?.closest('.form-group');
        const phoneGroup = document.getElementById('recoveryPhoneList')?.closest('.form-group');
        const workerGroup = document.getElementById('numWorkers')?.closest('.form-group');
        const operationsCard = document.getElementById('operationsCard');

        // Hide all
        [opsListS1, opsList, opsListS3, opsListS4, opsToolS1, opsToolS2, opsToolS3, opsToolS4]
            .forEach(el => { if (el) el.classList.add('hidden'); });

        if (pwdGroup) pwdGroup.classList.add('hidden');
        if (emailGroup) emailGroup.classList.add('hidden');
        if (phoneGroup) phoneGroup.classList.add('hidden');
        if (workerGroup) workerGroup.classList.remove('hidden');

        if (selectedSteps.length === 0) {
            if (operationsCard) {
                operationsCard.querySelector('h3').innerHTML = '<i class="fas fa-cogs"></i> 3. Operations';
                operationsCard.querySelector('.card-subtitle').innerText = 'Select at least one step above.';
            }
            return;
        }

        // Show grids/toolbars for selected steps
        selectedSteps.forEach(val => {
            if (val === 1) {
                if (opsListS1) opsListS1.classList.remove('hidden');
                if (opsToolS1) opsToolS1.classList.remove('hidden');
            }
            if (val === 2) {
                if (opsList) opsList.classList.remove('hidden');
                if (opsToolS2) opsToolS2.classList.remove('hidden');
                if (pwdGroup) pwdGroup.classList.remove('hidden');
                if (emailGroup) emailGroup.classList.remove('hidden');
                if (phoneGroup) phoneGroup.classList.remove('hidden');
            }
            if (val === 3) {
                if (opsListS3) opsListS3.classList.remove('hidden');
                if (opsToolS3) opsToolS3.classList.remove('hidden');
            }
            if (val === 4) {
                if (opsListS4) opsListS4.classList.remove('hidden');
                if (opsToolS4) opsToolS4.classList.remove('hidden');
            }
        });

        // Update header
        if (operationsCard) {
            if (selectedSteps.length === 1) {
                const val = selectedSteps[0];
                const headers = {
                    1: ['<i class="fas fa-language"></i> 3. Login Operations', 'Select which Step 1 operations to run after login.'],
                    2: ['<i class="fas fa-cogs"></i> 3. Mutators & Payload', 'Define the vectors to be injected into target accounts.'],
                    3: ['<i class="fas fa-map-marked-alt"></i> 3. Maps Review Operations', 'Select which Google Maps review operations to run.'],
                    4: ['<i class="fas fa-gavel"></i> 3. Appeal Operations', 'Select which account appeal operations to run.'],
                };
                const [h, s] = headers[val] || ['<i class="fas fa-cogs"></i> 3. Operations', ''];
                operationsCard.querySelector('h3').innerHTML = h;
                operationsCard.querySelector('.card-subtitle').innerText = s;
            } else {
                const stepLabels = selectedSteps.map(s => `Step ${s}`).join(' + ');
                operationsCard.querySelector('h3').innerHTML = '<i class="fas fa-layer-group"></i> 3. Combined Operations';
                operationsCard.querySelector('.card-subtitle').innerText = `Configure operations for ${stepLabels}.`;
            }
        }
    }

    function updateLinkToggle() {
        const linkToggle = document.getElementById('linkToggle');
        const linkCheckbox = document.getElementById('linkCheckbox');
        if (!linkToggle || !linkCheckbox) return;

        const selected = App.getSelectedSteps();
        if (selected.length >= 2) {
            linkToggle.classList.remove('disabled');
            linkCheckbox.disabled = false;
        } else {
            linkToggle.classList.add('disabled');
            linkCheckbox.disabled = true;
            linkCheckbox.checked = false;
        }
    }

    // ── Tier Restrictions (removed — all steps unlocked) ───────────────
    App.enforceTierRestrictions = function () { /* no-op */ };

    function setupStepSelection() {
        stepInputs.forEach(input => {
            input.addEventListener('change', () => {
                stepCards.forEach(card => {
                    const cb = card.querySelector('input[type="checkbox"]');
                    if (cb && cb.checked) {
                        card.classList.add('active');
                    } else {
                        card.classList.remove('active');
                    }
                });
                applyStepUI(App.getSelectedSteps());
                updateLinkToggle();
            });
        });
    }

    function setupOpsToolbars() {
        document.querySelectorAll('.ops-toolbar button').forEach(btn => {
            btn.addEventListener('click', () => {
                const targetId = btn.dataset.target;
                const grid = document.getElementById(targetId);
                if (!grid) return;
                const action = btn.dataset.action;
                grid.querySelectorAll('input[type="checkbox"]').forEach(cb => {
                    cb.checked = (action === 'select-all');
                });
            });
        });
    }

    // ── Update Check (removed) ──────────────────────────────────────────
    App.checkForUpdates = async function (manual) {
        // Update checking disabled — no-op
        try {
            const res = await fetch('http://localhost:5000/api/app/version');
            const data = await res.json();
            const verEl = document.getElementById('appVersionText');
            if (verEl) verEl.textContent = data.version || '...';
        } catch (e) {}
    };

    function setupUpdateCheck() {
        var btn = document.getElementById('checkUpdateBtn');
        if (btn) {
            btn.addEventListener('click', function () { App.checkForUpdates(true); });
        }
        var dismiss = document.getElementById('updateBannerDismiss');
        if (dismiss) {
            dismiss.addEventListener('click', function () {
                var banner = document.getElementById('updateBanner');
                if (banner) banner.style.display = 'none';
            });
        }
        // Load version text immediately
        fetch('http://localhost:5000/api/app/version').then(function (r) { return r.json(); }).then(function (d) {
            var el = document.getElementById('appVersionText');
            if (el && d.version) el.textContent = d.version;
        }).catch(function () {});
    }

    // ── Init ────────────────────────────────────────────────────────────
    function init() {
        setupNavigation();
        App.setupServerToggle();
        // Process page removed — guard optional setup calls
        if (App.setupFileInput) App.setupFileInput();
        if (App.setupProcessControls) App.setupProcessControls();
        if (stepInputs.length) setupStepSelection();
        if (document.querySelector('.ops-toolbar')) setupOpsToolbars();
        App.setupDeleteAllReports();
        App.setupGenerateReport();
        App.setupGenerateTemplate();
        App.setupClearLog();
        App.setupConfigPanel();
        App.setupFingerprintPanel();
        if (App.setupNstPanel) App.setupNstPanel();
        if (App.setupNexusApiKeyPanel) App.setupNexusApiKeyPanel();
        App.setupDebugPanel();
        if (App.setupSmsRelayPanel) App.setupSmsRelayPanel();
        if (App.setupToolsPage) App.setupToolsPage();
        if (App.setupProfilesPage) App.setupProfilesPage();
        setupUpdateCheck();

        // Start offline; auto-detect will flip to online if backend is already up
        App.setServerOffline();

        // After login, enforce tier restrictions
        var _origOnAuth = App.onAuthenticated;
        App.onAuthenticated = function () {
            if (typeof _origOnAuth === 'function') _origOnAuth();
            App.enforceTierRestrictions();
        };

        // Start polling for status
        App.startProgressPolling();

        // Auto-detect if backend is already running
        App.autoDetectServer();

        // Auto-detect running operations (restores progress panel after refresh)
        if (App._autoDetectRunningOps) App._autoDetectRunningOps();
    }

    document.addEventListener('DOMContentLoaded', init);

})(window.App || (window.App = {}));
