/**
 * modules/tools.js — Tools page: Screenshots, Auth Files, Cleaner, Password.
 */
(function (App) {
    'use strict';

    const API_BASE = 'http://localhost:5000';

    // ── State ─────────────────────────────────────────────────────────────
    let screenshotPage = 1;
    let authFilePage = 1;
    const PER_PAGE = 50;
    let screenshotDebounce = null;
    let authFileDebounce = null;
    let pendingCleanupCategory = null;

    // ── Helpers ───────────────────────────────────────────────────────────

    function esc(str) {
        const d = document.createElement('div');
        d.textContent = str;
        return d.innerHTML;
    }

    function fmtSize(bytes) {
        if (!bytes || bytes === 0) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB'];
        let i = 0;
        let v = bytes;
        while (v >= 1024 && i < units.length - 1) { v /= 1024; i++; }
        return v.toFixed(i === 0 ? 0 : 1) + ' ' + units[i];
    }

    // ── Tab Switching ─────────────────────────────────────────────────────

    function setupTabs() {
        document.querySelectorAll('.tools-tab').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.tools-tab').forEach(t => t.classList.remove('active'));
                btn.classList.add('active');

                const tab = btn.getAttribute('data-tools-tab');
                document.querySelectorAll('.tools-tab-content').forEach(c => c.classList.remove('active'));
                const target = document.querySelector('[data-tools-tab-content="' + tab + '"]');
                if (target) target.classList.add('active');

                // Load data for the active tab
                if (tab === 'screenshots') loadScreenshots();
                if (tab === 'authfiles') loadAuthFiles();
                if (tab === 'cleaner') loadStorageStats();
                if (tab === 'license') loadLicenseInfo();
            });
        });
    }

    // ── Screenshots ───────────────────────────────────────────────────────

    async function loadScreenshots() {
        const search = (document.getElementById('screenshotSearch') || {}).value || '';
        const listEl = document.getElementById('screenshotList');
        const countEl = document.getElementById('screenshotCount');
        const pagEl = document.getElementById('screenshotPagination');
        if (!listEl) return;

        listEl.innerHTML = '<div class="tools-empty"><i class="fas fa-spinner fa-spin"></i> Loading...</div>';

        try {
            const res = await App.apiFetch('/api/tools/screenshots?search=' + encodeURIComponent(search) +
                '&page=' + screenshotPage + '&per_page=' + PER_PAGE);
            const data = await res.json();

            if (!data.files || data.files.length === 0) {
                listEl.innerHTML = '<div class="tools-empty"><i class="fas fa-image"></i> No screenshots found</div>';
                if (countEl) countEl.textContent = '0 files';
                if (pagEl) pagEl.innerHTML = '';
                return;
            }

            if (countEl) countEl.textContent = data.total + ' files';

            let html = '';
            data.files.forEach(f => {
                html += '<div class="tools-file-item" data-screenshot="' + esc(f.name) + '">' +
                    '<i class="fas fa-image file-icon"></i>' +
                    '<span class="file-name">' + esc(f.name) + '</span>' +
                    '<span class="file-meta">' + fmtSize(f.size) + ' &middot; ' + esc(f.modified || '') + '</span>' +
                    '</div>';
            });
            listEl.innerHTML = html;

            // Wire clicks
            listEl.querySelectorAll('.tools-file-item[data-screenshot]').forEach(el => {
                el.addEventListener('click', () => openScreenshotModal(el.dataset.screenshot));
            });

            // Pagination
            renderPagination(pagEl, data.page, data.total_pages, (p) => {
                screenshotPage = p;
                loadScreenshots();
            });

        } catch (err) {
            listEl.innerHTML = '<div class="tools-empty"><i class="fas fa-exclamation-triangle"></i> Failed to load: ' + esc(String(err)) + '</div>';
        }
    }

    function openScreenshotModal(filename) {
        const modal = document.getElementById('screenshotModal');
        const img = document.getElementById('screenshotModalImg');
        const nameEl = document.getElementById('screenshotModalName');
        if (!modal || !img) return;

        const token = App.state.apiToken || '';
        img.src = API_BASE + '/api/tools/screenshot/' + encodeURIComponent(filename) + '?token=' + encodeURIComponent(token);
        if (nameEl) nameEl.textContent = filename;
        modal.classList.add('open');
    }

    // ── Auth Files ────────────────────────────────────────────────────────

    async function loadAuthFiles() {
        const search = (document.getElementById('authFileSearch') || {}).value || '';
        const listEl = document.getElementById('authFileList');
        const countEl = document.getElementById('authFileCount');
        const pagEl = document.getElementById('authFilePagination');
        if (!listEl) return;

        listEl.innerHTML = '<div class="tools-empty"><i class="fas fa-spinner fa-spin"></i> Loading...</div>';

        try {
            const res = await App.apiFetch('/api/tools/auth-files?search=' + encodeURIComponent(search) +
                '&page=' + authFilePage + '&per_page=' + PER_PAGE);
            const data = await res.json();

            if (!data.files || data.files.length === 0) {
                listEl.innerHTML = '<div class="tools-empty"><i class="fas fa-file-alt"></i> No authenticator files found</div>';
                if (countEl) countEl.textContent = '0 files';
                if (pagEl) pagEl.innerHTML = '';
                return;
            }

            if (countEl) countEl.textContent = data.total + ' files';

            let html = '';
            data.files.forEach(f => {
                const isAuth = f.name.startsWith('authenticator_key');
                const badgeClass = isAuth ? 'badge-auth' : 'badge-backup';
                const badgeText = isAuth ? 'AUTH KEY' : 'BACKUP';
                html += '<div class="tools-file-item" data-authfile="' + esc(f.name) + '">' +
                    '<i class="fas fa-file-alt file-icon"></i>' +
                    '<span class="file-name">' + esc(f.name) + '</span>' +
                    '<span class="file-badge ' + badgeClass + '">' + badgeText + '</span>' +
                    '<span class="file-meta">' + fmtSize(f.size) + ' &middot; ' + esc(f.modified || '') + '</span>' +
                    '</div>';
            });
            listEl.innerHTML = html;

            // Wire clicks
            listEl.querySelectorAll('.tools-file-item[data-authfile]').forEach(el => {
                el.addEventListener('click', () => openTextModal(el.dataset.authfile));
            });

            // Pagination
            renderPagination(pagEl, data.page, data.total_pages, (p) => {
                authFilePage = p;
                loadAuthFiles();
            });

        } catch (err) {
            listEl.innerHTML = '<div class="tools-empty"><i class="fas fa-exclamation-triangle"></i> Failed to load: ' + esc(String(err)) + '</div>';
        }
    }

    async function openTextModal(filename) {
        const modal = document.getElementById('textViewerModal');
        const titleEl = document.getElementById('textViewerTitle');
        const contentEl = document.getElementById('textViewerContent');
        if (!modal || !contentEl) return;

        if (titleEl) titleEl.textContent = filename;
        contentEl.textContent = 'Loading...';
        modal.classList.add('open');

        try {
            const res = await App.apiFetch('/api/tools/auth-file/' + encodeURIComponent(filename));
            const data = await res.json();
            contentEl.textContent = data.content || '(empty)';
        } catch (err) {
            contentEl.textContent = 'Error: ' + String(err);
        }
    }

    // ── Pagination Helper ─────────────────────────────────────────────────

    function renderPagination(container, currentPage, totalPages, onPageChange) {
        if (!container || totalPages <= 1) {
            if (container) container.innerHTML = '';
            return;
        }

        container.innerHTML =
            '<button class="prev-btn" ' + (currentPage <= 1 ? 'disabled' : '') + '><i class="fas fa-chevron-left"></i> Prev</button>' +
            '<span class="page-info">Page ' + currentPage + ' of ' + totalPages + '</span>' +
            '<button class="next-btn" ' + (currentPage >= totalPages ? 'disabled' : '') + '>Next <i class="fas fa-chevron-right"></i></button>';

        container.querySelector('.prev-btn').addEventListener('click', () => {
            if (currentPage > 1) onPageChange(currentPage - 1);
        });
        container.querySelector('.next-btn').addEventListener('click', () => {
            if (currentPage < totalPages) onPageChange(currentPage + 1);
        });
    }

    // ── Storage Stats & Cleanup ───────────────────────────────────────────

    async function loadStorageStats() {
        try {
            const res = await App.apiFetch('/api/tools/storage-stats');
            const data = await res.json();
            const s = data.stats || {};

            setStatText('statScreenshots', s.screenshots);
            setStatText('statAuthKeys', s.authenticator);
            setStatText('statBackupCodes', s.backup_codes);

            // Log is a single file
            const logEl = document.getElementById('statBackendLog');
            if (logEl && s.log) {
                logEl.textContent = fmtSize(s.log.total_size);
            }
        } catch (err) {
            // silently fail
        }
    }

    function setStatText(elId, stat) {
        const el = document.getElementById(elId);
        if (el && stat) {
            el.textContent = stat.count + ' files (' + fmtSize(stat.total_size) + ')';
        }
    }

    function setupCleanupButtons() {
        document.querySelectorAll('.tools-delete-btn[data-cleanup]').forEach(btn => {
            btn.addEventListener('click', () => {
                pendingCleanupCategory = btn.dataset.cleanup;
                const labels = {
                    screenshots: 'all screenshot files',
                    authenticator: 'all authenticator key files',
                    backup_codes: 'all backup code files',
                    log: 'the backend log file',
                };
                const msg = document.getElementById('cleanupConfirmMsg');
                if (msg) msg.textContent = 'Are you sure you want to permanently delete ' + (labels[pendingCleanupCategory] || 'these files') + '? This cannot be undone.';
                document.getElementById('cleanupConfirmOverlay').classList.add('open');
            });
        });

        const cancelBtn = document.getElementById('cleanupCancelBtn');
        if (cancelBtn) {
            cancelBtn.addEventListener('click', () => {
                document.getElementById('cleanupConfirmOverlay').classList.remove('open');
                pendingCleanupCategory = null;
            });
        }

        const confirmBtn = document.getElementById('cleanupConfirmBtn');
        if (confirmBtn) {
            confirmBtn.addEventListener('click', async () => {
                if (!pendingCleanupCategory) return;
                document.getElementById('cleanupConfirmOverlay').classList.remove('open');

                try {
                    const res = await App.apiFetch('/api/tools/cleanup', {
                        method: 'POST',
                        body: JSON.stringify({ category: pendingCleanupCategory }),
                    });
                    const data = await res.json();
                    if (data.success) {
                        App.toast('Deleted ' + (data.deleted || 0) + ' item(s)', 'success');
                    } else {
                        App.toast(data.message || 'Cleanup failed', 'error');
                    }
                } catch (err) {
                    App.toast('Cleanup failed: ' + String(err), 'error');
                }

                pendingCleanupCategory = null;
                loadStorageStats();
            });
        }

        // Refresh button
        const refreshBtn = document.getElementById('refreshStatsBtn');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', loadStorageStats);
        }
    }

    // ── License Info ────────────────────────────────────────────────────────

    function setupLicenseInfo() {
        const deactivateBtn = document.getElementById('licenseDeactivateBtn');
        if (deactivateBtn) {
            deactivateBtn.addEventListener('click', async () => {
                if (!confirm('Are you sure you want to deactivate your license?\nYou will need to re-enter your license key.')) {
                    return;
                }
                try {
                    const res = await App.apiFetch('/api/license/deactivate', { method: 'POST' });
                    const data = await res.json();
                    if (data.success) {
                        App.toast('License deactivated', 'success');
                        location.reload();
                    } else {
                        App.toast(data.message || 'Deactivation failed', 'error');
                    }
                } catch (err) {
                    App.toast('Error: ' + String(err), 'error');
                }
            });
        }

    }

    async function loadLicenseInfo() {
        const panel = document.getElementById('licenseInfoPanel');
        const loading = document.getElementById('licenseInfoLoading');
        if (!panel) return;

        try {
            const res = await App.apiFetch('/api/license/info');
            const data = await res.json();
            if (loading) loading.style.display = 'none';
            panel.style.display = '';

            // Populate fields
            const keyEl = document.getElementById('licenseInfoKey');
            const statusEl = document.getElementById('licenseInfoStatus');
            const idEl = document.getElementById('licenseInfoId');
            const expiryEl = document.getElementById('licenseInfoExpiry');
            const daysEl = document.getElementById('licenseInfoDays');
            const machineEl = document.getElementById('licenseInfoMachine');

            if (keyEl) {
                keyEl.textContent = data.license_key || 'N/A';
            }

            if (statusEl) {
                if (data.valid) {
                    statusEl.innerHTML = '<span style="color:var(--success);">Active</span>';
                } else {
                    statusEl.innerHTML = '<span style="color:var(--error);">Invalid</span>';
                }
            }

            if (idEl) idEl.textContent = data.license_id != null ? '#' + data.license_id : 'N/A';

            const tierEl = document.getElementById('licenseInfoTier');
            if (tierEl) {
                const tier = (data.tier || 'pro').toUpperCase();
                const tierColor = data.tier === 'basic' ? '#60a5fa' : '#c084fc';
                tierEl.innerHTML = '<span style="color:' + tierColor + ';">' + tier + '</span>';
            }

            if (expiryEl) {
                if (data.days_remaining === -1) {
                    expiryEl.textContent = 'Lifetime';
                } else {
                    expiryEl.textContent = data.expiry_date || 'N/A';
                }
            }

            if (daysEl) {
                if (data.days_remaining === -1) {
                    daysEl.innerHTML = '<span style="color:var(--success);">Unlimited</span>';
                } else if (data.days_remaining != null) {
                    const color = data.days_remaining <= 7 ? 'var(--error)' : 'var(--success)';
                    daysEl.innerHTML = '<span style="color:' + color + ';">' + data.days_remaining + ' days</span>';
                } else {
                    daysEl.textContent = 'N/A';
                }
            }

            if (machineEl) machineEl.textContent = data.machine_id || 'N/A';
        } catch (err) {
            if (loading) loading.textContent = 'Failed to load license info';
        }
    }

    // ── Modals ────────────────────────────────────────────────────────────

    function setupModals() {
        // Close buttons
        document.querySelectorAll('.tools-modal-close').forEach(btn => {
            btn.addEventListener('click', () => {
                const modal = btn.closest('.tools-modal');
                if (modal) modal.classList.remove('open');
            });
        });

        // Overlay click to close
        document.querySelectorAll('.tools-modal-overlay').forEach(overlay => {
            overlay.addEventListener('click', () => {
                const modal = overlay.closest('.tools-modal');
                if (modal) modal.classList.remove('open');
            });
        });

        // ESC key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                document.querySelectorAll('.tools-modal.open').forEach(m => m.classList.remove('open'));
                document.getElementById('cleanupConfirmOverlay')?.classList.remove('open');
            }
        });

        // Copy button in text viewer
        const copyBtn = document.getElementById('textViewerCopyBtn');
        if (copyBtn) {
            copyBtn.addEventListener('click', () => {
                const content = document.getElementById('textViewerContent');
                if (content) {
                    navigator.clipboard.writeText(content.textContent).then(() => {
                        App.toast('Copied to clipboard', 'success');
                    }).catch(() => {
                        App.toast('Copy failed', 'error');
                    });
                }
            });
        }
    }

    // ── Search Debounce ───────────────────────────────────────────────────

    function setupSearch() {
        const ssInput = document.getElementById('screenshotSearch');
        if (ssInput) {
            ssInput.addEventListener('input', () => {
                clearTimeout(screenshotDebounce);
                screenshotDebounce = setTimeout(() => {
                    screenshotPage = 1;
                    loadScreenshots();
                }, 300);
            });
        }

        const afInput = document.getElementById('authFileSearch');
        if (afInput) {
            afInput.addEventListener('input', () => {
                clearTimeout(authFileDebounce);
                authFileDebounce = setTimeout(() => {
                    authFilePage = 1;
                    loadAuthFiles();
                }, 300);
            });
        }
    }

    // ── Public API ────────────────────────────────────────────────────────

    // ── Tools TOTP ──────────────────────────────────────────────────────────

    let toolsTotpInterval = null;
    let toolsTotpSecret = '';

    function setupToolsTotp() {
        const secretInput = document.getElementById('toolsTotpSecretInput');
        const codeDisplay = document.getElementById('toolsTotpCode');
        const copyBtn = document.getElementById('toolsTotpCopyBtn');
        if (!secretInput) return;

        copyBtn.addEventListener('click', () => {
            const code = codeDisplay.innerText;
            if (code && code !== '------') {
                navigator.clipboard.writeText(code);
                copyBtn.innerHTML = '<i class="fas fa-check"></i> Copied';
                setTimeout(() => { copyBtn.innerHTML = '<i class="fas fa-copy"></i> Copy'; }, 1500);
            }
        });

        function onSecretChange() {
            const val = secretInput.value.trim();
            if (val.length >= 16) {
                toolsTotpSecret = val;
                startToolsTotp();
            } else if (val.length === 0) {
                toolsTotpSecret = '';
                stopToolsTotp();
            }
        }

        secretInput.addEventListener('input', onSecretChange);
        secretInput.addEventListener('paste', () => setTimeout(onSecretChange, 50));
    }

    async function updateToolsTotpCode() {
        const codeDisplay = document.getElementById('toolsTotpCode');
        const timerBar = document.getElementById('toolsTotpTimerBar');
        if (!toolsTotpSecret) {
            if (codeDisplay) codeDisplay.innerText = '------';
            if (timerBar) timerBar.style.width = '0%';
            return;
        }
        try {
            // Use the generateTOTP from App if available, or inline
            if (App._generateTOTP) {
                const code = await App._generateTOTP(toolsTotpSecret);
                codeDisplay.innerText = code || 'INVALID';
            } else {
                codeDisplay.innerText = '------';
            }
        } catch {
            codeDisplay.innerText = 'ERROR';
        }
        const remaining = 30 - (Math.floor(Date.now() / 1000) % 30);
        if (timerBar) timerBar.style.width = ((remaining / 30) * 100) + '%';
    }

    function startToolsTotp() {
        if (toolsTotpInterval) clearInterval(toolsTotpInterval);
        updateToolsTotpCode();
        toolsTotpInterval = setInterval(updateToolsTotpCode, 1000);
    }

    function stopToolsTotp() {
        if (toolsTotpInterval) clearInterval(toolsTotpInterval);
        toolsTotpInterval = null;
        const codeDisplay = document.getElementById('toolsTotpCode');
        const timerBar = document.getElementById('toolsTotpTimerBar');
        if (codeDisplay) codeDisplay.innerText = '------';
        if (timerBar) timerBar.style.width = '0%';
    }

    App.setupToolsPage = function () {
        setupTabs();
        setupModals();
        setupCleanupButtons();
        setupLicenseInfo();
        setupToolsTotp();
        setupSearch();
    };

    App.loadToolsPage = function () {
        // Load data for whichever tab is currently active
        const activeTab = document.querySelector('.tools-tab.active');
        const tab = activeTab ? activeTab.getAttribute('data-tools-tab') : 'screenshots';

        if (tab === 'screenshots') loadScreenshots();
        else if (tab === 'authfiles') loadAuthFiles();
        else if (tab === 'cleaner') loadStorageStats();
    };

})(window.App || (window.App = {}));
