/**
 * profiles.js — NST-Style Profile Manager (Complete Rebuild)
 *
 * Features:
 *   - NST-style table with columns (Profile, Proxy, Status, Group, Updated, Actions)
 *   - Filter bar (All, Logged In, Not Logged In, Failed, Running)
 *   - 4-tab Create/Edit modal (Overview, Proxy, Hardware, Advanced, Credentials)
 *   - Summary panel (right side)
 *   - Context menu (right-click)
 *   - Batch operations (Login, Ops, Appeal, Health)
 *   - Pagination with page size selector
 */
(function (App) {
    'use strict';

    let _searchDebounce = null;
    let _statusPoll = null;
    let _currentFilter = 'all';
    let _currentGroup = '';   // '' = all groups
    let _editingId = null;
    let _allProfiles = [];  // cached for filter counts
    let _contextProfileId = null;
    let _selectedIds = new Set();
    let _pmGroupsState = ['default'];  // groups for profile create/edit modal

    function _renderPmGroupTags() {
        const container = document.getElementById('pmGroupTags');
        if (!container) return;
        container.innerHTML = _pmGroupsState.map((g, i) => `
            <span class="pm-group-pill" style="display:inline-flex;align-items:center;gap:4px;cursor:default;">
                ${_esc(g)}
                <i class="fas fa-times" data-idx="${i}" style="font-size:9px;cursor:pointer;opacity:0.7;" title="Remove"></i>
            </span>`).join('');
        container.querySelectorAll('.fa-times').forEach(icon => icon.addEventListener('click', (e) => {
            const idx = parseInt(e.target.dataset.idx);
            _pmGroupsState.splice(idx, 1);
            if (!_pmGroupsState.length) _pmGroupsState = ['default'];
            _renderPmGroupTags();
        }));
    }

    // OS labels for display
    const _OS_LABELS = {
        random: 'Random',
        windows: 'Windows',
        macos: 'macOS',
        linux: 'Linux',
    };

    // Engine labels (NST only — Local hidden for now)
    const _ENGINE_LABELS = {
        nst:  { name: 'NST Browser',  badge: 'NST',   color: 'var(--primary)', tagClass: 'pm-engine-tag-nst' },
    };
    function _engineInfo(p) { return _ENGINE_LABELS.nst; }

    // ── Helpers ──────────────────────────────────────────────────────────

    function _esc(s) { return App.escapeHtml ? App.escapeHtml(String(s || '')) : String(s || ''); }

    function _timeAgo(iso) {
        if (!iso) return 'Never';
        const diff = (Date.now() - new Date(iso).getTime()) / 1000;
        if (diff < 60) return 'Just now';
        if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
        if (diff < 86400) return Math.floor(diff / 3600) + 'h ago';
        return Math.floor(diff / 86400) + 'd ago';
    }

    async function _api(url, opts) {
        const res = await App.apiFetch(url, opts);
        const ct = res.headers.get('content-type') || '';
        if (!ct.includes('application/json')) throw new Error('Non-JSON response. Restart backend.');
        return res.json();
    }

    function _$(id) { return document.getElementById(id); }
    function _btn(id, fn) { const el = _$(id); if (el) el.addEventListener('click', fn); }
    function _val(id) { return (_$(id) || {}).value || ''; }
    function _setVal(id, v) { const el = _$(id); if (el) el.value = v; }
    function _radio(name) { const el = document.querySelector(`input[name="${name}"]:checked`); return el ? el.value : ''; }
    function _setRadio(name, value) {
        const el = document.querySelector(`input[name="${name}"][value="${value}"]`);
        if (el) {
            el.checked = true;
            // Sync active class on pill/tab containers
            const pill = el.closest('.pm-os-pill, .pm-engine-tab');
            if (pill) {
                const container = pill.parentElement;
                container.querySelectorAll('.pm-os-pill, .pm-engine-tab').forEach(s => s.classList.remove('active'));
                pill.classList.add('active');
            }
        }
    }
    function _checked(id) { const el = _$(id); return el ? el.checked : false; }
    function _setChecked(id, v) { const el = _$(id); if (el) el.checked = !!v; }

    // Refocus webview after native confirm() steals focus (Electron bug)
    function _refocusAfterDialog() {
        window.focus();
        document.body.focus();
        setTimeout(() => {
            window.focus();
            document.body.dispatchEvent(new MouseEvent('mousedown', { bubbles: true }));
            document.body.dispatchEvent(new MouseEvent('mouseup', { bubbles: true }));
        }, 50);
        setTimeout(() => { window.focus(); }, 200);
    }

    // Non-blocking confirm dialog (avoids Electron focus/input lock bug)
    function _asyncConfirm(message) {
        return new Promise(resolve => {
            // Remove existing overlay
            const old = document.getElementById('pmConfirmOverlay');
            if (old) old.remove();

            const overlay = document.createElement('div');
            overlay.id = 'pmConfirmOverlay';
            overlay.style.cssText = 'position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.6);z-index:99999;display:flex;align-items:center;justify-content:center;';
            overlay.innerHTML = `
                <div style="background:var(--bg-card,#1e1e2e);border:1px solid var(--border,#333);border-radius:12px;padding:24px 28px;max-width:400px;text-align:center;">
                    <p style="color:var(--text,#fff);margin:0 0 20px;font-size:14px;">${message}</p>
                    <div style="display:flex;gap:12px;justify-content:center;">
                        <button id="pmConfirmYes" class="btn btn-danger" style="padding:8px 24px;">Delete</button>
                        <button id="pmConfirmNo" class="btn btn-outline" style="padding:8px 24px;">Cancel</button>
                    </div>
                </div>`;
            document.body.appendChild(overlay);

            overlay.querySelector('#pmConfirmYes').addEventListener('click', () => { overlay.remove(); resolve(true); });
            overlay.querySelector('#pmConfirmNo').addEventListener('click', () => { overlay.remove(); resolve(false); });
            overlay.addEventListener('click', (e) => { if (e.target === overlay) { overlay.remove(); resolve(false); } });
        });
    }

    // ══════════════════════════════════════════════════════════════════════
    // LOAD PROFILES (paginated + search + filter)
    // ══════════════════════════════════════════════════════════════════════

    let _loadRetryTimer = null;

    async function loadProfiles() {
        const search = _val('profileSearch');
        const searchBy = _val('profileSearchBy') || 'name';
        const listEl = _$('profileList');
        const countEl = _$('profileCount');
        if (!listEl) return;

        try {
            // Fetch ALL profiles in one call (no pagination)
            const filterParam = _currentFilter !== 'all' ? `&filter=${_currentFilter}` : '';
            const groupParam = _currentGroup ? `&group=${encodeURIComponent(_currentGroup)}` : '';
            const searchByParam = searchBy !== 'name' ? `&search_by=${searchBy}` : '';
            const [allData, data] = await Promise.all([
                _api(`/api/profiles?search=${encodeURIComponent(search)}&page=1&per_page=9999${searchByParam}`),
                _api(`/api/profiles?search=${encodeURIComponent(search)}&page=1&per_page=9999${filterParam}${groupParam}${searchByParam}`),
            ]);

            // Update counts from unfiltered data
            if (allData.success) {
                _updateFilterCounts(allData.profiles || []);
                _refreshGroupsFromProfiles(allData.profiles || []);
            }

            if (!data.success) { listEl.innerHTML = '<div class="tools-empty">Failed to load profiles</div>'; return; }

            let profiles = data.profiles || [];
            const total = profiles.length;
            _allProfiles = profiles;

            if (countEl) countEl.textContent = `${total} profile${total !== 1 ? 's' : ''}`;

            if (profiles.length === 0) {
                // If backend returned total=0 but allData had profiles, backend may still be starting — retry once
                const allTotal = (allData && allData.total) || 0;
                if (allTotal === 0 && !search && _currentFilter === 'all' && !_currentGroup) {
                    if (_loadRetryTimer) clearTimeout(_loadRetryTimer);
                    _loadRetryTimer = setTimeout(() => { _loadRetryTimer = null; loadProfiles(); }, 1500);
                }
                listEl.innerHTML = '<div class="tools-empty" style="padding:40px;"><i class="fas fa-user-circle"></i> No profiles match the current filter.</div>';
                return;
            }
            if (_loadRetryTimer) { clearTimeout(_loadRetryTimer); _loadRetryTimer = null; }

            listEl.innerHTML = profiles.map(p => {
                const statusCls = p.status === 'logged_in' ? 'pm-status-ok' :
                                  p.status === 'login_failed' ? 'pm-status-fail' : 'pm-status-none';
                const statusLbl = p.status === 'logged_in' ? 'Logged In' :
                                  p.status === 'login_failed' ? 'Failed' : 'Not Logged In';
                const isOpen = p.browser_open === 'running';
                const isStarting = p.browser_open === 'starting';
                const ov = p.overview || {};
                const fp = p.fingerprint || {};
                const osBase = (ov.os || fp.os_type || 'win').substring(0, 3).toUpperCase();
                const osVerRaw = ov.os_version || '';
                const osVerNum = osVerRaw.replace(/^Windows\s*/i, '').replace(/\.\d+\.\d+$/, '').trim();
                const os = osBase + (osVerNum ? ' ' + osVerNum : '');
                const engInfo = _engineInfo(p);
                const proxy = p.proxy || {};
                const proxyStr = proxy.host ? `${proxy.host}:${proxy.port || ''}` :
                                 proxy.server ? proxy.server.replace(/^https?:\/\//, '').split('@').pop() : '';
                const groups = (p.groups && p.groups.length) ? p.groups : [(p.group || 'default')];
                const groupPills = groups.map(g => `<span class="pm-group-pill" data-group="${_esc(g)}">${_esc(g)}</span>`).join('');
                const checked = _selectedIds.has(p.id) ? 'checked' : '';

                return `<div class="pm-row ${isOpen ? 'pm-browser-open' : ''} ${isStarting ? 'pm-browser-starting' : ''} ${_selectedIds.has(p.id) ? 'pm-selected' : ''}" data-profile-id="${p.id}">
                    <div class="pm-col-check"><input type="checkbox" class="pm-row-check" data-id="${p.id}" ${checked}></div>
                    <div class="pm-col-profile">
                        <div class="pm-name"><span class="pm-os-badge" style="margin-right:6px;">${os}</span><span class="pm-engine-tag ${engInfo.tagClass}">${engInfo.badge}</span>${_esc(p.name || 'Unnamed')}</div>
                        <div class="pm-email">${_esc(p.email || 'No email')}</div>
                    </div>
                    <div class="pm-col-proxy"><span class="pm-proxy-info">${_esc(proxyStr || 'No proxy')}</span></div>
                    <div class="pm-col-status">
                        <span class="pm-status ${statusCls}">${statusLbl}</span>
                        ${isOpen ? '<span class="pm-status pm-status-running" style="margin-left:4px;"><i class="fas fa-circle"></i> Open</span>'
                            : isStarting ? '<span class="pm-status pm-status-starting" style="margin-left:4px;"><i class="fas fa-spinner fa-spin"></i> Launching</span>' : ''}
                    </div>
                    <div class="pm-col-group">${groupPills}</div>
                    <div class="pm-col-updated">
                        ${(() => {
                            const appealCls = !p.last_appeal_at ? 'pm-act-none' : (p.last_appeal_ok ? 'pm-act-appeal-ok' : 'pm-act-appeal-fail');
                            const appealTip = p.last_appeal_at
                                ? `Appeal: ${_timeAgo(p.last_appeal_at)}${p.last_appeal_summary ? ' — ' + p.last_appeal_summary : ''}`
                                : 'Appeal: Never done';
                            const appealHistory = (p.appeal_history || []).slice(-5).reverse()
                                .map(h => `${h.date ? new Date(h.date).toLocaleDateString() : '?'}: ${h.ok ? '✓' : '✗'} ${h.summary || ''}`)
                                .join('&#10;');
                            const appealTipFull = appealHistory ? `${appealTip}&#10;&#10;History:&#10;${appealHistory}` : appealTip;

                            const healthCls = !p.last_health_at ? 'pm-act-none' : (p.last_health_ok ? 'pm-act-health-ok' : 'pm-act-health-fail');
                            const healthTip = p.last_health_at
                                ? `Health: ${_timeAgo(p.last_health_at)} — ${p.last_health_done||0}/${p.last_health_total||0} done`
                                : 'Health: Never done';
                            const healthHistory = (p.health_history || []).slice(-5).reverse()
                                .map(h => `${h.date ? new Date(h.date).toLocaleDateString() : '?'}: ${h.done||0}/${h.total||0} done`)
                                .join('&#10;');
                            const healthTipFull = healthHistory ? `${healthTip}&#10;&#10;History:&#10;${healthHistory}` : healthTip;

                            return `
                                <span class="pm-act-tag ${appealCls}" title="${appealTipFull}">
                                    <i class="fas fa-gavel"></i>
                                    ${p.last_appeal_at ? _timeAgo(p.last_appeal_at) : 'Never'}
                                </span>
                                <span class="pm-act-tag ${healthCls}" title="${healthTipFull}">
                                    <i class="fas fa-heartbeat"></i>
                                    ${p.last_health_at ? _timeAgo(p.last_health_at) : 'Never'}
                                </span>`;
                        })()}
                    </div>
                    <div class="pm-col-actions">
                        ${isOpen
                            ? `<button class="btn btn-danger btn-sm pm-close-btn" data-id="${p.id}" title="Close"><i class="fas fa-stop"></i></button>`
                            : isStarting
                            ? `<button class="btn btn-sm pm-launching-btn" disabled title="Launching..."><i class="fas fa-spinner fa-spin"></i></button>`
                            : `<button class="btn btn-primary btn-sm pm-launch-btn" data-id="${p.id}" title="Launch"><i class="fas fa-play"></i></button>`
                        }
                        <button class="btn btn-outline btn-sm pm-relogin-btn" data-id="${p.id}" title="Re-Login" style="color:#22c55e;border-color:rgba(34,197,94,0.4);"><i class="fas fa-sign-in-alt"></i></button>
                        <button class="btn btn-outline btn-sm pm-edit-btn" data-id="${p.id}" title="Edit"><i class="fas fa-pen"></i></button>
                        <button class="btn btn-danger-outline btn-sm pm-delete-btn" data-id="${p.id}" title="Delete"><i class="fas fa-trash"></i></button>
                        <button class="btn btn-outline btn-sm pm-ctx-btn" data-id="${p.id}" title="More"><i class="fas fa-ellipsis-v"></i></button>
                    </div>
                </div>`;
            }).join('');

            // Attach events
            _attachRowEvents(listEl);

            // Auto-start polling if any browsers are open or starting
            const hasActive = profiles.some(p => p.browser_open === 'running' || p.browser_open === 'starting');
            if (hasActive && !_statusPoll) _startStatusPolling();

        } catch (e) {
            listEl.innerHTML = `<div class="tools-empty">Error: ${_esc(e.message)}</div>`;
        }
    }

    function _attachRowEvents(listEl) {
        listEl.querySelectorAll('.pm-launch-btn').forEach(b => b.addEventListener('click', (e) => { e.stopPropagation(); launchProfile(b.dataset.id); }));
        listEl.querySelectorAll('.pm-close-btn').forEach(b => b.addEventListener('click', (e) => { e.stopPropagation(); closeProfile(b.dataset.id); }));
        listEl.querySelectorAll('.pm-relogin-btn').forEach(b => b.addEventListener('click', (e) => { e.stopPropagation(); reloginProfile(b.dataset.id); }));
        listEl.querySelectorAll('.pm-edit-btn').forEach(b => b.addEventListener('click', (e) => { e.stopPropagation(); openEditModal(b.dataset.id); }));
        listEl.querySelectorAll('.pm-delete-btn').forEach(b => b.addEventListener('click', (e) => { e.stopPropagation(); deleteProfile(b.dataset.id); }));
        listEl.querySelectorAll('.pm-ctx-btn').forEach(b => b.addEventListener('click', (e) => {
            e.stopPropagation();
            _contextProfileId = b.dataset.id;
            _showContextMenu(e.clientX, e.clientY);
        }));
        listEl.querySelectorAll('.pm-row-check').forEach(cb => cb.addEventListener('change', (e) => {
            const id = cb.dataset.id;
            if (cb.checked) _selectedIds.add(id); else _selectedIds.delete(id);
            cb.closest('.pm-row').classList.toggle('pm-selected', cb.checked);
            _updateBulkBar();
        }));
        // Click on group pill → filter by that group
        listEl.querySelectorAll('.pm-group-pill').forEach(pill => pill.addEventListener('click', (e) => {
            e.stopPropagation();
            const g = pill.dataset.group;
            _currentGroup = g;
            const sel = document.getElementById('pmGroupFilter');
            if (sel) sel.value = g;
            loadProfiles();
        }));
        // Right-click context menu on rows
        listEl.querySelectorAll('.pm-row').forEach(row => {
            row.addEventListener('contextmenu', (e) => {
                e.preventDefault();
                _contextProfileId = row.dataset.profileId;
                _showContextMenu(e.clientX, e.clientY);
            });
        });
    }

    // ── Bulk selection bar ────────────────────────────────────────────────────

    function _updateBulkBar() {
        const bar = _$('pmBulkBar');
        const countEl = _$('pmBulkCount');
        if (!bar) return;
        const n = _selectedIds.size;
        const totalVisible = document.querySelectorAll('.pm-row-check').length;
        if (n > 0) {
            bar.style.display = 'flex';
            const allSelected = n === totalVisible && totalVisible > 0;
            if (countEl) countEl.textContent = allSelected
                ? `All ${n} profiles selected`
                : `${n} of ${totalVisible} profiles selected`;
        } else {
            bar.style.display = 'none';
        }
    }

    function _bulkGroupInput() {
        return (_$('pmBulkGroupInput') ? _$('pmBulkGroupInput').value : '').trim();
    }
    function _bulkNoteInput() {
        return (_$('pmBulkNoteInput') ? _$('pmBulkNoteInput').value : '').trim();
    }

    async function _bulkAddToGroup() {
        const group = _bulkGroupInput();
        if (!group) { App.toast('Enter a group name', 'warn'); return; }
        if (!_selectedIds.size) { App.toast('No profiles selected', 'warn'); return; }
        const note = _bulkNoteInput();
        try {
            const data = await _api('/api/profiles/bulk-assign-group', {
                method: 'POST',
                body: JSON.stringify({ ids: [..._selectedIds], group, mode: 'add', note })
            });
            if (data.success) {
                let msg = `${data.updated} profile${data.updated !== 1 ? 's' : ''} added to "${group}"`;
                if (note && data.notes_updated) msg += ` · note saved`;
                App.toast(msg, 'success');
                loadProfiles(); _loadGroups();
            } else App.toast(data.message || 'Failed', 'error');
        } catch(e) { App.toast('Error: ' + e.message, 'error'); }
    }

    async function _bulkMoveToGroup() {
        const group = _bulkGroupInput();
        if (!group) { App.toast('Enter a group name', 'warn'); return; }
        if (!_selectedIds.size) { App.toast('No profiles selected', 'warn'); return; }
        const note = _bulkNoteInput();
        try {
            const data = await _api('/api/profiles/bulk-assign-group', {
                method: 'POST',
                body: JSON.stringify({ ids: [..._selectedIds], group, mode: 'set', note })
            });
            if (data.success) {
                let msg = `${data.updated} profile${data.updated !== 1 ? 's' : ''} moved to "${group}"`;
                if (note && data.notes_updated) msg += ` · note saved`;
                App.toast(msg, 'success');
                loadProfiles(); _loadGroups();
            } else App.toast(data.message || 'Failed', 'error');
        } catch(e) { App.toast('Error: ' + e.message, 'error'); }
    }

    async function _bulkRemoveFromGroup() {
        const group = _bulkGroupInput();
        if (!group) { App.toast('Enter a group name to remove from', 'warn'); return; }
        if (!_selectedIds.size) { App.toast('No profiles selected', 'warn'); return; }
        const note = _bulkNoteInput();
        try {
            const data = await _api('/api/profiles/bulk-remove-group', {
                method: 'POST',
                body: JSON.stringify({ ids: [..._selectedIds], group, note })
            });
            if (data.success) {
                let msg = `${data.updated} profile${data.updated !== 1 ? 's' : ''} removed from "${group}"`;
                if (note && data.notes_updated) msg += ` · note saved`;
                App.toast(msg, 'success');
                loadProfiles(); _loadGroups();
            } else App.toast(data.message || 'Failed', 'error');
        } catch(e) { App.toast('Error: ' + e.message, 'error'); }
    }

    async function _bulkUpdateProxy() {
        const user = (_$('pmBulkProxyUser') ? _$('pmBulkProxyUser').value : '').trim();
        const pass = (_$('pmBulkProxyPass') ? _$('pmBulkProxyPass').value : '').trim();
        if (!user && !pass) { App.toast('Enter proxy user or password', 'warn'); return; }
        if (!_selectedIds.size) { App.toast('No profiles selected', 'warn'); return; }
        try {
            const data = await _api('/api/profiles/bulk-update-proxy', {
                method: 'POST',
                body: JSON.stringify({ ids: [..._selectedIds], proxy_user: user, proxy_pass: pass })
            });
            if (data.success) {
                App.toast(`Proxy updated for ${data.updated} profile${data.updated !== 1 ? 's' : ''}`, 'success');
                if (_$('pmBulkProxyUser')) _$('pmBulkProxyUser').value = '';
                if (_$('pmBulkProxyPass')) _$('pmBulkProxyPass').value = '';
                loadProfiles();
            } else App.toast(data.message || 'Failed', 'error');
        } catch(e) { App.toast('Error: ' + e.message, 'error'); }
    }

    async function _bulkSaveNoteOnly() {
        const note = _bulkNoteInput();
        if (!note) { App.toast('Type a note first', 'warn'); return; }
        if (!_selectedIds.size) { App.toast('No profiles selected', 'warn'); return; }
        try {
            const data = await _api('/api/profiles/bulk-update-notes', {
                method: 'POST',
                body: JSON.stringify({ ids: [..._selectedIds], note })
            });
            if (data.success) {
                App.toast(`Note saved to ${data.updated} profile${data.updated !== 1 ? 's' : ''}`, 'success');
                loadProfiles();
            } else App.toast(data.message || 'Failed', 'error');
        } catch(e) { App.toast('Error: ' + e.message, 'error'); }
    }

    // ── Group Manager ────────────────────────────────────────────────────────

    let _groupManagerRenameTarget = '';
    let _groupManagerDeleteTarget = '';

    async function _openGroupManager() {
        _$('groupManagerOverlay').style.display = 'flex';
        await _renderGroupManager();
    }
    function _closeGroupManager() { _$('groupManagerOverlay').style.display = 'none'; }

    async function _renderGroupManager() {
        const listEl = _$('groupManagerList');
        if (!listEl) return;
        listEl.innerHTML = '<div style="color:#64748b;text-align:center;padding:20px;">Loading...</div>';
        try {
            const data = await _api('/api/profiles/groups');
            const groups = data.groups || [];
            const counts = data.counts || {};
            if (!groups.length) {
                listEl.innerHTML = '<div style="color:#64748b;text-align:center;padding:20px;">No groups yet</div>';
                return;
            }
            listEl.innerHTML = groups.map(g => `
                <div class="gm-row" data-group="${_esc(g)}">
                    <div style="display:flex;align-items:center;gap:10px;flex:1;">
                        <span class="pm-group-pill" style="pointer-events:none;">${_esc(g)}</span>
                        <span style="font-size:12px;color:#64748b;">${counts[g] || 0} profiles</span>
                    </div>
                    <div style="display:flex;gap:6px;">
                        <button class="btn btn-sm gm-rename-btn" data-group="${_esc(g)}" style="background:rgba(99,102,241,0.15);color:#a5b4fc;border:1px solid rgba(99,102,241,0.3);padding:3px 10px;"><i class="fas fa-edit"></i> Rename</button>
                        <button class="btn btn-sm gm-move-btn" data-group="${_esc(g)}" style="background:rgba(34,197,94,0.12);color:#4ade80;border:1px solid rgba(34,197,94,0.25);padding:3px 10px;"><i class="fas fa-arrows-alt"></i> Move</button>
                        ${g !== 'default' ? `<button class="btn btn-sm gm-delete-btn" data-group="${_esc(g)}" style="background:rgba(239,68,68,0.12);color:#f87171;border:1px solid rgba(239,68,68,0.25);padding:3px 10px;"><i class="fas fa-trash"></i></button>` : ''}
                    </div>
                </div>
            `).join('');

            listEl.querySelectorAll('.gm-rename-btn').forEach(btn => btn.addEventListener('click', () => _openRenameGroup(btn.dataset.group)));
            listEl.querySelectorAll('.gm-move-btn').forEach(btn => btn.addEventListener('click', () => _openMoveGroup(btn.dataset.group)));
            listEl.querySelectorAll('.gm-delete-btn').forEach(btn => btn.addEventListener('click', () => _openDeleteGroup(btn.dataset.group)));
        } catch(e) {
            listEl.innerHTML = `<div style="color:#f87171;text-align:center;padding:20px;">Error: ${e.message}</div>`;
        }
    }

    function _openRenameGroup(group) {
        _groupManagerRenameTarget = group;
        const el = _$('renameGroupOldName'); if (el) el.textContent = group;
        const inp = _$('renameGroupNewInput'); if (inp) { inp.value = group; inp.focus(); inp.select(); }
        _$('renameGroupOverlay').style.display = 'flex';
    }
    function _closeRenameGroup() { _$('renameGroupOverlay').style.display = 'none'; }

    async function _confirmRenameGroup() {
        const newName = (_$('renameGroupNewInput') ? _$('renameGroupNewInput').value : '').trim();
        if (!newName) { App.toast('Enter new group name', 'warn'); return; }
        if (newName === _groupManagerRenameTarget) { _closeRenameGroup(); return; }
        try {
            const data = await _api('/api/profiles/groups/rename', {
                method: 'POST',
                body: JSON.stringify({ old_name: _groupManagerRenameTarget, new_name: newName })
            });
            if (data.success) {
                App.toast(`Renamed "${_groupManagerRenameTarget}" → "${newName}" (${data.updated} profiles)`, 'success');
                _closeRenameGroup(); _renderGroupManager(); loadProfiles(); _loadGroups();
            } else App.toast(data.message || 'Failed', 'error');
        } catch(e) { App.toast('Error: ' + e.message, 'error'); }
    }

    // Move all profiles from one group to another
    function _openMoveGroup(group) {
        _groupManagerDeleteTarget = group;
        const nameEl = _$('deleteGroupName'); if (nameEl) nameEl.textContent = group;
        const inp = _$('deleteGroupReassignInput'); if (inp) inp.value = 'default';
        // Reuse delete modal but with "Move" intent
        const confirmBtn = _$('deleteGroupConfirmBtn');
        if (confirmBtn) { confirmBtn.textContent = ''; confirmBtn.innerHTML = '<i class="fas fa-arrows-alt"></i> Move Profiles'; confirmBtn.className = 'btn btn-primary'; }
        const info = _$('deleteGroupOverlay').querySelector('[class*="info-circle"]');
        _$('deleteGroupOverlay').style.display = 'flex';
        _$('deleteGroupOverlay').dataset.mode = 'move';
    }

    function _openDeleteGroup(group) {
        _groupManagerDeleteTarget = group;
        const nameEl = _$('deleteGroupName'); if (nameEl) nameEl.textContent = group;
        const inp = _$('deleteGroupReassignInput'); if (inp) inp.value = 'default';
        const confirmBtn = _$('deleteGroupConfirmBtn');
        if (confirmBtn) { confirmBtn.innerHTML = '<i class="fas fa-trash"></i> Delete Group'; confirmBtn.className = 'btn btn-danger'; }
        _$('deleteGroupOverlay').style.display = 'flex';
        _$('deleteGroupOverlay').dataset.mode = 'delete';
    }
    function _closeDeleteGroup() { _$('deleteGroupOverlay').style.display = 'none'; }

    async function _confirmDeleteGroup() {
        const reassignTo = (_$('deleteGroupReassignInput') ? _$('deleteGroupReassignInput').value : 'default').trim() || 'default';
        const mode = _$('deleteGroupOverlay') ? _$('deleteGroupOverlay').dataset.mode : 'delete';
        try {
            const data = await _api(`/api/profiles/groups/${encodeURIComponent(_groupManagerDeleteTarget)}`, {
                method: 'DELETE',
                body: JSON.stringify({ reassign_to: reassignTo })
            });
            if (data.success) {
                App.toast(mode === 'move'
                    ? `Moved ${data.updated} profiles from "${_groupManagerDeleteTarget}" → "${reassignTo}"`
                    : `Deleted group "${_groupManagerDeleteTarget}", ${data.updated} profiles moved to "${reassignTo}"`, 'success');
                _closeDeleteGroup(); _renderGroupManager(); loadProfiles(); _loadGroups();
            } else App.toast(data.message || 'Failed', 'error');
        } catch(e) { App.toast('Error: ' + e.message, 'error'); }
    }

    async function _createGroup() {
        const name = (_$('newGroupNameInput') ? _$('newGroupNameInput').value : '').trim();
        if (!name) { App.toast('Enter group name', 'warn'); return; }
        // Groups exist implicitly when profiles are assigned — just refresh and show success
        App.toast(`Group "${name}" ready — assign profiles to it using the bulk toolbar`, 'success');
        if (_$('newGroupNameInput')) _$('newGroupNameInput').value = '';
        _loadGroups();
    }

    function _updateFilterCounts(profiles) {
        const counts = { all: profiles.length, logged_in: 0, not_logged_in: 0, login_failed: 0, running: 0, nst: 0 };
        profiles.forEach(p => {
            if (p.status === 'logged_in') counts.logged_in++;
            else if (p.status === 'login_failed') counts.login_failed++;
            else counts.not_logged_in++;
            if (p.browser_open === 'running') counts.running++;
            counts.nst++;
        });
        const set = (id, n) => { const el = _$(id); if (el) el.textContent = n; };
        set('pmFilterAll', counts.all);
        set('pmFilterLoggedIn', counts.logged_in);
        set('pmFilterNotLoggedIn', counts.not_logged_in);
        set('pmFilterFailed', counts.login_failed);
        set('pmFilterRunning', counts.running);
        set('pmFilterNst', counts.nst);
    }

    // ══════════════════════════════════════════════════════════════════════
    // CONTEXT MENU
    // ══════════════════════════════════════════════════════════════════════

    function _showContextMenu(x, y) {
        const menu = _$('pmContextMenu');
        if (!menu) return;
        menu.style.display = 'block';
        menu.style.left = Math.min(x, window.innerWidth - 220) + 'px';
        menu.style.top = Math.min(y, window.innerHeight - 350) + 'px';
    }

    function _hideContextMenu() {
        const menu = _$('pmContextMenu');
        if (menu) menu.style.display = 'none';
    }

    function _handleContextAction(action) {
        _hideContextMenu();
        if (!_contextProfileId) return;
        const id = _contextProfileId;
        switch (action) {
            case 'launch': launchProfile(id); break;
            case 'relogin': reloginProfile(id); break;
            case 'edit': openEditModal(id); break;
            case 'close': closeProfile(id); break;
            case 'delete': deleteProfile(id); break;
            case 'clear-cache': clearProfileCache(id); break;
            case 'clear-cookies': clearProfileCookies(id); break;
            case 'export': exportProfile(id); break;
            default: App.toast('Action: ' + action, 'info');
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // CREATE / EDIT MODAL (4 Tabs)
    // ══════════════════════════════════════════════════════════════════════

    function openCreateModal() {
        _editingId = null;
        _$('profileModalTitle').textContent = 'Create Profile';
        _$('profileModalSaveText').textContent = 'Create Profile';
        _resetModal();
        _updateSummary();
        _$('profileModalOverlay').classList.add('active');
        // Fix: native confirm() dialog (from delete) can leave webview blurred.
        // Must refocus window first, then the input with a delay for Electron.
        window.focus();
        setTimeout(() => { window.focus(); const el = _$('pmName'); if (el) el.focus(); }, 150);
    }

    async function openEditModal(id) {
        try {
            const data = await _api(`/api/profiles/${id}`);
            if (!data.success) { App.toast('Failed to load profile', 'error'); return; }

            const p = data.profile;
            _editingId = id;
            _$('profileModalTitle').textContent = 'Edit Profile';
            _$('profileModalSaveText').textContent = 'Save Changes';

            // Overview tab
            _setVal('pmName', p.name || '');
            const profileGroups = (p.groups && p.groups.length) ? p.groups : [(p.group || 'default')];
            _pmGroupsState = [...profileGroups];
            _renderPmGroupTags();
            // Engine — always NST
            _setRadio('pmEngine', 'nst');
            const ov = p.overview || {};
            _setRadio('pmOS', ov.os || 'random');
            _setVal('pmStartupUrls', (ov.startup_urls || []).join(', '));
            const adv = p.advanced || {};
            const cbSaveTabs = _$('pmSaveTabs');
            if (cbSaveTabs) cbSaveTabs.checked = adv.save_tabs !== false; // default ON

            // Proxy tab
            const proxy = p.proxy || {};
            if (proxy.host) {
                _setVal('pmProxyType', proxy.type || 'http');
                _setVal('pmProxyHost', proxy.host || '');
                _setVal('pmProxyPort', proxy.port || '');
                _setVal('pmProxyUser', proxy.username || '');
                _setVal('pmProxyPass', proxy.password || '');
            } else if (proxy.server) {
                _setVal('pmProxyType', proxy.server.includes('socks5') ? 'socks5' : 'http');
                const cleaned = proxy.server.replace(/^(socks5|https?):\/\//, '');
                const parts = cleaned.split(':');
                _setVal('pmProxyHost', parts[0] || '');
                _setVal('pmProxyPort', parts[1] || '');
                _setVal('pmProxyUser', proxy.username || '');
                _setVal('pmProxyPass', proxy.password || '');
            } else {
                _setVal('pmProxyType', 'none');
            }
            _toggleProxyFields();

            // Credentials tab
            _setVal('pmEmail', p.email || '');
            _setVal('pmPassword', p.password || '');
            _setVal('pmTotp', p.totp_secret || '');
            _startPmTotp();
            _setVal('pmNotes', p.notes || '');
            _setVal('pmAddress', p.address || '');
            const codes = p.backup_codes || [];
            for (let i = 1; i <= 10; i++) _setVal('pmBC' + i, codes[i - 1] || '');

            _updateSummary();
            _$('profileModalOverlay').classList.add('active');
            setTimeout(() => { const el = _$('pmName'); if (el) { el.focus(); el.blur(); el.focus(); } }, 50);
        } catch (e) {
            App.toast('Error loading profile: ' + e.message, 'error');
        }
    }

    function _resetModal() {
        _setVal('pmName', '');
        _pmGroupsState = ['default'];
        _renderPmGroupTags();
        _setRadio('pmEngine', 'nst');
        _setRadio('pmOS', 'random');
        _setVal('pmStartupUrls', '');
        const cbST = _$('pmSaveTabs'); if (cbST) cbST.checked = true; // default ON
        _setVal('pmProxyType', 'none');
        _setVal('pmProxyHost', ''); _setVal('pmProxyPort', ''); _setVal('pmProxyUser', ''); _setVal('pmProxyPass', '');
        _setVal('pmProxyPaste', '');
        _toggleProxyFields();
        _setVal('pmEmail', ''); _setVal('pmPassword', ''); _setVal('pmTotp', ''); _setVal('pmNotes', ''); _setVal('pmAddress', '');
        _stopPmTotp();
        for (let i = 1; i <= 10; i++) _setVal('pmBC' + i, '');
        _setVal('pmBCParser', '');
        _switchTab('overview');
    }

    function _updateSummary() {
        const el = _$('pmSummary');
        if (!el) return;

        const os = _radio('pmOS') || 'random';
        const osLabel = _OS_LABELS[os] || os;
        const engine = 'nst';
        const engInfo = _ENGINE_LABELS.nst;
        const proxyType = _val('pmProxyType');
        const proxyHost = _val('pmProxyHost');
        const name = _val('pmName') || 'Auto';
        const email = _val('pmEmail');

        el.innerHTML = `
            <div class="pm-sum-section">
                <div class="pm-sum-title">Profile</div>
                <div class="pm-sum-row"><span class="pm-sum-key">Name</span><span class="pm-sum-val">${_esc(name)}</span></div>
                ${email ? `<div class="pm-sum-row"><span class="pm-sum-key">Email</span><span class="pm-sum-val">${_esc(email)}</span></div>` : ''}
                <div class="pm-sum-row"><span class="pm-sum-key">OS</span><span class="pm-sum-val">${_esc(osLabel)}</span></div>
                <div class="pm-sum-row"><span class="pm-sum-key">Engine</span><span class="pm-sum-val" style="color:${engInfo.color};">${engInfo.name}</span></div>
            </div>
            <div class="pm-sum-section">
                <div class="pm-sum-title">Proxy</div>
                <div class="pm-sum-row"><span class="pm-sum-key">Type</span><span class="pm-sum-val">${_esc(proxyType)}</span></div>
                ${proxyHost ? `<div class="pm-sum-row"><span class="pm-sum-key">Host</span><span class="pm-sum-val">${_esc(proxyHost)}</span></div>` : ''}
            </div>
            <div class="pm-sum-section">
                <div class="pm-sum-title">Fingerprint</div>
                <div class="pm-sum-row"><span class="pm-sum-key" style="color:${engInfo.color};">Auto</span><span class="pm-sum-val">Managed by ${engInfo.name}</span></div>
            </div>
        `;

        // Update engine info box
        const infoName = _$('pmEngineInfoName');
        if (infoName) infoName.textContent = engInfo.name;
    }

    function _switchTab(tabName) {
        document.querySelectorAll('.pm-tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tabName));
        document.querySelectorAll('.pm-tab-content').forEach(c => c.classList.toggle('active', c.dataset.tabContent === tabName));
    }

    function _toggleProxyFields() {
        const type = _val('pmProxyType');
        const fields = _$('pmProxyFields');
        if (fields) fields.style.display = type === 'none' ? 'none' : 'block';
    }

    // ── Inline TOTP widget (credentials tab) ──────────────────────────────
    let _pmTotpInterval = null;

    async function _updatePmTotp() {
        const codeEl = _$('pmTotpCode');
        const barEl  = _$('pmTotpTimerBar');
        if (!codeEl) return;
        const secret = _val('pmTotp').trim();
        if (!secret || secret.length < 16) {
            codeEl.innerText = '------';
            if (barEl) barEl.style.width = '0%';
            return;
        }
        try {
            const code = await App._generateTOTP(secret);
            codeEl.innerText = code || 'INVALID';
        } catch { codeEl.innerText = 'ERROR'; }
        const remaining = 30 - (Math.floor(Date.now() / 1000) % 30);
        if (barEl) barEl.style.width = ((remaining / 30) * 100) + '%';
    }

    function _startPmTotp() {
        _stopPmTotp();
        const secret = _val('pmTotp').trim();
        const widget = _$('pmTotpWidget');
        if (!widget) return;
        if (secret && secret.length >= 16) {
            widget.style.display = 'block';
            _updatePmTotp();
            _pmTotpInterval = setInterval(_updatePmTotp, 1000);
        } else {
            widget.style.display = 'none';
        }
    }

    function _stopPmTotp() {
        if (_pmTotpInterval) { clearInterval(_pmTotpInterval); _pmTotpInterval = null; }
        const widget = _$('pmTotpWidget');
        if (widget) widget.style.display = 'none';
        const codeEl = _$('pmTotpCode');
        if (codeEl) codeEl.innerText = '------';
        const barEl = _$('pmTotpTimerBar');
        if (barEl) barEl.style.width = '0%';
    }

    // ══════════════════════════════════════════════════════════════════════
    // SAVE PROFILE
    // ══════════════════════════════════════════════════════════════════════

    async function saveProfile() {
        const name = _val('pmName').trim() || `Profile ${Date.now().toString(36)}`;
        const email = _val('pmEmail').trim();

        // Build proxy
        const proxyType = _val('pmProxyType');
        let proxy = null;
        if (proxyType !== 'none') {
            const host = _val('pmProxyHost').trim();
            const port = _val('pmProxyPort').trim();
            if (host) {
                proxy = {
                    type: proxyType,
                    host: host,
                    port: parseInt(port) || 0,
                    username: _val('pmProxyUser').trim(),
                    password: _val('pmProxyPass').trim(),
                };
            }
        }

        // Backup codes
        const backup_codes = [];
        for (let i = 1; i <= 10; i++) {
            const v = _val('pmBC' + i).trim();
            if (v) backup_codes.push(v);
        }

        // Send OS + credentials with selected engine
        const os = _radio('pmOS') || 'random';
        const engine = 'nst';
        const body = {
            name,
            email,
            proxy,
            engine,
            notes: _val('pmNotes').trim(),
            address: _val('pmAddress').trim(),
            password: _val('pmPassword').trim(),
            totp_secret: _val('pmTotp').trim(),
            backup_codes,
            fingerprint_prefs: { os_type: os },
            groups: _pmGroupsState.length ? _pmGroupsState : ['default'],
            group: _pmGroupsState[0] || 'default',
            overview: {
                os: os,
                browser_kernel: 'nstbrowser',
                startup_urls: _val('pmStartupUrls').split(',').map(s => s.trim()).filter(Boolean),
            },
            advanced: {
                save_tabs: _$('pmSaveTabs') ? _$('pmSaveTabs').checked : true,
            },
        };

        try {
            let data;
            if (_editingId) {
                data = await _api(`/api/profiles/${_editingId}`, { method: 'PUT', body: JSON.stringify(body) });
            } else {
                data = await _api('/api/profiles', { method: 'POST', body: JSON.stringify(body) });
            }
            if (data.success) {
                App.toast(_editingId ? 'Profile updated' : 'Profile created', 'success');
                closeModal();
                loadProfiles();
                _loadGroups();
            } else {
                App.toast(data.message || 'Save failed', 'error');
            }
        } catch (e) {
            App.toast('Error saving: ' + e.message, 'error');
        }
    }

    function closeModal() {
        _$('profileModalOverlay').classList.remove('active');
        _editingId = null;
        _stopPmTotp();
    }

    // ══════════════════════════════════════════════════════════════════════
    // DELETE / LAUNCH / CLOSE
    // ══════════════════════════════════════════════════════════════════════

    async function deleteProfile(id) {
        const ok = await _asyncConfirm('Delete this profile? This cannot be undone.');
        if (!ok) return;
        try {
            const data = await _api(`/api/profiles/${id}`, { method: 'DELETE' });
            if (data.success) { App.toast('Profile deleted', 'success'); loadProfiles(); }
            else App.toast('Delete failed', 'error');
        } catch (e) { App.toast('Delete error', 'error'); }
    }

    async function deleteSelectedProfiles() {
        if (!_selectedIds.size) { App.toast('No profiles selected', 'warn'); return; }
        const count = _selectedIds.size;
        const ok = await _asyncConfirm(`DELETE ${count} SELECTED PROFILE${count > 1 ? 'S' : ''}? This cannot be undone!`);
        if (!ok) return;
        try {
            const data = await _api('/api/profiles/delete-bulk', {
                method: 'DELETE',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ ids: [..._selectedIds] })
            });
            if (data.success) {
                _selectedIds.clear();
                _updateBulkBar();
                _startOpProgress('delete');
            } else App.toast(data.message || 'Delete failed', 'error');
        } catch (e) { App.toast('Delete error', 'error'); }
    }

    async function reloginProfile(id) {
        const profile = _allProfiles.find(p => p.id === id);
        const email = profile?.email || 'this profile';
        if (!profile?.password) {
            App.toast('No saved password for this profile. Edit profile to add credentials.', 'error');
            return;
        }
        App.toast(`Re-login started for ${email}...`, 'info');
        try {
            const data = await _api(`/api/profiles/${id}/relogin`, { method: 'POST' });
            if (data.success) {
                App.toast(data.message || 'Re-login running...', 'success');
                _startStatusPolling();
            } else {
                App.toast(data.error || 'Re-login failed', 'error');
            }
        } catch (e) { App.toast('Re-login error', 'error'); }
    }

    async function launchProfile(id) {
        try {
            App.toast('Launching browser...', 'info');
            _startStatusPolling();  // start polling immediately for launch state
            const data = await _api(`/api/profiles/${id}/launch`, { method: 'POST' });
            if (data.success) {
                App.toast('Browser launched', 'success');
            }
            else App.toast(data.error || 'Launch failed', 'error');
            await loadProfiles();
        } catch (e) { App.toast('Launch error', 'error'); }
    }

    async function closeProfile(id) {
        try {
            App.toast('Closing browser...', 'info');
            const data = await _api(`/api/profiles/${id}/close`, { method: 'POST' });
            App.toast('Browser closed', 'success');
            await loadProfiles();
            // Polling will auto-stop when no browsers are open
        } catch (e) { App.toast('Close error', 'error'); }
    }

    async function closeAllProfiles() {
        try {
            await _api('/api/profiles/close-all', { method: 'POST' });
            App.toast('All browsers closed', 'success');
            loadProfiles();
        } catch (e) { App.toast('Close all error', 'error'); }
    }

    async function clearProfileCache(id) {
        App.toast('Cache clearing not yet implemented', 'info');
    }

    async function clearProfileCookies(id) {
        App.toast('Cookie clearing not yet implemented', 'info');
    }

    async function exportProfile(id) {
        App.toast('Export not yet implemented', 'info');
    }

    // ══════════════════════════════════════════════════════════════════════
    // BATCH OPERATIONS
    // ══════════════════════════════════════════════════════════════════════

    function openBatchLoginModal() {
        _$('batchLoginModalOverlay').classList.add('active');
        _loadGroups();
        // Clear any previous preview
        _setBatchPreview(null);
    }

    function closeBatchLoginModal() {
        _$('batchLoginModalOverlay').classList.remove('active');
    }

    function _setBatchPreview(info) {
        const el = _$('batchLoginPreview');
        if (!el) return;
        if (!info) { el.style.display = 'none'; return; }
        if (!info.success) {
            el.style.display = 'flex';
            el.innerHTML = `<span style="color:#f87171;"><i class="fas fa-exclamation-circle"></i> ${_esc(info.message || 'Could not read file')}</span>`;
            return;
        }
        el.style.display = 'flex';
        el.innerHTML = `
            <span style="color:#4ade80;"><i class="fas fa-file-excel"></i> <strong>${info.valid}</strong> valid accounts</span>
            ${info.valid !== info.total ? `<span style="color:#64748b;font-size:11px;">(${info.total} total rows, ${info.total - info.valid} skipped)</span>` : ''}
            <span style="color:#64748b;font-size:11px;">${info.columns && info.columns.includes('Proxy') ? '· Proxy column detected' : ''}</span>
        `;
    }

    let _batchPreviewTimer = null;
    async function _previewBatchFile() {
        const filePath = _val('batchLoginFilePath').trim();
        if (!filePath) { _setBatchPreview(null); return; }
        if (_batchPreviewTimer) clearTimeout(_batchPreviewTimer);
        _batchPreviewTimer = setTimeout(async () => {
            try {
                const data = await _api('/api/profiles/batch-login-preview', {
                    method: 'POST', body: JSON.stringify({ file_path: filePath })
                });
                _setBatchPreview(data);
            } catch(e) { _setBatchPreview({ success: false, message: e.message }); }
        }, 400);
    }

    async function startBatchLogin() {
        const filePath = _val('batchLoginFilePath').trim();
        const workers = parseInt(_val('batchLoginWorkers')) || 3;
        const staggerDelay = parseInt(_val('batchLoginStagger')) || 3;
        const engine = 'nst';
        const osRadio = document.querySelector('input[name="batchOs"]:checked');
        const osType = osRadio ? osRadio.value : 'random';
        const group = (_val('batchLoginGroup') || 'default').trim() || 'default';
        if (!filePath) { App.toast('Select an Excel file first', 'error'); return; }
        try {
            const data = await _api('/api/profiles/batch-login', {
                method: 'POST', body: JSON.stringify({ file_path: filePath, workers, engine, os_type: osType, group, stagger_delay: staggerDelay })
            });
            if (data.success) {
                App.toast(`Batch login started: ${data.total} accounts — group: ${group}`, 'success');
                closeBatchLoginModal();
                _startOpProgress('batch-login');
                _startStatusPolling();
                _loadGroups();
            } else App.toast(data.message || 'Batch login failed', 'error');
        } catch (e) { App.toast('Batch login error: ' + e.message, 'error'); }
    }

    // ══════════════════════════════════════════════════════════════════════
    // RUN OPS MODAL
    // ══════════════════════════════════════════════════════════════════════
    let _runOpsProfiles = [];
    const _runOpsChecked = new Set();
    let _runOpsSearch = '';
    let _runOpsPage = 1;
    let _runOpsGroupFilter = '';
    const _RUNOPS_PAGE_SIZE = 15;

    function openRunOpsModal() {
        const modal = document.getElementById('runOpsModal');
        if (!modal) return;
        // Pre-select from bulk selection
        _runOpsChecked.clear();
        _selectedIds.forEach(id => _runOpsChecked.add(id));
        // Load profiles
        _api('/api/profiles?per_page=9999').then(data => {
            _runOpsProfiles = data.profiles || [];
            // If no bulk selection, select all
            if (!_runOpsChecked.size) _runOpsProfiles.forEach(p => _runOpsChecked.add(p.id));
            _runOpsPage = 1;
            _renderRunOpsProfiles();
            _updateRunOpsCount();
            // Populate group filter
            const gf = document.getElementById('runOpsGroupFilter');
            if (gf) {
                const groups = [...new Set(_runOpsProfiles.flatMap(p => (p.groups && p.groups.length) ? p.groups : [p.group || 'default']))].sort();
                gf.innerHTML = '<option value="">All Groups</option>' + groups.map(g => `<option value="${g}">${g}</option>`).join('');
            }
        });
        // Reset ops checkboxes
        modal.querySelectorAll('.runops-op').forEach(cb => { cb.checked = false; });
        _updateRunOpsParams();
        modal.style.display = 'flex';
    }

    function _closeRunOpsModal() {
        const modal = document.getElementById('runOpsModal');
        if (modal) modal.style.display = 'none';
    }

    function _filteredRunOpsProfiles() {
        let list = _runOpsProfiles;
        const q = _runOpsSearch.trim().toLowerCase();
        if (q) list = list.filter(p => (p.email || '').toLowerCase().includes(q) || (p.name || '').toLowerCase().includes(q));
        if (_runOpsGroupFilter) list = list.filter(p => {
            const gs = (p.groups && p.groups.length) ? p.groups : [p.group || 'default'];
            return gs.some(g => g.toLowerCase() === _runOpsGroupFilter.toLowerCase());
        });
        return list;
    }

    function _renderRunOpsProfiles() {
        const container = document.getElementById('runOpsProfileList');
        if (!container) return;
        const filtered = _filteredRunOpsProfiles();
        const totalPages = Math.max(1, Math.ceil(filtered.length / _RUNOPS_PAGE_SIZE));
        _runOpsPage = Math.min(_runOpsPage, totalPages);
        const start = (_runOpsPage - 1) * _RUNOPS_PAGE_SIZE;
        const page = filtered.slice(start, start + _RUNOPS_PAGE_SIZE);

        let html = page.map(p => {
            const checked = _runOpsChecked.has(p.id) ? 'checked' : '';
            const statusCls = p.status === 'logged_in' ? 'color:#22c55e' : p.status === 'login_failed' ? 'color:#ef4444' : 'color:#64748b';
            return `<label class="runops-profile-row">
                <input type="checkbox" data-id="${p.id}" ${checked}>
                <div style="flex:1;min-width:0;">
                    <div style="font-size:12px;color:#e2e8f0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${_esc(p.name || 'Unnamed')}</div>
                    <div style="font-size:10px;color:#64748b;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${_esc(p.email || '')}</div>
                </div>
                <span style="font-size:10px;${statusCls};">${p.status === 'logged_in' ? '✓' : p.status === 'login_failed' ? '✗' : '—'}</span>
            </label>`;
        }).join('');

        // Pagination
        if (totalPages > 1) {
            html += '<div class="modal-pg-bar">';
            if (_runOpsPage > 1) html += `<button class="modal-pg-btn" data-pg="${_runOpsPage - 1}">‹</button>`;
            for (let i = 1; i <= totalPages; i++) {
                html += `<button class="modal-pg-btn ${i === _runOpsPage ? 'active' : ''}" data-pg="${i}">${i}</button>`;
            }
            if (_runOpsPage < totalPages) html += `<button class="modal-pg-btn" data-pg="${_runOpsPage + 1}">›</button>`;
            html += '</div>';
        }
        container.innerHTML = html;

        // Bind checkbox events
        container.querySelectorAll('input[type="checkbox"]').forEach(cb => {
            cb.addEventListener('change', () => {
                if (cb.checked) _runOpsChecked.add(cb.dataset.id);
                else _runOpsChecked.delete(cb.dataset.id);
                _updateRunOpsCount();
            });
        });
        // Bind pagination
        container.querySelectorAll('.modal-pg-btn').forEach(btn => {
            btn.addEventListener('click', () => { _runOpsPage = parseInt(btn.dataset.pg); _renderRunOpsProfiles(); });
        });
    }

    function _updateRunOpsCount() {
        const el = document.getElementById('runOpsSelectedCount');
        if (el) el.textContent = _runOpsChecked.size;
    }

    function _updateRunOpsParams() {
        const ops = new Set();
        document.querySelectorAll('.runops-op:checked').forEach(cb => ops.add(cb.value));

        const show = (id, visible) => { const el = document.getElementById(id); if (el) el.style.display = visible ? 'block' : 'none'; };
        show('runOpsParamPassword', ops.has('1'));
        show('runOpsParamRecoveryPhone', ops.has('2a') || ops.has('6a'));
        show('runOpsParamRecoveryEmail', ops.has('3a'));
        show('runOpsParam2FAPhone', ops.has('6a'));
        show('runOpsParamNames', ops.has('8'));
        show('runOpsNoParams', !ops.has('1') && !ops.has('2a') && !ops.has('3a') && !ops.has('6a') && !ops.has('8'));
    }

    function _setupRunOpsModal() {
        // Close buttons
        _btn('runOpsModalClose', _closeRunOpsModal);
        _btn('runOpsModalCancelBtn', _closeRunOpsModal);

        // Step tabs
        document.querySelectorAll('.runops-step-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                document.querySelectorAll('.runops-step-tab').forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
                document.querySelectorAll('.runops-step-panel').forEach(p => p.style.display = 'none');
                const panel = document.getElementById('runOpsStep' + tab.dataset.step);
                if (panel) panel.style.display = 'block';
            });
        });

        // Op checkboxes → toggle param fields
        document.querySelectorAll('.runops-op').forEach(cb => {
            cb.addEventListener('change', _updateRunOpsParams);
        });

        // Search
        const searchEl = document.getElementById('runOpsSearchInput');
        if (searchEl) searchEl.addEventListener('input', () => {
            _runOpsSearch = searchEl.value; _runOpsPage = 1; _renderRunOpsProfiles();
        });

        // Group filter
        const gfEl = document.getElementById('runOpsGroupFilter');
        if (gfEl) gfEl.addEventListener('change', () => {
            _runOpsGroupFilter = gfEl.value; _runOpsPage = 1; _renderRunOpsProfiles();
        });

        // Select All / None
        _btn('runOpsSelectAll', () => {
            _filteredRunOpsProfiles().forEach(p => _runOpsChecked.add(p.id));
            _renderRunOpsProfiles(); _updateRunOpsCount();
        });
        _btn('runOpsDeselectAll', () => {
            _runOpsChecked.clear(); _renderRunOpsProfiles(); _updateRunOpsCount();
        });

        // Load names from file
        _btn('runOpsLoadNamesBtn', () => {
            document.getElementById('runOpsNameFileInput')?.click();
        });
        const nameFileInput = document.getElementById('runOpsNameFileInput');
        if (nameFileInput) nameFileInput.addEventListener('change', async (e) => {
            const file = e.target.files[0];
            if (!file) return;
            try {
                const text = await file.text();
                const textarea = document.getElementById('runOpsNameList');
                if (textarea) textarea.value = text.trim();
                App.toast(`Loaded ${text.trim().split('\n').length} names from file`, 'success');
            } catch (err) { App.toast('Failed to read file', 'error'); }
            nameFileInput.value = '';
        });

        // Start button
        _btn('runOpsModalStartBtn', _submitRunOps);
    }

    async function _submitRunOps() {
        if (!_runOpsChecked.size) { App.toast('No profiles selected', 'warn'); return; }

        // Collect selected operations
        const ops = [];
        document.querySelectorAll('.runops-op:checked').forEach(cb => ops.push(cb.value));
        if (!ops.length) { App.toast('No operations selected', 'warn'); return; }

        const workers = parseInt(document.getElementById('runOpsWorkers')?.value || '5') || 5;
        const stagger = parseInt(document.getElementById('runOpsStagger')?.value || '3') || 3;

        // Collect params
        const params = {};
        if (ops.includes('1')) params.new_password = _val('runOpsNewPassword') || '';
        if (ops.includes('2a') || ops.includes('6a')) params.recovery_phone = _val('runOpsRecoveryPhone') || '';
        if (ops.includes('3a')) params.recovery_email = _val('runOpsRecoveryEmail') || '';
        if (ops.includes('6a')) params.twofa_phone = _val('runOps2FAPhone') || '';
        if (ops.includes('8')) {
            params.name_list = (_val('runOpsNameList') || '').trim();
            params.name_country = document.getElementById('runOpsNameCountry')?.value || 'US';
        }

        // Validate required params
        if (ops.includes('1') && !params.new_password) { App.toast('New password is required for Change Password', 'warn'); return; }

        try {
            const data = await _api('/api/profiles/run-ops', {
                method: 'POST',
                body: JSON.stringify({
                    profile_ids: [..._runOpsChecked],
                    operations: ops.join(','),
                    params,
                    num_workers: workers,
                    stagger_delay: stagger,
                })
            });
            if (data.success) {
                _closeRunOpsModal();
                App.toast(`Operations started on ${data.total || _runOpsChecked.size} profiles`, 'success');
                _startOpProgress('run-ops');
                _startStatusPolling();
            } else {
                App.toast(data.error || data.message || 'Failed to start operations', 'error');
            }
        } catch (e) { App.toast('Error: ' + e.message, 'error'); }
    }

    // ══════════════════════════════════════════════════════════════════════
    // WRITE REVIEW
    // ══════════════════════════════════════════════════════════════════════

    let _wrPreviewTimer = null;

    function openWriteReviewModal() {
        _$('writeReviewModalOverlay').style.display = 'flex';
        _setWRPreview(null);
    }

    function closeWriteReviewModal() { _$('writeReviewModalOverlay').style.display = 'none'; }

    function _setWRPreview(info) {
        const el = _$('writeReviewPreview');
        if (!el) return;
        if (!info) { el.style.display = 'none'; return; }
        el.style.display = 'flex';
        if (!info.success) {
            el.innerHTML = `<span style="color:#f87171;"><i class="fas fa-exclamation-circle"></i> ${_esc(info.message || 'Cannot read file')}</span>`;
            return;
        }
        el.innerHTML = `
            <span style="color:#4ade80;"><i class="fas fa-file-excel"></i> <strong>${info.valid_rows}</strong> rows with GMB URL</span>
            <span style="color:#a5b4fc;"><i class="fas fa-users"></i> <strong>${info.matched_profiles}</strong> profiles matched</span>
            ${info.has_review_text ? '<span style="color:#64748b;font-size:11px;">· Review Text ✓</span>' : '<span style="color:#f59e0b;font-size:11px;">· No Review Text col</span>'}
            ${info.has_stars ? '<span style="color:#64748b;font-size:11px;">· Stars ✓</span>' : ''}
        `;
    }

    async function _previewWRFile() {
        const filePath = (_$('writeReviewFilePath') ? _$('writeReviewFilePath').value : '').trim();
        if (!filePath) { _setWRPreview(null); return; }
        if (_wrPreviewTimer) clearTimeout(_wrPreviewTimer);
        _wrPreviewTimer = setTimeout(async () => {
            try {
                const data = await _api('/api/profiles/write-review-preview', {
                    method: 'POST', body: JSON.stringify({ excel_file: filePath })
                });
                _setWRPreview(data);
            } catch(e) { _setWRPreview({ success: false, message: e.message }); }
        }, 400);
    }

    async function startWriteReview() {
        const filePath = (_$('writeReviewFilePath') ? _$('writeReviewFilePath').value : '').trim();
        if (!filePath) { App.toast('Select an Excel file first', 'error'); return; }
        const workers = parseInt(_$('writeReviewWorkers') ? _$('writeReviewWorkers').value : '3') || 3;
        closeWriteReviewModal();
        try {
            const data = await _api('/api/profiles/do-write-review', {
                method: 'POST',
                body: JSON.stringify({ excel_file: filePath, num_workers: workers })
            });
            if (data.success) {
                App.toast(`Write Review started: ${data.matched} profiles matched by email`, 'success');
                _startOpProgress('review');
            } else App.toast(data.message || data.error || 'Failed to start', 'error');
        } catch(e) { App.toast('Write Review error: ' + e.message, 'error'); }
    }

    // ── GMB URL → Review URL Converter ──────────────────────────────────────
    let _gmbPreviewTimer = null;

    function openGmbToReviewModal() {
        _$('gmbToReviewModalOverlay').style.display = 'flex';
        _setGmbPreview(null);
        _$('gmbToReviewProgress').style.display = 'none';
        const btn = _$('gmbToReviewStartBtn');
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fas fa-play"></i> Generate Review URLs'; }
    }

    function closeGmbToReviewModal() {
        _$('gmbToReviewModalOverlay').style.display = 'none';
    }

    function _setGmbPreview(info) {
        const el = _$('gmbToReviewPreview');
        if (!el) return;
        if (!info) { el.style.display = 'none'; return; }
        el.style.display = 'flex';
        if (!info.success) {
            el.innerHTML = `<span style="color:#f87171;"><i class="fas fa-exclamation-circle"></i> ${_esc(info.message || 'Cannot read file')}</span>`;
            return;
        }
        el.innerHTML = `
            <span style="color:#4ade80;"><i class="fas fa-file-excel"></i> <strong>${info.total_rows}</strong> total rows</span>
            <span style="color:#a5b4fc;"><i class="fas fa-link"></i> <strong>${info.gmb_url_count}</strong> GMB URLs found</span>
            ${info.columns ? `<span style="color:#64748b;font-size:11px;">Columns: ${_esc(info.columns.join(', '))}</span>` : ''}
        `;
    }

    async function _previewGmbFile() {
        const filePath = _val('gmbToReviewFilePath').trim();
        if (!filePath) { _setGmbPreview(null); return; }
        if (_gmbPreviewTimer) clearTimeout(_gmbPreviewTimer);
        _gmbPreviewTimer = setTimeout(async () => {
            try {
                const data = await _api('/api/gmb-to-review/preview', {
                    method: 'POST', body: JSON.stringify({ file_path: filePath })
                });
                _setGmbPreview(data);
            } catch(e) { _setGmbPreview({ success: false, message: e.message }); }
        }, 400);
    }

    async function startGmbToReview() {
        const filePath = _val('gmbToReviewFilePath').trim();
        if (!filePath) { App.toast('Select an Excel file first', 'error'); return; }

        try {
            const data = await _api('/api/gmb-to-review/process', {
                method: 'POST',
                body: JSON.stringify({ file_path: filePath })
            });
            if (data.success) {
                closeGmbToReviewModal();
                App.toast('GMB Review URL generation started', 'success');
                _startOpProgress('gmb-review');
            } else {
                App.toast(data.message || 'Failed to start', 'error');
            }
        } catch(e) {
            App.toast('GMB Review URL error: ' + e.message, 'error');
        }
    }

    // ── Pagination helper ─────────────────────────────────────────────────────
    const _MODAL_PAGE_SIZE = 15;

    function _modalPagination(page, total) {
        if (total <= 1) return '';
        let s = Math.max(1, page - 2), e = Math.min(total, s + 4);
        if (e - s < 4) s = Math.max(1, e - 4);
        let btns = '';
        btns += `<button class="modal-pg-btn" data-pg="${page - 1}" ${page <= 1 ? 'disabled' : ''}>&#8249;</button>`;
        for (let i = s; i <= e; i++) {
            btns += `<button class="modal-pg-btn${i === page ? ' active' : ''}" data-pg="${i}">${i}</button>`;
        }
        btns += `<button class="modal-pg-btn" data-pg="${page + 1}" ${page >= total ? 'disabled' : ''}>&#8250;</button>`;
        btns += `<span class="modal-pg-info">${page} / ${total}</span>`;
        return `<div class="modal-pg-bar">${btns}</div>`;
    }

    // ── Appeal Modal ─────────────────────────────────────────────────────────

    let _appealModalProfiles = [];
    const _appealChecked = new Set();
    let _appealSearch = '';
    let _appealPage = 1;
    let _appealGroupFilter = '';

    function _filteredAppeal() {
        let list = _appealModalProfiles;
        const q = _appealSearch.trim().toLowerCase();
        if (q) list = list.filter(p =>
            (p.email || '').toLowerCase().includes(q) || (p.name || '').toLowerCase().includes(q)
        );
        if (_appealGroupFilter) list = list.filter(p => {
            const gs = (p.groups && p.groups.length) ? p.groups : [(p.group || 'default')];
            return gs.map(g => g.toLowerCase()).includes(_appealGroupFilter.toLowerCase());
        });
        return list;
    }

    async function openAppealModal() {
        const modal = document.getElementById('appealModal');
        if (!modal) return;

        // Reset mode to Select
        _appealMode = 'select';
        _appealExcelPath = '';
        _setAppealMode('select');
        const excelName = document.getElementById('appealExcelFileName');
        if (excelName) excelName.textContent = 'No file selected';
        const excelInfo = document.getElementById('appealExcelMatchInfo');
        if (excelInfo) { excelInfo.style.display = 'none'; excelInfo.innerHTML = ''; }

        // Reset search/page/group state
        _appealSearch = '';
        _appealPage = 1;
        _appealGroupFilter = '';
        const searchEl = document.getElementById('appealSearchInput');
        if (searchEl) searchEl.value = '';
        const groupEl = document.getElementById('appealGroupFilter');
        if (groupEl) groupEl.value = '';
        _loadGroups();

        // Load profiles
        _appealModalProfiles = [];
        _appealChecked.clear();
        document.getElementById('appealProfileList').innerHTML =
            '<div style="color:#64748b;font-size:13px;text-align:center;padding:30px;">Loading...</div>';
        modal.style.display = 'flex';

        try {
            const data = await _api('/api/profiles?per_page=1000');
            _appealModalProfiles = (data.profiles || data || []);
        } catch (e) {
            _appealModalProfiles = [];
        }

        // Pre-check profiles that were already selected in the main table
        _appealModalProfiles.forEach(p => {
            if (_selectedIds.has(p.id)) _appealChecked.add(p.id);
        });

        _renderAppealList();
        _updateAppealCount();
    }

    function _renderAppealList() {
        const container = document.getElementById('appealProfileList');
        if (!container) return;

        const filtered = _filteredAppeal();
        if (!filtered.length) {
            container.innerHTML = '<div style="color:#64748b;font-size:13px;text-align:center;padding:30px;">No profiles found</div>';
            return;
        }

        const totalPages = Math.max(1, Math.ceil(filtered.length / _MODAL_PAGE_SIZE));
        if (_appealPage > totalPages) _appealPage = totalPages;
        const pageItems = filtered.slice((_appealPage - 1) * _MODAL_PAGE_SIZE, _appealPage * _MODAL_PAGE_SIZE);

        const cards = pageItems.map(p => {
            const checked = _appealChecked.has(p.id) ? 'checked' : '';
            const email = p.email || p.name || p.id;
            const status = p.login_status || p.status || 'unknown';
            const proxy = p.proxy ? `${p.proxy.host || ''}:${p.proxy.port || ''}` : '—';
            const osVerRaw = p.overview?.os_version || '';
            const osVerNum = osVerRaw.replace(/^Windows\s*/i, '').replace(/\.\d+\.\d+$/, '').trim();
            const winTag = osVerNum ? `<span style="font-size:10px;background:rgba(99,102,241,0.2);color:#a5b4fc;padding:1px 5px;border-radius:4px;">WIN ${osVerNum}</span>` : '';
            const engTag = p.engine === 'nst' ? '<span style="font-size:10px;background:rgba(59,130,246,0.2);color:#60a5fa;padding:1px 5px;border-radius:4px;">NST</span>' : '';
            const statusColor = status === 'logged_in' ? '#22c55e' : '#94a3b8';
            const statusBg = status === 'logged_in' ? 'rgba(34,197,94,0.12)' : 'rgba(100,116,139,0.15)';

            let appealTrack;
            if (p.last_appeal_at) {
                const ico = p.last_appeal_ok ? '✓' : '✗';
                const clr = p.last_appeal_ok ? '#34d399' : '#f87171';
                const summ = p.last_appeal_summary ? ` — ${_esc(p.last_appeal_summary)}` : '';
                const hist = (p.appeal_history || []).slice(-5).reverse();
                const histHtml = hist.length > 1 ? hist.map(h => {
                    const d = h.date ? new Date(h.date).toLocaleDateString('en-GB', {day:'2-digit',month:'short'}) : '?';
                    const hIco = h.ok ? '<span style="color:#34d399;">✓</span>' : '<span style="color:#f87171;">✗</span>';
                    const hSumm = h.summary ? ` <span style="color:#64748b;">${_esc(h.summary)}</span>` : '';
                    return `<span style="display:inline-flex;align-items:center;gap:3px;background:rgba(255,255,255,0.04);border-radius:3px;padding:1px 5px;font-size:10px;">${hIco} ${d}${hSumm}</span>`;
                }).join('') : '';
                appealTrack = `<div style="font-size:11px;color:${clr};margin-top:4px;display:flex;align-items:center;gap:4px;flex-wrap:wrap;">
                    <span style="font-weight:600;">${ico} Last: ${_timeAgo(p.last_appeal_at)}${summ}</span>
                </div>${histHtml ? `<div style="display:flex;flex-wrap:wrap;gap:3px;margin-top:4px;">${histHtml}</div>` : ''}`;
            } else {
                appealTrack = `<div style="font-size:11px;color:#475569;margin-top:4px;"><i class="fas fa-clock" style="margin-right:4px;font-size:10px;"></i>Never appealed</div>`;
            }

            return `<label style="display:flex;align-items:flex-start;gap:10px;padding:10px 12px;border-radius:8px;cursor:pointer;border:1px solid rgba(255,255,255,0.07);background:rgba(255,255,255,0.025);transition:background 0.15s;margin-bottom:4px;" class="appeal-row">
                <input type="checkbox" data-id="${p.id}" ${checked} style="width:15px;height:15px;accent-color:#f59e0b;flex-shrink:0;margin-top:3px;">
                <div style="flex:1;min-width:0;">
                    <div style="display:flex;align-items:center;gap:5px;flex-wrap:wrap;margin-bottom:2px;">${winTag}${engTag}<span style="font-size:13px;font-weight:600;color:#e2e8f0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${_esc(email)}</span></div>
                    <div style="font-size:11px;color:#64748b;">${_esc(proxy)}</div>
                    ${appealTrack}
                </div>
                <span style="font-size:10px;padding:2px 8px;border-radius:10px;white-space:nowrap;flex-shrink:0;background:${statusBg};color:${statusColor};margin-top:2px;">${status.replace(/_/g,' ')}</span>
            </label>`;
        }).join('');

        container.innerHTML = cards + _modalPagination(_appealPage, totalPages);

        container.querySelectorAll('input[type="checkbox"]').forEach(cb => {
            cb.addEventListener('change', () => {
                if (cb.checked) _appealChecked.add(cb.dataset.id);
                else _appealChecked.delete(cb.dataset.id);
                _updateAppealCount();
            });
        });
        container.querySelectorAll('.modal-pg-btn:not([disabled])').forEach(btn => {
            btn.addEventListener('click', () => {
                _appealPage = parseInt(btn.dataset.pg);
                _renderAppealList();
                container.scrollTop = 0;
            });
        });
    }

    function _updateAppealCount() {
        const el = document.getElementById('appealSelectedCount');
        if (el) el.textContent = _appealChecked.size;
    }

    function closeAppealModal() {
        const modal = document.getElementById('appealModal');
        if (modal) modal.style.display = 'none';
    }

    // ── Appeal Mode Toggle (Select vs Excel) ────────────────────────────────
    let _appealMode = 'select'; // 'select' | 'excel'
    let _appealExcelPath = '';

    function _setAppealMode(mode) {
        _appealMode = mode;
        const selectBtn = document.getElementById('appealModeSelectBtn');
        const excelBtn = document.getElementById('appealModeExcelBtn');
        const selectControls = document.getElementById('appealSelectControls');
        const excelControls = document.getElementById('appealExcelControls');
        const hint = document.getElementById('appealModeHint');

        if (mode === 'excel') {
            selectBtn.classList.remove('active');
            selectBtn.style.background = 'transparent';
            selectBtn.style.color = '#94a3b8';
            excelBtn.classList.add('active');
            excelBtn.style.background = '';
            excelBtn.style.color = '';
            if (selectControls) selectControls.style.display = 'none';
            if (excelControls) excelControls.style.display = '';
            if (hint) hint.textContent = 'Upload an Excel file with an Email column.';
        } else {
            excelBtn.classList.remove('active');
            excelBtn.style.background = 'transparent';
            excelBtn.style.color = '#94a3b8';
            selectBtn.classList.add('active');
            selectBtn.style.background = '';
            selectBtn.style.color = '';
            if (selectControls) selectControls.style.display = '';
            if (excelControls) excelControls.style.display = 'none';
            if (hint) hint.textContent = 'Select profiles to run Appeal on.';
        }
    }

    async function _appealBrowseExcel() {
        const filePath = await window.electronAPI.selectFile();
        if (!filePath) return;
        _appealExcelPath = filePath;
        const nameEl = document.getElementById('appealExcelFileName');
        if (nameEl) nameEl.textContent = filePath.split(/[\\/]/).pop();

        // Match emails against profiles
        const infoEl = document.getElementById('appealExcelMatchInfo');
        if (infoEl) {
            infoEl.style.display = '';
            infoEl.innerHTML = '<i class="fas fa-spinner fa-spin" style="color:#f59e0b;"></i> <span style="color:#94a3b8;font-size:12px;">Matching emails...</span>';
        }
        try {
            const data = await _api('/api/profiles/appeal-match-excel', {
                method: 'POST',
                body: JSON.stringify({ file_path: filePath })
            });
            if (!data.success) {
                if (infoEl) infoEl.innerHTML = `<span style="color:#f87171;font-size:12px;"><i class="fas fa-exclamation-circle"></i> ${_esc(data.message)}</span>`;
                return;
            }
            // Auto-select matched profiles
            _appealChecked.clear();
            (data.matched || []).forEach(m => _appealChecked.add(m.id));
            _renderAppealList();
            _updateAppealCount();

            // Show match summary
            let html = `<div style="display:flex;gap:14px;flex-wrap:wrap;align-items:center;">
                <span style="font-size:13px;color:#e2e8f0;font-weight:600;"><i class="fas fa-check-circle" style="color:#22c55e;margin-right:4px;"></i>${data.matched_count} matched</span>
                <span style="font-size:12px;color:#94a3b8;">out of ${data.total_emails} emails</span>`;
            if (data.not_found_count > 0) {
                html += `<span style="font-size:12px;color:#f59e0b;"><i class="fas fa-exclamation-triangle" style="margin-right:3px;"></i>${data.not_found_count} not found</span>`;
            }
            html += `</div>`;
            if (data.not_found_count > 0 && data.not_found.length <= 10) {
                html += `<div style="margin-top:6px;font-size:11px;color:#64748b;max-height:80px;overflow-y:auto;">Not found: ${data.not_found.map(e => _esc(e)).join(', ')}</div>`;
            }
            if (infoEl) infoEl.innerHTML = html;
        } catch (e) {
            if (infoEl) infoEl.innerHTML = `<span style="color:#f87171;font-size:12px;"><i class="fas fa-exclamation-circle"></i> Error reading file</span>`;
        }
    }

    async function startDoAllAppeal() {
        if (_appealChecked.size === 0) { App.toast('Select at least one profile', 'error'); return; }
        const profileIds = Array.from(_appealChecked);
        const workers = parseInt(document.getElementById('appealWorkers')?.value || '3', 10);
        closeAppealModal();
        try {
            const data = await _api('/api/profiles/do-all-appeal', {
                method: 'POST',
                body: JSON.stringify({ num_workers: workers, profile_ids: profileIds })
            });
            if (data.success) {
                App.toast(`Appeal started on ${profileIds.length} profile(s)`, 'success');
                _startOpProgress('appeal');
                _startStatusPolling();
            }
            else App.toast(data.message || data.error || 'Failed', 'error');
        } catch (e) { App.toast('Appeal error', 'error'); }
    }

    // ── Health Modal ─────────────────────────────────────────────────────────

    let _healthModalProfiles = [];
    const _healthChecked = new Set();
    let _healthProfileSearch = '';
    let _healthProfilePage = 1;
    let _healthGroupFilter = '';

    function _filteredHealth() {
        let list = _healthModalProfiles;
        const q = _healthProfileSearch.trim().toLowerCase();
        if (q) list = list.filter(p =>
            (p.email || '').toLowerCase().includes(q) || (p.name || '').toLowerCase().includes(q)
        );
        if (_healthGroupFilter) list = list.filter(p => {
            const gs = (p.groups && p.groups.length) ? p.groups : [(p.group || 'default')];
            return gs.map(g => g.toLowerCase()).includes(_healthGroupFilter.toLowerCase());
        });
        return list;
    }

    // ── Group helpers ──────────────────────────────────────────────────────────

    function _refreshGroupsFromProfiles(allProfiles) {
        if (!allProfiles || !allProfiles.length) return;
        const groupSet = new Set();
        allProfiles.forEach(p => {
            const gs = (p.groups && p.groups.length) ? p.groups : [(p.group || 'default')];
            gs.forEach(g => { if (g && g.trim()) groupSet.add(g.trim()); });
        });
        const groups = [...groupSet].sort();
        const selectors = ['pmGroupFilter', 'appealGroupFilter', 'healthGroupFilter'];
        selectors.forEach(id => {
            const el = document.getElementById(id);
            if (!el) return;
            const current = el.value;
            el.innerHTML = '<option value="">All Groups</option>';
            groups.forEach(g => {
                const opt = document.createElement('option');
                opt.value = g;
                opt.textContent = g;
                el.appendChild(opt);
            });
            el.value = (groups.includes(current)) ? current : '';
        });
        // Also update datalists
        const datalists = ['pmGroupList', 'batchLoginGroupList'];
        datalists.forEach(id => {
            const dl = document.getElementById(id);
            if (!dl) return;
            dl.innerHTML = '';
            groups.forEach(g => {
                const opt = document.createElement('option');
                opt.value = g;
                dl.appendChild(opt);
            });
        });
    }

    async function _loadGroups() {
        try {
            const data = await _api('/api/profiles/groups');
            const groups = data.groups || [];

            // Populate all group selects/datalists
            const selectors = ['pmGroupFilter', 'appealGroupFilter', 'healthGroupFilter'];
            selectors.forEach(id => {
                const el = document.getElementById(id);
                if (!el) return;
                const current = el.value;
                // Keep "All Groups" option
                el.innerHTML = '<option value="">All Groups</option>';
                groups.forEach(g => {
                    const opt = document.createElement('option');
                    opt.value = g;
                    opt.textContent = g;
                    el.appendChild(opt);
                });
                el.value = current;
            });

            // Populate datalists for text inputs
            const datalists = ['pmGroupList', 'batchLoginGroupList'];
            datalists.forEach(id => {
                const dl = document.getElementById(id);
                if (!dl) return;
                dl.innerHTML = '';
                groups.forEach(g => {
                    const opt = document.createElement('option');
                    opt.value = g;
                    dl.appendChild(opt);
                });
            });
        } catch (e) { /* ignore */ }
    }

    async function openHealthModal() {
        const modal = document.getElementById('healthModal');
        if (!modal) return;

        _healthProfileSearch = '';
        _healthProfilePage = 1;
        _healthGroupFilter = '';
        const hSearchEl = document.getElementById('healthProfileSearchInput');
        if (hSearchEl) hSearchEl.value = '';
        const hGroupEl = document.getElementById('healthGroupFilter');
        if (hGroupEl) hGroupEl.value = '';
        _loadGroups();

        _healthModalProfiles = [];
        _healthChecked.clear();
        const listEl = document.getElementById('healthProfileList');
        if (listEl) listEl.innerHTML = '<div style="color:#64748b;font-size:12px;text-align:center;padding:20px;">Loading...</div>';
        _updateHealthCount();
        modal.style.display = 'flex';

        try {
            const data = await _api('/api/profiles?per_page=1000');
            _healthModalProfiles = (data.profiles || data || []);
        } catch (e) {
            _healthModalProfiles = [];
        }

        // Pre-check profiles selected in main table (only those from current group filter if active)
        _healthModalProfiles.forEach(p => {
            if (_selectedIds.has(p.id)) _healthChecked.add(p.id);
        });
        // If none pre-selected, DON'T auto-check all — let user pick via Select All button

        _renderHealthProfileList();
        _updateHealthCount();
    }

    function _renderHealthProfileList() {
        const container = document.getElementById('healthProfileList');
        if (!container) return;

        const filtered = _filteredHealth();
        if (!filtered.length) {
            container.innerHTML = '<div style="color:#64748b;font-size:12px;text-align:center;padding:20px;">No profiles found</div>';
            return;
        }

        const totalPages = Math.max(1, Math.ceil(filtered.length / _MODAL_PAGE_SIZE));
        if (_healthProfilePage > totalPages) _healthProfilePage = totalPages;
        const pageItems = filtered.slice((_healthProfilePage - 1) * _MODAL_PAGE_SIZE, _healthProfilePage * _MODAL_PAGE_SIZE);

        const cards = pageItems.map(p => {
            const checked = _healthChecked.has(p.id) ? 'checked' : '';
            const email = p.email || p.name || p.id;
            const status = p.login_status || p.status || 'unknown';
            const engTag = p.engine === 'nst' ? '<span style="font-size:9px;background:rgba(59,130,246,0.2);color:#60a5fa;padding:1px 4px;border-radius:3px;">NST</span>' : '';
            const dot = status === 'logged_in' ? '#22c55e' : '#64748b';

            let healthTrack;
            if (p.last_health_at) {
                const done = p.last_health_done || 0;
                const total = p.last_health_total || 0;
                const clr = p.last_health_ok ? '#34d399' : '#f87171';
                const ico = p.last_health_ok ? '✓' : '✗';
                const hist = (p.health_history || []).slice(-5).reverse();
                const histHtml = hist.length > 1 ? hist.map(h => {
                    const d = h.date ? new Date(h.date).toLocaleDateString('en-GB', {day:'2-digit',month:'short'}) : '?';
                    const hIco = h.ok ? '<span style="color:#34d399;">✓</span>' : '<span style="color:#f87171;">✗</span>';
                    return `<span style="display:inline-flex;align-items:center;gap:2px;background:rgba(255,255,255,0.04);border-radius:3px;padding:1px 4px;font-size:9px;">${hIco} ${d}: ${h.done||0}/${h.total||0}</span>`;
                }).join('') : '';
                healthTrack = `<div style="font-size:10px;color:${clr};margin-top:3px;font-weight:600;"><span>${ico} ${_timeAgo(p.last_health_at)} — ${done}/${total} done</span></div>
                ${histHtml ? `<div style="display:flex;flex-wrap:wrap;gap:2px;margin-top:3px;">${histHtml}</div>` : ''}`;
            } else {
                healthTrack = `<div style="font-size:10px;color:#475569;margin-top:3px;">Never run</div>`;
            }

            return `<label style="display:flex;align-items:flex-start;gap:8px;padding:8px 10px;border-radius:6px;cursor:pointer;border:1px solid rgba(255,255,255,0.06);background:rgba(255,255,255,0.025);transition:background 0.12s;margin-bottom:4px;" class="health-profile-row">
                <input type="checkbox" data-id="${p.id}" ${checked} style="width:13px;height:13px;accent-color:#22c55e;flex-shrink:0;margin-top:3px;">
                <span style="width:7px;height:7px;border-radius:50%;background:${dot};flex-shrink:0;margin-top:4px;"></span>
                <div style="flex:1;min-width:0;">
                    <div style="display:flex;align-items:center;gap:4px;flex-wrap:wrap;">${engTag}<span style="font-size:12px;font-weight:600;color:#e2e8f0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">${_esc(email)}</span></div>
                    ${healthTrack}
                </div>
            </label>`;
        }).join('');

        container.innerHTML = cards + _modalPagination(_healthProfilePage, totalPages);

        container.querySelectorAll('input[type="checkbox"]').forEach(cb => {
            cb.addEventListener('change', () => {
                if (cb.checked) _healthChecked.add(cb.dataset.id);
                else _healthChecked.delete(cb.dataset.id);
                _updateHealthCount();
            });
        });
        container.querySelectorAll('.modal-pg-btn:not([disabled])').forEach(btn => {
            btn.addEventListener('click', () => {
                _healthProfilePage = parseInt(btn.dataset.pg);
                _renderHealthProfileList();
                container.scrollTop = 0;
            });
        });
    }

    function closeHealthModal() {
        const modal = document.getElementById('healthModal');
        if (modal) modal.style.display = 'none';
    }

    function _updateHealthCount() {
        // Activity count (only activity checkboxes in right panel, not profile checkboxes)
        const actChecked = document.querySelectorAll('#healthModal .health-act-item input[type="checkbox"]:checked');
        const countEl = document.getElementById('healthSelectedCount');
        if (countEl) countEl.textContent = actChecked.length;
        const countEl2 = document.getElementById('healthSelectedCount2');
        if (countEl2) countEl2.textContent = actChecked.length;
        // Profile count
        const profCount = document.getElementById('healthProfileCount');
        if (profCount) profCount.textContent = _healthChecked.size;
        const profCountFooter = document.getElementById('healthProfileCountFooter');
        if (profCountFooter) profCountFooter.textContent = _healthChecked.size;
    }

    // Smart Activity Presets — activities picked automatically based on goal
    const _SMART_PRESETS = {
        'gmail_trust': {
            activities: [
                'gmail_inbox', 'gmail_read_email', 'gmail_scroll_inbox', 'gmail_search_email', 'gmail_check_sent',
                'search_news', 'search_weather', 'search_local', 'search_restaurants', 'search_tech',
                'youtube_browse_feed', 'youtube_trending', 'youtube_watch_video',
                'maps_search_restaurants', 'maps_browse_places',
                'drive_browse', 'drive_recent',
                'account_security', 'account_activity',
                'news_headlines', 'calendar_view',
            ],
            duration: 10, rounds: 2,
        },
        'review_ready': {
            activities: [
                'maps_search_restaurants', 'maps_browse_places', 'maps_read_reviews', 'maps_view_photos', 'maps_street_view',
                'maps_coffee_shops', 'maps_hotels', 'maps_supermarkets', 'maps_parks',
                'search_restaurants', 'search_local', 'search_food', 'search_realestate',
                'gmail_inbox', 'gmail_read_email',
                'youtube_browse_feed', 'youtube_travel',
                'custom_gmb',
            ],
            duration: 8, rounds: 2,
        },
        'review_sticky': {
            activities: [
                'maps_search_restaurants', 'maps_browse_places', 'maps_read_reviews', 'maps_view_photos', 'maps_directions',
                'maps_street_view', 'maps_coffee_shops', 'maps_hotels', 'maps_parks', 'maps_supermarkets',
                'maps_gas_stations', 'maps_pharmacies', 'maps_banks', 'maps_museums',
                'search_local', 'search_restaurants', 'search_food',
                'gmail_inbox',
                'custom_gmb',
            ],
            duration: 15, rounds: 3,
        },
        'full_warmup': {
            activities: [
                'gmail_inbox', 'gmail_read_email', 'gmail_check_spam', 'gmail_check_sent', 'gmail_scroll_inbox',
                'search_news', 'search_weather', 'search_tech', 'search_movies', 'search_sports',
                'search_restaurants', 'search_local', 'search_music', 'search_books',
                'youtube_browse_feed', 'youtube_trending', 'youtube_watch_video', 'youtube_music_playlist', 'youtube_shorts',
                'maps_search_restaurants', 'maps_browse_places', 'maps_read_reviews',
                'drive_browse', 'drive_recent', 'drive_shared',
                'account_security', 'account_activity', 'account_profile',
                'news_headlines', 'news_tech',
                'shopping_electronics', 'shopping_clothing',
                'photos_browse', 'translate_phrases', 'calendar_view', 'keep_browse',
            ],
            duration: 20, rounds: 2,
        },
        'local_presence': {
            activities: [
                'maps_search_restaurants', 'maps_browse_places', 'maps_read_reviews', 'maps_view_photos',
                'maps_directions', 'maps_street_view', 'maps_coffee_shops', 'maps_hotels',
                'maps_gas_stations', 'maps_parks', 'maps_shopping_malls', 'maps_pharmacies',
                'maps_banks', 'maps_supermarkets', 'maps_museums', 'maps_gyms',
                'search_local', 'search_restaurants', 'search_food', 'search_realestate',
                'custom_gmb',
            ],
            duration: 12, rounds: 2,
        },
        'social_engage': {
            activities: [
                'youtube_browse_feed', 'youtube_trending', 'youtube_watch_video', 'youtube_shorts',
                'youtube_gaming', 'youtube_cooking', 'youtube_news', 'youtube_comedy', 'youtube_music_playlist',
                'news_headlines', 'news_tech', 'news_sports', 'news_entertainment',
                'shopping_electronics', 'shopping_clothing', 'shopping_deals',
                'gmail_inbox', 'gmail_read_email',
                'search_news', 'search_movies', 'search_sports',
            ],
            duration: 10, rounds: 2,
        },
    };

    async function startHealth() {
        const isSmartMode = !!(document.getElementById('healthSmartPanel')?.style.display !== 'none');
        const profileIds = _healthChecked.size > 0 ? Array.from(_healthChecked) : [];
        const workers = parseInt(document.getElementById('healthWorkers')?.value || '3', 10);
        const country = document.getElementById('healthCountry')?.value || 'US';
        let activities, rounds, duration, gmbName, gmbAddress, smartGoal = '';

        if (isSmartMode) {
            // Smart mode — use preset
            smartGoal = document.querySelector('input[name="healthSmartGoal"]:checked')?.value || 'gmail_trust';
            const preset = _SMART_PRESETS[smartGoal] || _SMART_PRESETS['gmail_trust'];
            activities = [...preset.activities];
            rounds = parseInt(document.getElementById('healthRounds')?.value || String(preset.rounds), 10);
            duration = parseInt(document.getElementById('healthDuration')?.value || String(preset.duration), 10);
            gmbName = (document.getElementById('healthSmartGmbName')?.value || '').trim();
            gmbAddress = (document.getElementById('healthSmartGmbAddress')?.value || '').trim();
        } else {
            // Manual mode
            activities = Array.from(document.querySelectorAll('#healthManualPanel .health-act-item input[type="checkbox"]:checked')).map(cb => cb.value);
            rounds = parseInt(document.getElementById('healthRounds')?.value || '1', 10);
            duration = parseInt(document.getElementById('healthDuration')?.value || '0', 10);
            gmbName = (document.getElementById('healthGmbName')?.value || '').trim();
            gmbAddress = (document.getElementById('healthGmbAddress')?.value || '').trim();
        }

        // Auto-add custom_gmb if GMB fields filled
        if (gmbName && gmbAddress && !activities.includes('custom_gmb')) {
            activities.unshift('custom_gmb');
        }
        if (activities.length === 0) { App.toast('Select at least one activity', 'error'); return; }

        closeHealthModal();
        try {
            const data = await _api('/api/profiles/run-health', {
                method: 'POST',
                body: JSON.stringify({
                    num_workers: workers, activities, profile_ids: profileIds, country,
                    rounds, duration_minutes: duration,
                    gmb_name: gmbName, gmb_address: gmbAddress,
                    smart_goal: smartGoal,
                })
            });
            if (data.success) {
                const modeLabel = isSmartMode ? `Smart: ${smartGoal.replace(/_/g,' ')}` : 'Manual';
                App.toast(`Health started (${modeLabel}) on ${data.total} profile(s)`, 'success');
                _startOpProgress('health');
                _startStatusPolling();
            }
            else App.toast(data.message || data.error || 'Failed', 'error');
        } catch (e) { App.toast('Health error', 'error'); }
    }

    async function cleanupOrphans() {
        try {
            const data = await _api('/api/profiles/cleanup', { method: 'POST' });
            if (data.success) App.toast(`Cleanup done. Removed ${data.removed || 0} orphan folders.`, 'success');
        } catch (e) { App.toast('Cleanup error', 'error'); }
    }

    async function checkProxy() {
        const host = _val('pmProxyHost').trim();
        const port = _val('pmProxyPort').trim();
        if (!host) { App.toast('Enter proxy host first', 'error'); return; }
        const resultEl = _$('pmProxyResult');
        if (resultEl) {
            resultEl.style.display = 'block';
            resultEl.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Checking proxy...';
        }
        // For now just show the proxy info — server-side check can be added later
        if (resultEl) {
            resultEl.innerHTML = `<i class="fas fa-check" style="color:#22c55e;"></i> Proxy: ${_esc(host)}:${_esc(port)} (check via server not yet wired)`;
        }
    }

    function parseProxyString() {
        const raw = _val('pmProxyPaste').trim();
        if (!raw) return;
        // Try common formats
        let host = '', port = '', user = '', pass = '', type = 'https';

        // socks5://user:pass@host:port or https://user:pass@host:port
        const urlMatch = raw.match(/^(socks5|http|https):\/\/([^:]+):([^@]+)@([^:]+):(\d+)/);
        if (urlMatch) {
            type = urlMatch[1] === 'socks5' ? 'socks5' : (urlMatch[1] === 'http' ? 'https' : urlMatch[1]);
            user = urlMatch[2]; pass = urlMatch[3]; host = urlMatch[4]; port = urlMatch[5];
        } else if (raw.includes('@')) {
            // user:pass@host:port
            const [auth, hp] = raw.split('@');
            const [u, p] = auth.split(':');
            const [h, pt] = hp.split(':');
            user = u || ''; pass = p || ''; host = h || ''; port = pt || '';
        } else {
            // host:port:user:pass
            const parts = raw.split(':');
            if (parts.length >= 4) { host = parts[0]; port = parts[1]; user = parts[2]; pass = parts[3]; }
            else if (parts.length === 2) { host = parts[0]; port = parts[1]; }
        }

        if (host) {
            _setVal('pmProxyType', type);
            _setVal('pmProxyHost', host);
            _setVal('pmProxyPort', port);
            _setVal('pmProxyUser', user);
            _setVal('pmProxyPass', pass);
            _toggleProxyFields();
            App.toast('Proxy parsed', 'success');
        } else {
            App.toast('Could not parse proxy string', 'error');
        }
    }

    // ── Operation Progress Panel (Rich) ────────────────────────────────
    let _opPoll = null;
    let _opType = null;
    let _opStartTime = null;
    let _opTimerInterval = null;

    // Operation type configs
    const _OP_CONFIGS = {
        'batch-login': { icon: 'fa-file-excel',   label: 'Batch Login',    successLbl: 'Logged In',   failLbl: 'Failed',  pendingLbl: 'Remaining' },
        'relogin':     { icon: 'fa-sign-in-alt',  label: 'Re-Login',       successLbl: 'Re-Logged In',failLbl: 'Failed',  pendingLbl: 'Remaining' },
        'appeal':      { icon: 'fa-gavel',        label: 'Appeal',         successLbl: 'Submitted',   failLbl: 'Refused',  pendingLbl: 'Remaining' },
        'review':      { icon: 'fa-star',         label: 'Write Review',   successLbl: 'Posted',      failLbl: 'Failed',  pendingLbl: 'Pending' },
        'proxy':       { icon: 'fa-plug',         label: 'Proxy Update',   successLbl: 'Updated',     failLbl: 'Failed',  pendingLbl: 'Remaining' },
        'health':      { icon: 'fa-heartbeat',    label: 'Health Activity', successLbl: 'Done',       failLbl: 'Failed',  pendingLbl: 'Remaining' },
        'setai':       { icon: 'fa-robot',        label: 'SetAI Hook',     successLbl: 'Hooked',      failLbl: 'Failed',  pendingLbl: 'Remaining' },
        'delete':      { icon: 'fa-trash',        label: 'Delete Profiles', successLbl: 'Deleted',    failLbl: 'Failed',  pendingLbl: 'Remaining' },
        'run-ops':     { icon: 'fa-cogs',         label: 'Run Operations',  successLbl: 'Done',       failLbl: 'Failed',  pendingLbl: 'Remaining' },
        'gmb-review':  { icon: 'fa-link',         label: 'GMB Review URL',  successLbl: 'Resolved',   failLbl: 'Failed',  pendingLbl: 'Remaining' },
    };

    // Auto-detect running operations on page load / refresh
    async function _autoDetectRunningOps() {
        if (!App.state.serverOnline || _opType) return;
        try {
            // Check all operation status endpoints
            const checks = [
                { type: 'run-ops', url: '/api/profiles/ops-status' },
                { type: 'appeal', url: '/api/profiles/appeal-status' },
                { type: 'health', url: '/api/profiles/health-status' },
                { type: 'review', url: '/api/profiles/review-status' },
                { type: 'gmb-review', url: '/api/gmb-to-review/status' },
            ];
            for (const chk of checks) {
                try {
                    const res = await App.apiFetch(chk.url);
                    const st = await res.json();
                    if (st.running) {
                        _startOpProgress(chk.type);
                        return;
                    }
                } catch (e) { /* ignore */ }
            }
            // Check bulk delete progress
            try {
                const delRes = await App.apiFetch('/api/profiles/delete-bulk-status');
                const delData = await delRes.json();
                if (delData.success && delData.progress && delData.progress.status === 'processing') {
                    _startOpProgress('delete'); return;
                }
            } catch (e) { /* ignore */ }
            // Check main progress endpoint for batch login / relogin
            try {
                const res = await App.apiFetch('/api/progress');
                const data = await res.json();
                if (data.success && data.progress && data.progress.status === 'processing') {
                    const jt = data.progress.job_type;
                    if (jt === 'batch_login') { _startOpProgress('batch-login'); return; }
                    if (jt === 'bulk_relogin') { _startOpProgress('relogin'); return; }
                }
            } catch (e) { /* ignore */ }
        } catch (e) { /* ignore */ }
    }

    // Expose for app.js to call after auth
    App._autoDetectRunningOps = _autoDetectRunningOps;

    function _formatTimer(ms) {
        const s = Math.floor(ms / 1000);
        const m = Math.floor(s / 60);
        const sec = s % 60;
        return `${String(m).padStart(2,'0')}:${String(sec).padStart(2,'0')}`;
    }

    function _setOpStat(id, val) {
        const el = document.getElementById(id);
        if (!el) return;
        const valEl = el.querySelector('.op-stat-val');
        if (valEl) valEl.textContent = val;
    }
    function _setOpStatLabel(id, lbl) {
        const el = document.getElementById(id);
        if (!el) return;
        const lblEl = el.querySelector('.op-stat-lbl');
        if (lblEl) lblEl.textContent = lbl;
    }

    function _startOpProgress(type) {
        _opType = type;
        _opStartTime = Date.now();
        const cssType = 'op-type-' + type;

        const panel   = document.getElementById('opProgressPanel');
        const iconW   = document.getElementById('opProgressIconWrap');
        const icon    = document.getElementById('opProgressIcon');
        const label   = document.getElementById('opProgressLabel');
        const sublabel = document.getElementById('opProgressSublabel');
        const bar     = document.getElementById('opProgressBar');
        const count   = document.getElementById('opProgressCount');
        const pctEl   = document.getElementById('opProgressPct');
        const timerEl = document.getElementById('opProgressTimer');
        if (!panel) return;

        const cfg = _OP_CONFIGS[type] || _OP_CONFIGS['health'];

        // Set icon + colors based on type
        if (icon) icon.className = 'fas ' + cfg.icon;
        if (iconW) { iconW.className = 'op-icon-wrap ' + cssType; }
        if (label) label.textContent = cfg.label;
        if (sublabel) sublabel.textContent = 'Starting...';
        if (bar) { bar.className = 'op-bar-fill ' + cssType; bar.style.width = '0%'; }
        if (count) count.textContent = '0 / 0';
        if (pctEl) pctEl.textContent = '0%';
        if (timerEl) timerEl.textContent = '00:00';

        // Set stat labels
        _setOpStatLabel('opStatSuccess', cfg.successLbl);
        _setOpStatLabel('opStatFailed', cfg.failLbl);
        _setOpStatLabel('opStatPending', cfg.pendingLbl);
        _setOpStat('opStatTotal', '0');
        _setOpStat('opStatSuccess', '0');
        _setOpStat('opStatFailed', '0');
        _setOpStat('opStatPending', '0');

        panel.style.display = 'block';

        // Reset Stop button (in case it was changed to Close from a previous run)
        const stopBtn = document.getElementById('opStopBtn');
        if (stopBtn) {
            stopBtn.innerHTML = '<i class="fas fa-stop"></i> Stop';
            stopBtn.dataset.completed = '';
        }

        // Timer update
        if (_opTimerInterval) clearInterval(_opTimerInterval);
        _opTimerInterval = setInterval(() => {
            if (timerEl && _opStartTime) timerEl.textContent = _formatTimer(Date.now() - _opStartTime);
        }, 1000);

        // Status polling
        if (_opPoll) clearInterval(_opPoll);
        _opPoll = setInterval(async () => {
            try {
                let done = 0, total = 0, successCount = 0, failedCount = 0, isRunning = true;
                let reportPath = null;

                if (type === 'delete') {
                    // Bulk delete progress endpoint
                    const st = await _api('/api/profiles/delete-bulk-status');
                    if (st.success && st.progress) {
                        const p = st.progress;
                        total = p.total || 0;
                        done = (p.deleted || 0) + (p.failed || 0);
                        successCount = p.deleted || 0;
                        failedCount = p.failed || 0;
                        isRunning = p.status === 'processing';
                    }
                } else if (type === 'batch-login' || type === 'relogin') {
                    // These use the main progress endpoint
                    const res = await App.apiFetch('/api/progress');
                    const data = await res.json();
                    if (data.success && data.progress) {
                        const p = data.progress;
                        total = p.total || 0;
                        done = p.current || 0;
                        successCount = p.success || 0;
                        failedCount = p.failed || 0;
                        isRunning = p.status === 'processing';
                        reportPath = p.report_path;
                    }
                } else if (type === 'gmb-review') {
                    const st = await _api('/api/gmb-to-review/status');
                    done = st.done || 0;
                    total = st.total || 0;
                    isRunning = !!st.running;
                    successCount = st.success || 0;
                    failedCount = st.failed || 0;
                    reportPath = st.report_path;
                } else {
                    let endpoint;
                    if (type === 'run-ops') endpoint = '/api/profiles/ops-status';
                    else if (type === 'appeal') endpoint = '/api/profiles/appeal-status';
                    else if (type === 'review') endpoint = '/api/profiles/review-status';
                    else endpoint = '/api/profiles/health-status';

                    const st = await _api(endpoint);
                    done = st.done || 0;
                    total = st.total || 0;
                    isRunning = !!st.running;
                    reportPath = st.report_path;

                    if (st.results && st.results.length) {
                        successCount = st.results.filter(r => r.status === 'success' || r.ok || r.success === true).length;
                        failedCount  = st.results.filter(r => r.status === 'failed' || r.status === 'error' || r.ok === false || r.success === false).length;
                    } else {
                        successCount = st.success || done;
                        failedCount  = st.failed || 0;
                    }
                }

                const pct = total > 0 ? Math.round((done / total) * 100) : 0;
                const remaining = Math.max(0, total - done);

                // Update panel
                if (count) count.textContent = `${done} / ${total}`;
                if (pctEl) pctEl.textContent = pct + '%';
                if (bar) bar.style.width = `${pct}%`;
                if (sublabel) sublabel.textContent = isRunning ? `Processing... ${done}/${total}` : 'Complete';
                _setOpStat('opStatTotal', total);
                _setOpStat('opStatSuccess', successCount);
                _setOpStat('opStatFailed', failedCount);
                _setOpStat('opStatPending', remaining);

                // Also update main Dashboard overview for ALL operation types
                {
                    const overviewBar  = document.getElementById('progressBar');
                    const overviewPct  = document.getElementById('progressPercentage');
                    const overviewTxt  = document.getElementById('progressText');
                    const overviewCur  = document.getElementById('currentAccount');
                    const overviewStep = document.getElementById('stepIndicator');
                    if (overviewBar) overviewBar.style.width = pct + '%';
                    if (overviewPct) overviewPct.innerText = pct + '%';
                    if (overviewTxt) overviewTxt.innerText = `${cfg.label}: ${done} / ${total}`;
                    if (overviewCur) overviewCur.innerText = isRunning
                        ? `${cfg.label.toUpperCase()} RUNNING... (${done}/${total})`
                        : `${cfg.label.toUpperCase()} COMPLETE`;
                    if (overviewStep) overviewStep.innerText = cfg.label.toUpperCase();
                    const _updCard = (id, val, lbl, ico) => {
                        const el = document.getElementById(id);
                        if (!el) return;
                        el.innerText = val;
                        const card = el.closest('.stat-card');
                        if (!card) return;
                        const lblEl = card.querySelector('.stat-label');
                        if (lblEl) lblEl.innerText = lbl;
                        const icoEl = card.querySelector('.stat-icon i');
                        if (icoEl) icoEl.className = 'fas ' + ico;
                    };
                    _updCard('totalAccounts', total,        'Total Profiles',  'fa-users');
                    _updCard('totalSuccess',  successCount, cfg.successLbl,    'fa-check-circle');
                    _updCard('totalFailed',   failedCount,  cfg.failLbl,       'fa-times-circle');
                    _updCard('totalPending',  remaining,    cfg.pendingLbl,    'fa-hourglass-half');
                }

                if (!isRunning) {
                    // Stop polling but keep panel visible with final state
                    if (_opPoll) { clearInterval(_opPoll); _opPoll = null; }
                    if (_opTimerInterval) { clearInterval(_opTimerInterval); _opTimerInterval = null; }

                    // Update panel to show completion state
                    if (sublabel) sublabel.textContent = 'Complete';
                    if (bar) bar.style.width = '100%';
                    if (pctEl) pctEl.textContent = '100%';

                    // Switch Stop button to Close button
                    const stopBtn = document.getElementById('opStopBtn');
                    if (stopBtn) {
                        stopBtn.innerHTML = '<i class="fas fa-times"></i> Close';
                        stopBtn.dataset.completed = 'true';  // flag for click handler
                    }

                    // Show toast notifications
                    if (type === 'review' && reportPath) {
                        _showReviewReportReady(reportPath, done, total);
                    } else if (type === 'review') {
                        App.toast(`Write Review complete: ${done}/${total} done`, 'success');
                    } else if (type === 'delete') {
                        App.toast(`Delete complete: ${successCount} deleted, ${failedCount} failed`, 'success');
                    } else if (type === 'run-ops') {
                        App.toast(`Operations complete: ${successCount} done, ${failedCount} failed`, 'success');
                    } else if (type === 'batch-login') {
                        App.toast(`Batch Login complete: ${successCount} logged in, ${failedCount} failed`, 'success');
                    } else if (type === 'relogin') {
                        App.toast(`Re-Login complete: ${successCount} success, ${failedCount} failed`, 'success');
                        if (reportPath) _showReloginReportReady(reportPath, { success: successCount, failed: failedCount });
                    } else if (type === 'gmb-review') {
                        App.toast(`GMB Review URLs: ${successCount} resolved, ${failedCount} failed`, 'success');
                        // Auto-download the generated Excel
                        if (reportPath) {
                            try {
                                const dlResp = await App.apiFetch('/api/gmb-to-review/download');
                                if (dlResp.ok) {
                                    const cd = dlResp.headers.get('Content-Disposition') || '';
                                    const m = cd.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
                                    const fn = m ? m[1].replace(/['"]/g, '') : 'GMB_Review_URLs.xlsx';
                                    const blob = await dlResp.blob();
                                    const u = URL.createObjectURL(blob);
                                    const a = document.createElement('a');
                                    a.href = u; a.download = fn;
                                    document.body.appendChild(a); a.click();
                                    setTimeout(() => { document.body.removeChild(a); URL.revokeObjectURL(u); }, 1000);
                                }
                            } catch(e) { /* ignore download error */ }
                            // Refresh Reports tab so file is always accessible
                            if (typeof App !== 'undefined' && App.loadReports) setTimeout(() => App.loadReports(), 1000);
                        }
                    }
                    loadProfiles();
                }
            } catch (e) { /* ignore */ }
        }, 2000);
    }

    function _showReloginReportReady(reportPath, p) {
        const ok = p.success || 0, fail = p.failed || 0;
        App.toast(`✓ Re-Login Report Ready — ${ok} success, ${fail} failed`, 'success');
        if (typeof App !== 'undefined' && App.loadReports) setTimeout(() => App.loadReports(), 1000);
        if (window.electronAPI && window.electronAPI.openPath) {
            window.electronAPI.openPath(reportPath);
        }
    }

    function _showReviewReportReady(reportPath, done, total) {
        const live = reportPath.match(/(\d+)live/)?.[1] || '?';
        App.toast(`✓ Review Report Ready — ${live} live, ${done}/${total} done`, 'success');
        // Refresh Results tab so report shows up there immediately
        if (typeof App !== 'undefined' && App.loadReports) {
            setTimeout(() => App.loadReports(), 1000);
        }
        // Show a persistent notification bar if possible
        const bar = document.getElementById('reviewReportBar');
        if (bar) {
            bar.style.display = 'flex';
            const link = bar.querySelector('#reviewReportPath');
            if (link) { link.textContent = reportPath.split(/[\\/]/).pop(); link.dataset.path = reportPath; }
        }
    }

    function _stopOpProgress(sendStop = true) {
        if (_opPoll) { clearInterval(_opPoll); _opPoll = null; }
        if (_opTimerInterval) { clearInterval(_opTimerInterval); _opTimerInterval = null; }
        if (sendStop && _opType) {
            let endpoint;
            if (_opType === 'appeal') endpoint = '/api/profiles/stop-appeal';
            else if (_opType === 'review') endpoint = '/api/profiles/stop-review';
            else endpoint = '/api/profiles/stop-health';
            _api(endpoint, { method: 'POST' }).catch(() => {});
        }
        const panel = document.getElementById('opProgressPanel');
        if (panel) panel.style.display = 'none';
        _opType = null;
        _opStartTime = null;
        // Auto-refresh Report Ledger so new report appears immediately
        if (typeof App !== 'undefined' && App.loadReports) {
            setTimeout(() => App.loadReports(), 1500);
        }
    }

    // ── Status polling (real-time sync) ────────────────────────────────
    // Polls every 2s while any browser is open. Auto-stops when none are running.

    function _startStatusPolling() {
        if (_statusPoll) return;  // already polling
        _statusPoll = setInterval(async () => {
            try {
                await loadProfiles();
                // Auto-stop polling if no browsers are running or starting
                const rows = document.querySelectorAll('.pm-close-btn, .pm-launching-btn');
                if (rows.length === 0 && _statusPoll) {
                    clearInterval(_statusPoll);
                    _statusPoll = null;
                }
            } catch (e) { /* ignore */ }
        }, 2000);
    }

    function _stopStatusPolling() {
        if (_statusPoll) { clearInterval(_statusPoll); _statusPoll = null; }
    }

    // ── File browser ────────────────────────────────────────────────────

    async function browseFile(inputId) {
        try {
            if (window.electronAPI && window.electronAPI.selectFile) {
                const filePath = await window.electronAPI.selectFile();
                if (filePath) _setVal(inputId, filePath);
            } else if (window.electronAPI && window.electronAPI.selectFolder && inputId === 'profileStoragePath') {
                const folderPath = await window.electronAPI.selectFolder();
                if (folderPath) _setVal(inputId, folderPath);
            } else {
                App.toast('File picker not available', 'error');
            }
        } catch (e) { App.toast('File picker error', 'error'); }
    }

    // ══════════════════════════════════════════════════════════════════════
    // SETUP — Wire up all buttons and events
    // ══════════════════════════════════════════════════════════════════════

    App.loadProfiles = loadProfiles;

    App.setupProfilesPage = function () {
        // Search
        const searchEl = _$('profileSearch');
        if (searchEl) {
            searchEl.addEventListener('input', () => {
                clearTimeout(_searchDebounce);
                _searchDebounce = setTimeout(() => loadProfiles(), 300);
            });
        }

        // Group filter dropdown
        const groupFilterEl = document.getElementById('pmGroupFilter');
        if (groupFilterEl) {
            groupFilterEl.addEventListener('change', () => {
                _currentGroup = groupFilterEl.value;
                loadProfiles();
            });
        }

        // Filter buttons
        document.querySelectorAll('.pm-filter-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.pm-filter-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                _currentFilter = btn.dataset.filter;
                loadProfiles();
            });
        });

        // Select All checkbox — only selects VISIBLE (filtered) profiles
        const selectAll = _$('pmSelectAll');
        if (selectAll) {
            selectAll.addEventListener('change', () => {
                if (selectAll.checked) {
                    // Add only currently visible rows (respects group/status/search filter)
                    document.querySelectorAll('.pm-row-check').forEach(cb => {
                        cb.checked = true;
                        _selectedIds.add(cb.dataset.id);
                        cb.closest('.pm-row').classList.add('pm-selected');
                    });
                } else {
                    // Clear ALL selections (not just visible)
                    _selectedIds.clear();
                    document.querySelectorAll('.pm-row-check').forEach(cb => {
                        cb.checked = false;
                        cb.closest('.pm-row').classList.remove('pm-selected');
                    });
                }
                _updateBulkBar();
            });
        }

        // Bulk re-login
        _btn('pmBulkReloginBtn', async () => {
            if (!_selectedIds.size) { App.toast('Select profiles first', 'warn'); return; }
            const workers = parseInt(_$('pmBulkReloginWorkers') ? _$('pmBulkReloginWorkers').value : '2') || 2;
            const staggerDelay = parseInt(_$('pmBulkReloginStagger') ? _$('pmBulkReloginStagger').value : '3') || 3;
            try {
                const data = await _api('/api/profiles/bulk-relogin', {
                    method: 'POST',
                    body: JSON.stringify({ ids: [..._selectedIds], workers, stagger_delay: staggerDelay })
                });
                if (data.success) {
                    App.toast(`Re-login started for ${data.total} profiles (${workers} workers)`, 'success');
                    _startOpProgress('relogin');
                    _startStatusPolling();
                } else App.toast(data.error || 'Failed to start re-login', 'error');
            } catch(e) { App.toast('Re-login error: ' + e.message, 'error'); }
        });

        // Export selected profiles to Excel
        _btn('pmBulkExportBtn', async () => {
            if (!_selectedIds.size) { App.toast('Select profiles first', 'warn'); return; }
            const btn = _$('pmBulkExportBtn');
            if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Exporting…'; }
            try {
                const resp = await App.apiFetch('/api/profiles/export-excel', {
                    method: 'POST',
                    body: JSON.stringify({ ids: [..._selectedIds] })
                });
                if (!resp.ok) {
                    const err = await resp.json().catch(() => ({}));
                    throw new Error(err.message || `HTTP ${resp.status}`);
                }
                // Extract filename from Content-Disposition header
                const cd = resp.headers.get('Content-Disposition') || '';
                const match = cd.match(/filename[^;=\n]*=((['"]).*?\2|[^;\n]*)/);
                const filename = match ? match[1].replace(/['"]/g, '') : 'profiles_export.xlsx';

                const blob = await resp.blob();
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = filename;
                document.body.appendChild(a);
                a.click();
                setTimeout(() => { document.body.removeChild(a); URL.revokeObjectURL(url); }, 1000);

                App.toast(`✅ Exported ${_selectedIds.size} profiles → ${filename}`, 'success');
            } catch(e) {
                App.toast('Export failed: ' + e.message, 'error');
            } finally {
                if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fas fa-file-excel"></i> Export'; }
            }
        });

        // Bulk bar buttons
        _btn('pmBulkAddBtn', _bulkAddToGroup);
        _btn('pmBulkMoveBtn', _bulkMoveToGroup);
        _btn('pmBulkRemoveBtn', _bulkRemoveFromGroup);
        _btn('pmBulkDeleteBtn', deleteSelectedProfiles);
        _btn('pmBulkSaveNoteBtn', _bulkSaveNoteOnly);
        _btn('pmBulkUpdateProxyBtn', _bulkUpdateProxy);
        _btn('pmBulkClearBtn', () => {
            _selectedIds.clear();
            document.querySelectorAll('.pm-row-check').forEach(cb => {
                cb.checked = false;
                if (cb.closest('.pm-row')) cb.closest('.pm-row').classList.remove('pm-selected');
            });
            const sa = _$('pmSelectAll'); if (sa) sa.checked = false;
            _updateBulkBar();
        });

        // Group manager
        _btn('manageGroupsBtn', _openGroupManager);
        _btn('groupManagerCloseBtn', _closeGroupManager);
        _btn('groupManagerDoneBtn', _closeGroupManager);
        _btn('createGroupBtn', _createGroup);
        _btn('renameGroupCloseBtn', _closeRenameGroup);
        _btn('renameGroupCancelBtn', _closeRenameGroup);
        _btn('renameGroupConfirmBtn', _confirmRenameGroup);
        _btn('deleteGroupCloseBtn', _closeDeleteGroup);
        _btn('deleteGroupCancelBtn', _closeDeleteGroup);
        _btn('deleteGroupConfirmBtn', _confirmDeleteGroup);

        // Create dropdown
        _btn('profileCreateBtn', (e) => {
            e.stopPropagation();
            _$('profileCreateMenu').classList.toggle('show');
        });
        _btn('pmCreateSingle', (e) => { e.preventDefault(); _$('profileCreateMenu').classList.remove('show'); openCreateModal(); });
        _btn('pmBatchCreate', (e) => { e.preventDefault(); _$('profileCreateMenu').classList.remove('show'); App.toast('Batch create coming soon', 'info'); });
        _btn('pmBatchImport', (e) => { e.preventDefault(); _$('profileCreateMenu').classList.remove('show'); App.toast('Batch import coming soon', 'info'); });

        // Close dropdown on outside click
        document.addEventListener('click', () => {
            const menu = _$('profileCreateMenu');
            if (menu) menu.classList.remove('show');
            _hideContextMenu();
        });

        // Action buttons
        _btn('profileCloseAllBtn', closeAllProfiles);
        _btn('profileCleanupBtn', cleanupOrphans);
        _btn('profileBatchLoginBtn', openBatchLoginModal);
        _btn('profileRunOpsBtn', openRunOpsModal);
        _setupRunOpsModal();
        _btn('profileDoAllAppealBtn', openAppealModal);
        _btn('appealModalClose', closeAppealModal);
        _btn('appealModalCancelBtn', closeAppealModal);
        _btn('appealModalStartBtn', startDoAllAppeal);
        _btn('appealSelectAll', () => {
            // Only select profiles visible in current filter (group + search)
            _filteredAppeal().forEach(p => _appealChecked.add(p.id));
            _renderAppealList();
            _updateAppealCount();
        });
        _btn('appealDeselectAll', () => {
            // Clear ALL selections (clean slate), not just filtered
            _appealChecked.clear();
            _renderAppealList();
            _updateAppealCount();
        });
        const appealSearchEl = document.getElementById('appealSearchInput');
        if (appealSearchEl) {
            appealSearchEl.addEventListener('input', () => {
                _appealSearch = appealSearchEl.value;
                _appealPage = 1;
                _renderAppealList();
            });
        }
        const appealGroupEl = document.getElementById('appealGroupFilter');
        if (appealGroupEl) {
            appealGroupEl.addEventListener('change', () => {
                _appealGroupFilter = appealGroupEl.value;
                _appealPage = 1;
                // Clear old selections when group changes to avoid cross-group accumulation
                _appealChecked.clear();
                _renderAppealList();
                _updateAppealCount();
            });
        }
        // Appeal mode toggle (Select vs Excel)
        _btn('appealModeSelectBtn', () => _setAppealMode('select'));
        _btn('appealModeExcelBtn', () => _setAppealMode('excel'));
        _btn('appealExcelBrowseBtn', _appealBrowseExcel);

        document.getElementById('appealModal')?.addEventListener('click', e => {
            if (e.target === document.getElementById('appealModal')) closeAppealModal();
        });
        _btn('profileHealthBtn', openHealthModal);
        _btn('profileWriteReviewBtn', openWriteReviewModal);
        _btn('writeReviewCloseBtn', closeWriteReviewModal);
        _btn('writeReviewCancelBtn', closeWriteReviewModal);
        _btn('writeReviewStartBtn', startWriteReview);
        _btn('writeReviewBrowseBtn', async () => { await browseFile('writeReviewFilePath'); _previewWRFile(); });

        // GMB → Review URL
        _btn('profileGmbToReviewBtn', openGmbToReviewModal);
        _btn('gmbToReviewCloseBtn', closeGmbToReviewModal);
        _btn('gmbToReviewCancelBtn', closeGmbToReviewModal);
        _btn('gmbToReviewStartBtn', startGmbToReview);
        _btn('gmbToReviewBrowseBtn', async () => { await browseFile('gmbToReviewFilePath'); _previewGmbFile(); });
        const gmbFileInp = _$('gmbToReviewFilePath');
        if (gmbFileInp) gmbFileInp.addEventListener('input', _previewGmbFile);

        // Export template
        _btn('writeReviewTemplateBtn', async () => {
            const btn = _$('writeReviewTemplateBtn');
            if (btn) { btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Downloading…'; }
            try {
                const resp = await App.apiFetch('/api/profiles/write-review-template', { method: 'GET' });
                if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
                const blob = await resp.blob();
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = 'WriteReview_Template.xlsx';
                document.body.appendChild(a);
                a.click();
                setTimeout(() => { document.body.removeChild(a); URL.revokeObjectURL(url); }, 1000);
                App.toast('✅ Template downloaded — open it to see instructions + examples', 'success');
            } catch(e) {
                App.toast('Template download failed: ' + e.message, 'error');
            } finally {
                if (btn) { btn.disabled = false; btn.innerHTML = '<i class="fas fa-file-download"></i> Export Template'; }
            }
        });
        const wrFileInp = _$('writeReviewFilePath');
        if (wrFileInp) wrFileInp.addEventListener('input', _previewWRFile);
        _btn('healthModalClose', closeHealthModal);
        _btn('healthModalCancelBtn', closeHealthModal);
        _btn('healthModalStartBtn', startHealth);

        // Smart vs Manual mode toggle
        document.querySelectorAll('.health-mode-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.health-mode-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                const mode = btn.dataset.mode;
                const smartPanel = document.getElementById('healthSmartPanel');
                const manualPanel = document.getElementById('healthManualPanel');
                if (smartPanel) smartPanel.style.display = mode === 'smart' ? 'block' : 'none';
                if (manualPanel) manualPanel.style.display = mode === 'manual' ? 'block' : 'none';
                // When switching to smart, auto-set recommended duration
                if (mode === 'smart') {
                    const goal = document.querySelector('input[name="healthSmartGoal"]:checked')?.value || 'gmail_trust';
                    const preset = _SMART_PRESETS[goal];
                    if (preset) {
                        const durEl = document.getElementById('healthDuration');
                        const rndEl = document.getElementById('healthRounds');
                        if (durEl && !parseInt(durEl.value)) durEl.value = preset.duration;
                        if (rndEl) rndEl.value = preset.rounds;
                    }
                }
            });
        });
        // Smart goal change → update recommended duration
        document.querySelectorAll('input[name="healthSmartGoal"]').forEach(radio => {
            radio.addEventListener('change', () => {
                const preset = _SMART_PRESETS[radio.value];
                if (preset) {
                    const durEl = document.getElementById('healthDuration');
                    const rndEl = document.getElementById('healthRounds');
                    if (durEl) durEl.value = preset.duration;
                    if (rndEl) rndEl.value = preset.rounds;
                }
            });
        });
        _btn('healthSelectAll', () => {
            document.querySelectorAll('#healthModal .health-act-item input[type="checkbox"]').forEach(cb => cb.checked = true);
            _updateHealthCount();
        });
        _btn('healthDeselectAll', () => {
            document.querySelectorAll('#healthModal .health-act-item input[type="checkbox"]').forEach(cb => cb.checked = false);
            _updateHealthCount();
        });
        _btn('healthRandomPick', () => {
            const countInput = document.getElementById('healthRandomCount');
            const n = Math.max(1, parseInt(countInput ? countInput.value : '5') || 5);
            const allCbs = Array.from(document.querySelectorAll('#healthModal .health-act-item input[type="checkbox"]'));
            // Uncheck all first
            allCbs.forEach(cb => cb.checked = false);
            // Shuffle and pick N
            const shuffled = allCbs.sort(() => Math.random() - 0.5);
            const pick = Math.min(n, shuffled.length);
            for (let i = 0; i < pick; i++) shuffled[i].checked = true;
            _updateHealthCount();
            App.toast(`Randomly picked ${pick} activities`, 'info');
        });
        _btn('healthProfileSelectAll', () => {
            // Only select profiles visible in current filter (group + search)
            _filteredHealth().forEach(p => _healthChecked.add(p.id));
            _renderHealthProfileList();
            _updateHealthCount();
        });
        _btn('healthProfileDeselectAll', () => {
            // Clear ALL selections (clean slate)
            _healthChecked.clear();
            _renderHealthProfileList();
            _updateHealthCount();
        });
        const healthSearchEl = document.getElementById('healthProfileSearchInput');
        if (healthSearchEl) {
            healthSearchEl.addEventListener('input', () => {
                _healthProfileSearch = healthSearchEl.value;
                _healthProfilePage = 1;
                _renderHealthProfileList();
            });
        }
        const healthGroupEl = document.getElementById('healthGroupFilter');
        if (healthGroupEl) {
            healthGroupEl.addEventListener('change', () => {
                _healthGroupFilter = healthGroupEl.value;
                _healthProfilePage = 1;
                // Clear old selections when group changes to avoid cross-group accumulation
                _healthChecked.clear();
                _renderHealthProfileList();
                _updateHealthCount();
            });
        }
        document.querySelectorAll('#healthModal .health-act-item input[type="checkbox"]').forEach(cb => {
            cb.addEventListener('change', _updateHealthCount);
        });
        const healthModal = document.getElementById('healthModal');
        if (healthModal) {
            healthModal.addEventListener('click', (e) => {
                if (e.target === healthModal) closeHealthModal();
            });
        }
        _btn('opStopBtn', () => {
            const btn = document.getElementById('opStopBtn');
            if (btn && btn.dataset.completed === 'true') {
                // Operation already finished — just close the panel
                const panel = document.getElementById('opProgressPanel');
                if (panel) panel.style.display = 'none';
                _opType = null;
                _opStartTime = null;
                btn.dataset.completed = '';
                if (typeof App !== 'undefined' && App.loadReports) setTimeout(() => App.loadReports(), 500);
            } else {
                _stopOpProgress(true);
            }
        });
        _btn('reviewReportDismissBtn', () => {
            const bar = document.getElementById('reviewReportBar');
            if (bar) bar.style.display = 'none';
        });
        _btn('reviewReportOpenBtn', async () => {
            const pathEl = document.getElementById('reviewReportPath');
            if (!pathEl) return;
            const reportPath = pathEl.dataset.path || '';
            if (!reportPath) return;
            try {
                if (window.electronAPI && window.electronAPI.openPath) {
                    window.electronAPI.openPath(reportPath);
                }
            } catch(e) { App.toast('Could not open report', 'error'); }
        });

        // Modal buttons
        _btn('profileModalSaveBtn', saveProfile);
        _btn('profileModalCloseBtn', closeModal);
        _btn('profileModalCancelBtn', closeModal);
        // Multi-group input in profile modal
        _btn('pmGroupAddBtn', () => {
            const inp = _$('pmGroup');
            const g = (inp ? inp.value : '').trim();
            if (!g) return;
            if (!_pmGroupsState.includes(g)) { _pmGroupsState.push(g); _renderPmGroupTags(); }
            if (inp) inp.value = '';
        });
        const pmGroupInp = _$('pmGroup');
        if (pmGroupInp) pmGroupInp.addEventListener('keydown', (e) => {
            if (e.key === 'Enter') { e.preventDefault(); document.getElementById('pmGroupAddBtn').click(); }
        });

        _btn('pmCheckProxy', checkProxy);
        _btn('pmParseProxy', parseProxyString);

        // TOTP widget: live update on secret change + copy btn
        const pmTotpInp = _$('pmTotp');
        if (pmTotpInp) {
            pmTotpInp.addEventListener('input', () => _startPmTotp());
            pmTotpInp.addEventListener('paste', () => setTimeout(_startPmTotp, 50));
        }
        _btn('pmTotpCopyBtn', () => {
            const code = (_$('pmTotpCode') || {}).innerText;
            if (code && code !== '------') {
                navigator.clipboard.writeText(code);
                const btn = _$('pmTotpCopyBtn');
                if (btn) { btn.innerHTML = '<i class="fas fa-check"></i>'; setTimeout(() => { btn.innerHTML = '<i class="fas fa-copy"></i>'; }, 1500); }
            }
        });

        // Tab switching
        document.querySelectorAll('.pm-tab').forEach(tab => {
            tab.addEventListener('click', () => _switchTab(tab.dataset.tab));
        });

        // Show mobile OS (Android/iOS supported via fingerprint emulation)
        function _toggleMobileOS() {
            const hideM = false;
            document.querySelectorAll('.pm-os-pills input[name="pmOS"]').forEach(r => {
                const pill = r.closest('.pm-os-pill');
                if (!pill) return;
                if (r.value === 'android' || r.value === 'ios') {
                    pill.style.display = hideM ? 'none' : '';
                    if (hideM && r.checked) {
                        const rand = document.querySelector('input[name="pmOS"][value="random"]');
                        if (rand) { rand.checked = true; rand.closest('.pm-os-pill').classList.add('active'); pill.classList.remove('active'); }
                    }
                }
            });
        }
        // Engine tabs
        document.querySelectorAll('.pm-engine-nav .pm-engine-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                const nav = tab.closest('.pm-engine-nav');
                nav.querySelectorAll('.pm-engine-tab').forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
                const radio = tab.querySelector('input[type="radio"]');
                if (radio) radio.checked = true;
                _toggleMobileOS();
                _updateSummary();
            });
        });
        // OS pills
        document.querySelectorAll('.pm-os-pills .pm-os-pill').forEach(pill => {
            pill.addEventListener('click', () => {
                const pills = pill.closest('.pm-os-pills');
                pills.querySelectorAll('.pm-os-pill').forEach(p => p.classList.remove('active'));
                pill.classList.add('active');
                const radio = pill.querySelector('input[type="radio"]');
                if (radio) radio.checked = true;
                _updateSummary();
            });
        });
        _toggleMobileOS(); // initial state

        // OS radio change -> update summary
        document.querySelectorAll('input[name="pmOS"]').forEach(radio => {
            radio.addEventListener('change', _updateSummary);
        });

        // Proxy type change
        const proxyTypeSel = _$('pmProxyType');
        if (proxyTypeSel) proxyTypeSel.addEventListener('change', () => {
            _toggleProxyFields();
            _updateSummary();
        });

        // Update summary on proxy host change
        ['pmProxyHost', 'pmName', 'pmEmail'].forEach(id => {
            const el = _$(id);
            if (el) el.addEventListener('input', _updateSummary);
        });

        // Batch login modal
        _btn('batchLoginStartBtn', startBatchLogin);
        _btn('batchLoginCloseBtn', closeBatchLoginModal);
        _btn('batchLoginBrowseBtn', async () => {
            await browseFile('batchLoginFilePath');
            _previewBatchFile();
        });
        const blFileInp = _$('batchLoginFilePath');
        if (blFileInp) blFileInp.addEventListener('input', _previewBatchFile);

        // Batch engine tabs + OS pills
        function _toggleBatchMobileOS() {
            const hideM = false;
            document.querySelectorAll('#batchLoginModal .pm-os-pills input[name="batchOs"]').forEach(r => {
                const pill = r.closest('.pm-os-pill');
                if (!pill) return;
                if (r.value === 'android' || r.value === 'ios') {
                    pill.style.display = hideM ? 'none' : '';
                    if (hideM && r.checked) {
                        const rand = document.querySelector('input[name="batchOs"][value="random"]');
                        if (rand) { rand.checked = true; rand.closest('.pm-os-pill').classList.add('active'); pill.classList.remove('active'); }
                    }
                }
            });
        }
        document.querySelectorAll('#batchLoginModal .pm-engine-tab').forEach(tab => {
            tab.addEventListener('click', () => {
                const nav = tab.closest('.pm-engine-nav');
                nav.querySelectorAll('.pm-engine-tab').forEach(t => t.classList.remove('active'));
                tab.classList.add('active');
                const radio = tab.querySelector('input[type="radio"]');
                if (radio) radio.checked = true;
                _toggleBatchMobileOS();
            });
        });
        document.querySelectorAll('#batchLoginModal .pm-os-pill').forEach(pill => {
            pill.addEventListener('click', () => {
                const pills = pill.closest('.pm-os-pills');
                pills.querySelectorAll('.pm-os-pill').forEach(p => p.classList.remove('active'));
                pill.classList.add('active');
                const radio = pill.querySelector('input[type="radio"]');
                if (radio) radio.checked = true;
            });
        });
        _toggleBatchMobileOS();

        // Storage
        _btn('profileSaveStorageBtn', async () => {
            const path = _val('profileStoragePath').trim();
            if (!path) return;
            try {
                const data = await _api('/api/profiles/config', { method: 'POST', body: JSON.stringify({ storage_path: path }) });
                if (data.success) App.toast('Storage path saved', 'success');
            } catch (e) { App.toast('Error', 'error'); }
        });
        _btn('profileSelectStorageBtn', async () => {
            try {
                if (window.electronAPI && window.electronAPI.selectFolder) {
                    const folderPath = await window.electronAPI.selectFolder();
                    if (folderPath) _setVal('profileStoragePath', folderPath);
                }
            } catch (e) { App.toast('Folder picker error', 'error'); }
        });

        // Backup Codes parser — paste all codes at once, auto-fill 10 fields
        const _bcParseBtn = _$('pmBCParseBtn');
        if (_bcParseBtn) {
            _bcParseBtn.addEventListener('click', () => {
                const raw = (_val('pmBCParser') || '').trim();
                if (!raw) return App.toast('Paste your backup codes first', 'warning');
                // Split by newline, comma, tab, or 2+ spaces — then clean each token
                const tokens = raw.split(/[\n\r,\t]+|[ ]{2,}/)
                    .map(t => t.trim().replace(/\s+/g, ' '))
                    .filter(t => t.length > 0);
                // Each code is 8 digits, possibly formatted as "XXXX XXXX"
                // If we got fewer than 10, try splitting further by single space
                let codes = tokens;
                if (codes.length < 10) {
                    // Try pairing consecutive 4-digit groups
                    const allParts = raw.replace(/[,\t\n\r]+/g, ' ').trim().split(/\s+/);
                    if (allParts.length >= 20 && allParts.every(p => /^\d{4}$/.test(p))) {
                        codes = [];
                        for (let i = 0; i < allParts.length - 1; i += 2) {
                            codes.push(allParts[i] + ' ' + allParts[i + 1]);
                        }
                    }
                }
                if (codes.length < 1) return App.toast('Could not parse any codes', 'error');
                const filled = Math.min(codes.length, 10);
                for (let i = 1; i <= 10; i++) {
                    _setVal('pmBC' + i, i <= filled ? codes[i - 1] : '');
                }
                _$('pmBCParser').value = '';
                App.toast(`${filled} backup code${filled > 1 ? 's' : ''} filled`, 'success');
            });
        }

        // Close modals on overlay click
        ['profileModalOverlay', 'batchLoginModalOverlay'].forEach(id => {
            const el = _$(id);
            if (el) el.addEventListener('click', (e) => { if (e.target === el) { if (id === 'profileModalOverlay') closeModal(); else closeBatchLoginModal(); } });
        });

        // Context menu actions
        const ctxMenu = _$('pmContextMenu');
        if (ctxMenu) {
            ctxMenu.querySelectorAll('a[data-action]').forEach(a => {
                a.addEventListener('click', (e) => { e.preventDefault(); _handleContextAction(a.dataset.action); });
            });
        }

        // Load storage config
        (async () => {
            try {
                const data = await _api('/api/profiles/config');
                if (data.success && data.config && data.config.storage_path) {
                    _setVal('profileStoragePath', data.config.storage_path);
                }
            } catch (e) { /* silent */ }
        })();

        // Load groups for dropdowns/datalists
        _loadGroups();
    };

})(window.App || (window.App = {}));
