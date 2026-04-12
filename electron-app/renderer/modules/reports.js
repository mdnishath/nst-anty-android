/**
 * reports.js — Two-section report view: Pro Reports + Raw Output.
 * Each raw output file has its own Generate button to create a Pro report.
 */
(function (App) {
    'use strict';

    function _esc(s) { return App.escapeHtml ? App.escapeHtml(String(s || '')) : String(s || ''); }

    function _formatSize(bytes) {
        return bytes >= 1024 * 1024
            ? (bytes / 1024 / 1024).toFixed(1) + ' MB'
            : (bytes / 1024).toFixed(1) + ' KB';
    }

    function _opTag(name) {
        const n = name.toLowerCase();
        if (n.includes('appeal'))  return { label: 'Appeal',  color: '#f59e0b' };
        if (n.includes('health')) return { label: 'Health',  color: '#22c55e' };
        if (n.includes('reviewurls')) return { label: 'GMB Review', color: '#6366f1' };
        if (n.includes('review') || n.includes('step3')) return { label: 'Review', color: '#f472b6' };
        if (n.includes('step2'))  return { label: 'Step 2',  color: '#a78bfa' };
        if (n.includes('step1'))  return { label: 'Step 1',  color: '#60a5fa' };
        if (n.includes('login') || n.includes('batch')) return { label: 'Login', color: '#34d399' };
        return null;
    }

    // ── Load & Render ───────────────────────────────────────────────────

    App.loadReports = async function () {
        if (!App.state.serverOnline) return;

        const proSection  = document.getElementById('proReportsSection');
        const proList     = document.getElementById('proReportsList');
        const proCount    = document.getElementById('proReportCount');
        const rawSection  = document.getElementById('rawOutputSection');
        const rawList     = document.getElementById('rawOutputList');
        const rawCount    = document.getElementById('rawOutputCount');
        if (!rawList) return;

        rawList.innerHTML = '<div style="color:#64748b;padding:16px 0;">Loading...</div>';
        if (proList) proList.innerHTML = '';

        try {
            const res = await App.apiFetch('/api/reports');
            const data = await res.json();
            if (!data.success) { rawList.innerHTML = '<div style="color:#f87171;">Failed to load.</div>'; return; }

            const reports = data.reports || [];
            const pro = reports.filter(r => r.type === 'mailnexus');
            const raw = reports.filter(r => r.type !== 'mailnexus');

            // ── Pro Reports ─────────────────────────────────────────────
            if (proSection) proSection.style.display = pro.length ? 'block' : 'none';
            if (proCount) proCount.textContent = pro.length;

            if (proList) {
                proList.innerHTML = '';
                pro.forEach(r => proList.appendChild(_buildProItem(r)));
            }

            // ── Raw Output ──────────────────────────────────────────────
            if (rawCount) rawCount.textContent = raw.length;
            if (!raw.length) {
                rawList.innerHTML = '<div style="color:#64748b;font-style:italic;padding:20px 0;">No output files yet.</div>';
                return;
            }

            rawList.innerHTML = '';
            raw.forEach(r => rawList.appendChild(_buildRawItem(r)));

        } catch (e) {
            rawList.innerHTML = '<div style="color:#f87171;">Error loading reports.</div>';
        }
    };

    // ── Build Pro Report Item ────────────────────────────────────────────

    function _buildProItem(report) {
        const date = new Date(report.modified * 1000).toLocaleString();
        const size = _formatSize(report.size);
        const tag = _opTag(report.name);
        const tagHtml = tag
            ? `<span class="rpt-tag" style="color:${tag.color};border-color:${tag.color}33;background:${tag.color}18;">${tag.label}</span>`
            : '';

        const d = document.createElement('div');
        d.className = 'rpt-item rpt-item-pro';
        d.innerHTML = `
            <div class="rpt-item-badges">
                <span class="rpt-badge rpt-badge-report">REPORT</span>
                <span class="rpt-badge rpt-badge-pro">PRO</span>
                ${tagHtml}
            </div>
            <div class="rpt-item-name">${_esc(report.name)}</div>
            <div class="rpt-item-meta">${size} | ${date} | MailNexus Pro</div>
            <div class="rpt-item-actions">
                <button class="rpt-act-btn rpt-open-btn"><i class="fas fa-external-link-alt"></i> Open</button>
                <button class="rpt-act-btn rpt-del-btn"><i class="fas fa-trash"></i></button>
            </div>`;

        d.querySelector('.rpt-open-btn').onclick = () => _openFile(report.path);
        d.querySelector('.rpt-del-btn').onclick = () => _deleteReport(report.path, report.name);
        return d;
    }

    // ── Build Raw Output Item ────────────────────────────────────────────

    function _buildRawItem(report) {
        const date = new Date(report.modified * 1000).toLocaleString();
        const size = _formatSize(report.size);
        const tag = _opTag(report.name);
        const tagHtml = tag
            ? `<span class="rpt-tag" style="color:${tag.color};border-color:${tag.color}33;background:${tag.color}18;">${tag.label}</span>`
            : '';

        const d = document.createElement('div');
        d.className = 'rpt-item';
        d.innerHTML = `
            <div class="rpt-item-left">
                <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
                    <span class="rpt-badge rpt-badge-raw">RAW</span>
                    ${tagHtml}
                    <span class="rpt-item-name">${_esc(report.name)}</span>
                </div>
                <div class="rpt-item-meta">${size} | ${date} | Output Data</div>
            </div>
            <div class="rpt-item-actions">
                <button class="rpt-act-btn rpt-gen-btn" title="Generate Pro report from this file"><i class="fas fa-file-export"></i> Generate</button>
                <button class="rpt-act-btn rpt-open-btn"><i class="fas fa-external-link-alt"></i> Open</button>
                <button class="rpt-act-btn rpt-del-btn"><i class="fas fa-trash"></i></button>
            </div>`;

        d.querySelector('.rpt-gen-btn').onclick = (e) => _generateFromFile(report.path, e.currentTarget);
        d.querySelector('.rpt-open-btn').onclick = () => _openFile(report.path);
        d.querySelector('.rpt-del-btn').onclick = () => _deleteReport(report.path, report.name);
        return d;
    }

    // ── Actions ──────────────────────────────────────────────────────────

    function _openFile(path) {
        if (window.electronAPI && window.electronAPI.openPath) window.electronAPI.openPath(path);
    }

    async function _deleteReport(path, name) {
        if (!confirm(`Delete "${name}"?`)) return;
        try {
            const res = await App.apiFetch('/api/reports/single', {
                method: 'DELETE', body: JSON.stringify({ path })
            });
            const data = await res.json();
            if (data.success) { App.toast('Deleted', 'success'); App.loadReports(); }
            else App.toast('Delete failed', 'error');
        } catch (e) { App.toast('Delete error', 'error'); }
    }

    async function _generateFromFile(filePath, btn) {
        if (!App.state.serverOnline) { App.toast('Server offline', 'error'); return; }
        const orig = btn.innerHTML;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
        btn.disabled = true;

        try {
            const res = await App.apiFetch('/api/reports/generate', {
                method: 'POST',
                body: JSON.stringify({ source: 'file', file_path: filePath })
            });
            const data = await res.json();
            if (data.success) {
                btn.innerHTML = '<i class="fas fa-check" style="color:#4ade80;"></i> Done';
                App.toast(data.message || 'Report generated', 'success');
                App.loadReports();
            } else {
                App.toast(data.message || 'Generation failed', 'error');
                btn.innerHTML = orig;
            }
        } catch (e) {
            App.toast('Error generating report', 'error');
            btn.innerHTML = orig;
        }
        btn.disabled = false;
        setTimeout(() => { btn.innerHTML = orig; }, 3000);
    }

    // ── Setup ────────────────────────────────────────────────────────────

    App.setupGenerateReport = function () {
        const refreshBtn = document.getElementById('refreshReportsBtn');
        if (refreshBtn) refreshBtn.addEventListener('click', () => App.loadReports());
    };

    App.setupDeleteAllReports = function () {
        const btn = document.getElementById('deleteAllReportsBtn');
        if (!btn) return;
        btn.addEventListener('click', async () => {
            if (!confirm('Delete ALL reports? This cannot be undone.')) return;
            btn.disabled = true; btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i>';
            try {
                const res = await App.apiFetch('/api/reports/all', { method: 'DELETE' });
                const data = await res.json();
                if (data.success) { App.toast(`Deleted ${data.deleted} file(s)`, 'success'); App.loadReports(); }
                else App.toast('Delete failed', 'error');
            } catch (e) { App.toast('Error', 'error'); }
            btn.disabled = false; btn.innerHTML = '<i class="fas fa-trash-alt"></i> Delete All';
        });
    };

    App.setupGenerateTemplate = function () {
        const btn = document.getElementById('generateTemplateBtn');
        if (!btn) return;
        btn.addEventListener('click', async () => {
            if (!App.state.serverOnline) return alert('Server not running.');
            const step = document.getElementById('templateStepSelect')?.value;
            if (!step) return;
            const orig = btn.innerHTML;
            btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i>'; btn.disabled = true;
            try {
                const res = await App.apiFetch('/api/template/generate', {
                    method: 'POST', body: JSON.stringify({ step: parseInt(step) })
                });
                const data = await res.json();
                if (data.success) {
                    App.toast('Template generated', 'success');
                    if (window.electronAPI?.openPath) window.electronAPI.openPath(data.template_path);
                } else App.toast('Failed: ' + data.message, 'error');
            } catch (e) { App.toast('Error', 'error'); }
            btn.disabled = false; btn.innerHTML = orig;
        });
    };

})(window.App || (window.App = {}));
