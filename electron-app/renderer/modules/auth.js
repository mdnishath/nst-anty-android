/**
 * auth.js — API fetch wrapper + backend boot detection.
 */
(function (App) {
    'use strict';

    App.state = App.state || {};
    App.state.apiToken = 'local';
    App.state.authenticated = false;

    const API_BASE = 'http://localhost:5000';

    App.apiFetch = function (url, options) {
        options = options || {};
        if (url.startsWith('/')) url = API_BASE + url;
        options.headers = options.headers || {};
        if (!options.headers['Content-Type'] && options.method !== 'GET') {
            options.headers['Content-Type'] = 'application/json';
        }
        return fetch(url, options);
    };

    function _setSplashStatus(msg) {
        const el = document.getElementById('splashStatus');
        if (el) el.textContent = msg;
    }

    function _hideSplash() {
        const splash = document.getElementById('launchSplash');
        if (!splash) return;
        splash.style.opacity = '0';
        setTimeout(() => { if (splash.parentNode) splash.parentNode.removeChild(splash); }, 520);
    }

    function unlockApp() {
        if (App.state.authenticated) return;
        App.state.authenticated = true;
        const overlay = document.getElementById('loginOverlay');
        if (overlay) overlay.style.display = 'none';
        _setSplashStatus('Ready!');
        setTimeout(_hideSplash, 300);
        if (typeof App.onAuthenticated === 'function') App.onAuthenticated();
    }

    let _attempt = 0;
    function waitForBackend() {
        _attempt++;
        if (_attempt <= 3)       _setSplashStatus('Starting backend…');
        else if (_attempt <= 8)  _setSplashStatus('Loading modules…');
        else if (_attempt <= 15) _setSplashStatus('Almost ready…');
        else                     _setSplashStatus('Still loading…');

        fetch(API_BASE + '/api/health')
            .then(r => { if (r.ok) unlockApp(); else setTimeout(waitForBackend, 1000); })
            .catch(() => setTimeout(waitForBackend, 1000));
    }

    if (window.electronAPI && typeof window.electronAPI.onBackendReady === 'function') {
        window.electronAPI.onBackendReady(() => unlockApp());
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', () => setTimeout(waitForBackend, 200));
    } else {
        setTimeout(waitForBackend, 200);
    }

})(window.App || (window.App = {}));
