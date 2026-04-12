const { app, BrowserWindow, ipcMain, dialog } = require('electron');
const path = require('path');
const fs = require('fs');
const { spawn, execSync } = require('child_process');

let mainWindow;
let pythonProcess;
let isQuitting = false;
let apiToken = null;  // Captured from backend stdout [AUTH_TOKEN]

// Log backend output to file for debugging
const backendLogPath = path.join(app.getPath('userData'), 'backend.log');
function logBackend(msg) {
    const ts = new Date().toISOString();
    try { fs.appendFileSync(backendLogPath, `[${ts}] ${msg}\n`); } catch(e) {}
    console.log(msg);
}

// ─────────────────────────────────────────────────────────────────────────────
// Force-kill backend process tree (Windows: taskkill /T /F kills all children)
// ─────────────────────────────────────────────────────────────────────────────

function _safePid(pid) {
    const n = parseInt(pid, 10);
    return (Number.isInteger(n) && n > 0) ? n : null;
}

function forceKillBackend() {
    if (!pythonProcess) return;
    const pid = _safePid(pythonProcess.pid);
    if (!pid) { pythonProcess = null; return; }
    console.log(`[Backend] Force-killing backend process tree (PID ${pid})...`);

    // PowerShell Stop-Process — most reliable on Windows for process tree kill
    try {
        execSync(
            `powershell -Command "Stop-Process -Id ${pid} -Force -ErrorAction SilentlyContinue"`,
            { timeout: 5000 }
        );
        console.log(`[Backend] Killed PID ${pid}`);
    } catch(e) {
        // Process may already be dead — that's fine
    }

    pythonProcess = null;
}

// ─────────────────────────────────────────────────────────────────────────────
// Kill any zombie Python server on port 5000 (Windows-safe)
// ─────────────────────────────────────────────────────────────────────────────

function killZombieServers() {
    // Single fast netstat-based kill — no PowerShell overhead, no sleep
    try {
        const out = execSync(
            'netstat -ano | findstr ":5000.*LISTENING"',
            { encoding: 'utf8', timeout: 3000 }
        ).trim();
        if (out) {
            const pids = new Set();
            for (const line of out.split('\n')) {
                const pid = _safePid(line.trim().split(/\s+/).pop());
                if (pid && pid > 4) pids.add(pid);  // skip System PIDs
            }
            for (const pid of pids) {
                console.log(`[Backend] Killing zombie on port 5000: PID ${pid}`);
                try { execSync(`taskkill /PID ${pid} /F`, { timeout: 3000 }); } catch(e) {}
            }
        }
    } catch(e) {
        // Port is free — good
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Backend process management (shared by auto-start and IPC handler)
// ─────────────────────────────────────────────────────────────────────────────

function startBackendProcess() {
    return new Promise((resolve, reject) => {
        // Already running — nothing to do
        if (pythonProcess) {
            resolve({ success: true, port: 5000 });
            return;
        }

        // Kill any orphaned Python servers from a previous Electron session
        killZombieServers();

        // Port should be free after zombie kill — no extra check needed

        const isDev = !app.isPackaged;

        // Playwright browsers stored in user's AppData (persists between runs)
        const playwrightBrowsersPath = path.join(app.getPath('userData'), 'playwright');

        if (!isDev) {
            // PRODUCTION: spawn nexus-anty-engine.exe (PyInstaller bundle)
            const backendExePath = path.join(process.resourcesPath, 'backend', 'nexus-anty-engine.exe');

            console.log(`[Backend] PRODUCTION mode — nexus-anty-engine.exe`);
            console.log(`[Backend] Path: ${backendExePath}`);
            console.log(`[Backend] RESOURCES_PATH: ${process.resourcesPath}`);
            console.log(`[Backend] PLAYWRIGHT_BROWSERS_PATH: ${playwrightBrowsersPath}`);

            pythonProcess = spawn(backendExePath, [], {
                env: {
                    ...process.env,
                    RESOURCES_PATH: process.resourcesPath,
                    PLAYWRIGHT_BROWSERS_PATH: playwrightBrowsersPath,
                    PYTHONUTF8: '1',  // UTF-8 stdout/stderr on Windows pipes
                }
            });

        } else {
            // DEVELOPMENT: spawn system Python + server.py
            const backendServerPath = path.join(__dirname, 'backend', 'server.py');
            const botRootPath       = path.join(__dirname, '..');

            console.log(`[Backend] DEV mode`);
            console.log(`[Backend] server.py: ${backendServerPath}`);
            console.log(`[Backend] botRootPath: ${botRootPath}`);

            pythonProcess = spawn('python', [backendServerPath], {
                env: {
                    ...process.env,
                    RESOURCES_PATH: botRootPath,
                    PYTHONUTF8: '1',  // UTF-8 stdout/stderr on Windows pipes
                }
            });
        }

        pythonProcess.stdout.on('data', (data) => {
            const text = data.toString();
            logBackend(`[stdout] ${text.trim()}`);

            // Capture API auth token from backend
            const tokenMatch = text.match(/\[AUTH_TOKEN\]\s+(\S+)/);
            if (tokenMatch) {
                apiToken = tokenMatch[1];
                console.log('[Backend] API token captured');
                if (mainWindow && !mainWindow.isDestroyed()) {
                    mainWindow.webContents.send('api-token', apiToken);
                }
            }

            // Forward to renderer for live visibility
            if (mainWindow && !mainWindow.isDestroyed()) {
                mainWindow.webContents.send('backend-log', text.trim());
            }
            if (text.includes('Server started')) {
                resolve({ success: true, port: 5000 });
            }
        });

        pythonProcess.stderr.on('data', (data) => {
            const text = data.toString().trim();
            logBackend(`[stderr] ${text}`);

            // Filter out Flask/Werkzeug noise — don't forward to UI:
            //   - HTTP access logs:  127.0.0.1 - - [...] "GET /api/... HTTP/1.1" 200 -
            //   - Startup banner:    * Serving Flask app ...  /  * Running on ...
            if (/^\d+\.\d+\.\d+\.\d+\s+-\s+-\s+\[/.test(text)) return;
            if (/^\* (Serving|Running|Debugger|Restarting)/.test(text)) return;
            // Filter Node.js/Electron deprecation warnings (harmless, not from our code)
            if (/DeprecationWarning:|DEP\d{4}|Use `node --trace-deprecation/.test(text)) return;

            // Forward genuine errors to renderer
            if (mainWindow && !mainWindow.isDestroyed()) {
                mainWindow.webContents.send('backend-log', `[ERR] ${text}`);
            }
        });

        // Keep reference to THIS process so the exit handler doesn't
        // accidentally null-out a NEW process started after a restart.
        const thisProcess = pythonProcess;

        thisProcess.on('error', (error) => {
            if (pythonProcess === thisProcess) pythonProcess = null;
            reject({ success: false, error: error.message });
        });

        thisProcess.on('exit', (code, signal) => {
            if (signal === 'SIGTERM' || code === 0) {
                console.log('[Backend] Server stopped.');
            } else {
                console.log(`[Backend] Process exited — code=${code}, signal=${signal}`);
            }
            if (pythonProcess === thisProcess) pythonProcess = null;
        });

        // Timeout fallback — resolves UI even during first-run Chromium install
        // (first run may take 1-2 min; 120s gives enough headroom)
        const timeoutMs = isDev ? 5000 : 120000;
        setTimeout(() => {
            resolve({ success: true, port: 5000 });
        }, timeoutMs);
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// Window
// ─────────────────────────────────────────────────────────────────────────────

function createWindow() {
    mainWindow = new BrowserWindow({
        width: 1400,
        height: 900,
        minWidth: 1200,
        minHeight: 800,
        webPreferences: {
            nodeIntegration: false,
            contextIsolation: true,
            preload: path.join(__dirname, 'preload.js')
        },
        frame: true,
        backgroundColor: '#0f1117',  // match app dark bg — no white flash
        show: false,                  // hidden until ready-to-show fires
        icon: path.join(__dirname, 'assets/icon.png')
    });

    // Show window only when fully rendered — eliminates white flash completely
    mainWindow.once('ready-to-show', () => {
        mainWindow.show();
    });

    mainWindow.loadFile('renderer/index.html');

    // Open DevTools in development (disabled for production)
    // mainWindow.webContents.openDevTools();

    // ── Pre-install Playwright Chromium (so nexus-anty-engine.exe doesn't block) ────
    const playwrightBrowsersPath = path.join(app.getPath('userData'), 'playwright');
    logBackend(`[App] Backend log file: ${backendLogPath}`);
    logBackend(`[App] Playwright browsers path: ${playwrightBrowsersPath}`);

    // ── Auto-start backend as soon as the app opens ──────────────────────
    startBackendProcess()
        .then(async () => {
            console.log('[Backend] Auto-start complete, waiting for health endpoint...');
            // Poll until Flask actually responds — fast 500ms interval
            const http = require('http');
            for (let i = 0; i < 120; i++) {
                try {
                    const ok = await new Promise((resolve) => {
                        const req = http.get('http://127.0.0.1:5000/api/health', { timeout: 1000 }, (res) => {
                            resolve(res.statusCode === 200);
                        });
                        req.on('error', () => resolve(false));
                        req.on('timeout', () => { req.destroy(); resolve(false); });
                    });
                    if (ok) {
                        console.log('[Backend] Health check passed — notifying renderer');
                        if (mainWindow && !mainWindow.isDestroyed()) {
                            mainWindow.webContents.send('backend-ready');
                        }
                        return;
                    }
                } catch(e) {}
                await new Promise(r => setTimeout(r, 500));
            }
            console.log('[Backend] Health check never passed after 60s');
        })
        .catch(err => console.error('[Backend] Auto-start failed:', err));

    mainWindow.on('closed', () => {
        mainWindow = null;
    });
}

app.whenReady().then(createWindow);

// ── App lifecycle: ensure backend dies when Electron exits ──────────────────

app.on('before-quit', () => {
    isQuitting = true;
    // Close all running browser profiles BEFORE killing the backend.
    // Uses synchronous HTTP so browsers are fully terminated before backend dies.
    // NOTE: Do NOT kill chrome.exe globally — it kills the user's own Chrome browser.
    // Only close browsers tracked by our profile manager.
    console.log('[App] before-quit → closing all managed browser profiles...');
    try {
        const headers = apiToken ? `-Headers @{Authorization="Bearer ${apiToken}"}` : '';
        execSync(
            `powershell -Command "try { Invoke-WebRequest -Uri http://127.0.0.1:5000/api/profiles/close-all -Method POST ${headers} -TimeoutSec 10 | Out-Null } catch {}"`,
            { timeout: 15000 }
        );
        console.log('[App] before-quit → all profile browsers closed');
    } catch(e) {
        console.log('[App] before-quit → close-all timed out or backend already dead');
    }
    console.log('[App] before-quit → killing backend...');
    forceKillBackend();
});

app.on('will-quit', (event) => {
    // Double-check: if backend is STILL alive, kill again
    if (pythonProcess) {
        console.log('[App] will-quit → backend still alive, force-killing again...');
        forceKillBackend();
    }
    // In production: kill any nexus-anty-engine.exe by name (covers zombies, children)
    if (app.isPackaged) {
        try {
            execSync('taskkill /IM nexus-anty-engine.exe /F /T', { timeout: 5000 });
        } catch(e) { /* no nexus-anty-engine.exe running — fine */ }
    }
    // Kill any zombie on port 5000 (covers edge cases)
    killZombieServers();
});

app.on('window-all-closed', () => {
    if (process.platform !== 'darwin') {
        app.quit();
    }
});

app.on('activate', () => {
    if (mainWindow === null) {
        createWindow();
    }
});

// ── Safety net: if Node process itself exits unexpectedly ───────────────────
process.on('exit', () => {
    if (pythonProcess) {
        const pid = _safePid(pythonProcess.pid);
        if (pid) {
            try {
                execSync(`powershell -Command "Stop-Process -Id ${pid} -Force -ErrorAction SilentlyContinue"`, { timeout: 3000 });
            } catch(e) {}
        }
    }
    // In production: kill any nexus-anty-engine.exe by name
    if (app.isPackaged) {
        try {
            execSync('taskkill /IM nexus-anty-engine.exe /F /T', { timeout: 3000 });
        } catch(e) {}
    }
});

// ─────────────────────────────────────────────────────────────────────────────
// IPC Handlers
// ─────────────────────────────────────────────────────────────────────────────

// Select Excel File
ipcMain.handle('select-file', async () => {
    const result = await dialog.showOpenDialog(mainWindow, {
        properties: ['openFile'],
        filters: [
            { name: 'Excel Files', extensions: ['xlsx', 'xls'] }
        ]
    });

    if (!result.canceled) {
        return result.filePaths[0];
    }
    return null;
});

// Select Folder (for Profile Manager storage path)
ipcMain.handle('select-folder', async () => {
    const result = await dialog.showOpenDialog(mainWindow, {
        properties: ['openDirectory'],
        title: 'Select Profile Storage Folder',
    });
    if (!result.canceled) {
        return result.filePaths[0];
    }
    return null;
});

// Start Python Backend Server (reuses shared function — no-op if already running)
ipcMain.handle('start-backend', async () => {
    return startBackendProcess();
});

// Stop Python Backend
ipcMain.handle('stop-backend', async () => {
    if (pythonProcess) {
        const proc = pythonProcess;
        const pid = proc.pid;

        // Step 1: Try graceful Flask /api/shutdown (calls os._exit(0))
        // Wait 3s for it to die on its own
        await new Promise(resolve => {
            const fallback = setTimeout(() => {
                // Step 2: Force kill entire process tree
                console.log(`[Backend] Graceful shutdown timed out → force-killing PID ${pid}...`);
                forceKillBackend();
                resolve();
            }, 3000);

            proc.once('exit', () => {
                clearTimeout(fallback);
                if (pythonProcess === proc) pythonProcess = null;
                resolve();
            });
        });
        // Extra wait for port release
        await new Promise(resolve => setTimeout(resolve, 500));
    }
    return { success: true };
});

// Get app path
ipcMain.handle('get-app-path', async () => {
    return app.getAppPath();
});

// Get API auth token (renderer may request after boot)
ipcMain.handle('get-api-token', async () => {
    return apiToken;
});

// Open file or folder in default application
ipcMain.handle('open-path', async (event, filePath) => {
    const { shell } = require('electron');
    try {
        // Security: only allow opening files inside the project or user output dirs
        const resolved = path.resolve(filePath);
        const appRoot = path.resolve(__dirname, '..');
        const userHome = require('os').homedir();
        const appData = process.env.LOCALAPPDATA || path.join(userHome, 'AppData', 'Local');
        const appDataRoaming = process.env.APPDATA || path.join(userHome, 'AppData', 'Roaming');
        const allowedRoots = [
            appRoot,
            path.join(userHome, 'output'),
            path.join(userHome, 'OneDrive'),
            path.join(appData, 'GmailBotPro'),
            path.join(appData, 'MailNexusPro'),
            path.join(appDataRoaming, 'MailNexusPro'),
        ];

        const isAllowed = allowedRoots.some(root => resolved.startsWith(root));
        if (!isAllowed) {
            console.warn(`[Security] open-path blocked: ${resolved}`);
            return { success: false, error: 'Path not in an allowed directory' };
        }

        await shell.openPath(resolved);
        return { success: true };
    } catch (error) {
        console.error('Error opening path:', error);
        return { success: false, error: error.message };
    }
});
