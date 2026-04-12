const { contextBridge, ipcRenderer } = require('electron');

// Expose protected methods to renderer
contextBridge.exposeInMainWorld('electronAPI', {
    selectFile: () => ipcRenderer.invoke('select-file'),
    selectFolder: () => ipcRenderer.invoke('select-folder'),
    startBackend: () => ipcRenderer.invoke('start-backend'),
    stopBackend: () => ipcRenderer.invoke('stop-backend'),
    getAppPath: () => ipcRenderer.invoke('get-app-path'),
    openPath: (path) => ipcRenderer.invoke('open-path', path),
    getApiToken: () => ipcRenderer.invoke('get-api-token'),
    onApiToken: (callback) => ipcRenderer.on('api-token', (_event, token) => callback(token)),
    onBackendReady: (callback) => ipcRenderer.on('backend-ready', (_event) => callback()),
    onBackendLog: (callback) => ipcRenderer.on('backend-log', (_event, msg) => callback(msg)),
});
