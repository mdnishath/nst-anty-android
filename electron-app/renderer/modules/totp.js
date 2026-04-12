/**
 * totp.js — TOTP generator widget, base32, HMAC-SHA1.
 */
(function (App) {
    'use strict';

    // Base32 decode (RFC 4648)
    function base32Decode(input) {
        const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ234567';
        let cleaned = input.replace(/[\s=-]/g, '').toUpperCase();
        let bits = '';
        for (const ch of cleaned) {
            const val = alphabet.indexOf(ch);
            if (val === -1) continue;
            bits += val.toString(2).padStart(5, '0');
        }
        const bytes = new Uint8Array(Math.floor(bits.length / 8));
        for (let i = 0; i < bytes.length; i++) {
            bytes[i] = parseInt(bits.substr(i * 8, 8), 2);
        }
        return bytes;
    }

    // HMAC-SHA1 via Web Crypto
    async function hmacSHA1(keyBytes, messageBytes) {
        const cryptoKey = await crypto.subtle.importKey(
            'raw', keyBytes, { name: 'HMAC', hash: 'SHA-1' }, false, ['sign']
        );
        const sig = await crypto.subtle.sign('HMAC', cryptoKey, messageBytes);
        return new Uint8Array(sig);
    }

    // Generate TOTP code from secret
    async function generateTOTP(secret) {
        const keyBytes = base32Decode(secret);
        if (keyBytes.length === 0) return null;

        const epoch = Math.floor(Date.now() / 1000);
        const counter = Math.floor(epoch / 30);

        const counterBytes = new Uint8Array(8);
        let tmp = counter;
        for (let i = 7; i >= 0; i--) {
            counterBytes[i] = tmp & 0xff;
            tmp = Math.floor(tmp / 256);
        }

        const hash = await hmacSHA1(keyBytes, counterBytes);
        const offset = hash[hash.length - 1] & 0x0f;
        const code = (
            ((hash[offset] & 0x7f) << 24) |
            ((hash[offset + 1] & 0xff) << 16) |
            ((hash[offset + 2] & 0xff) << 8) |
            (hash[offset + 3] & 0xff)
        ) % 1000000;

        return code.toString().padStart(6, '0');
    }

    // Expose for tools page TOTP tab
    App._generateTOTP = generateTOTP;

    // TOTP Widget setup
    App.setupTOTPWidget = function () {
        const widget = document.getElementById('totpWidget');
        const toggleBtn = document.getElementById('totpToggleBtn');
        const secretInput = document.getElementById('totpSecretInput');
        const codeDisplay = document.getElementById('totpCodeDisplay');
        const copyBtn = document.getElementById('totpCopyBtn');
        const timerBar = document.getElementById('totpTimerBar');

        if (!widget) return;

        let totpInterval = null;
        let currentSecret = '';

        toggleBtn.addEventListener('click', () => {
            widget.classList.toggle('collapsed');
        });

        copyBtn.addEventListener('click', () => {
            const code = codeDisplay.innerText;
            if (code && code !== '------') {
                navigator.clipboard.writeText(code);
                copyBtn.innerHTML = '<i class="fas fa-check"></i>';
                setTimeout(() => { copyBtn.innerHTML = '<i class="fas fa-copy"></i>'; }, 1500);
            }
        });

        async function updateCode() {
            if (!currentSecret) {
                codeDisplay.innerText = '------';
                timerBar.style.width = '0%';
                return;
            }
            try {
                const code = await generateTOTP(currentSecret);
                codeDisplay.innerText = code || 'INVALID';
            } catch {
                codeDisplay.innerText = 'ERROR';
            }
            const remaining = 30 - (Math.floor(Date.now() / 1000) % 30);
            timerBar.style.width = ((remaining / 30) * 100) + '%';
        }

        function startLoop() {
            if (totpInterval) clearInterval(totpInterval);
            updateCode();
            totpInterval = setInterval(updateCode, 1000);
        }

        function stopLoop() {
            if (totpInterval) clearInterval(totpInterval);
            totpInterval = null;
            codeDisplay.innerText = '------';
            timerBar.style.width = '0%';
        }

        secretInput.addEventListener('input', () => {
            const val = secretInput.value.trim();
            if (val.length >= 16) {
                currentSecret = val;
                startLoop();
            } else if (val.length === 0) {
                currentSecret = '';
                stopLoop();
            }
        });

        secretInput.addEventListener('paste', () => {
            setTimeout(() => {
                const val = secretInput.value.trim();
                if (val.length >= 16) {
                    currentSecret = val;
                    startLoop();
                }
            }, 50);
        });
    };

})(window.App || (window.App = {}));
