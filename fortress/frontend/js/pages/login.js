/**
 * Login Page — simple API key authentication
 */

import { loginWithApiKey } from '../api.js';

export function renderLogin(container, onSuccess) {
    container.innerHTML = `
        <div style="display:flex; align-items:center; justify-content:center; min-height:80vh">
            <div class="card" style="width:100%; max-width:400px; text-align:center">
                <div style="font-size:3rem; margin-bottom:var(--space-lg)">🏰</div>
                <h1 style="font-size:var(--font-xl); font-weight:700; margin-bottom:var(--space-xs)">Fortress</h1>
                <p style="color:var(--text-muted); font-size:var(--font-sm); margin-bottom:var(--space-2xl)">
                    Entrez votre clé d'accès pour continuer
                </p>
                <form id="login-form">
                    <input type="password" id="login-key" placeholder="Clé API"
                        autocomplete="current-password"
                        style="width:100%; padding:var(--space-md); background:var(--bg-input);
                               border:1px solid var(--border-default); border-radius:var(--radius-md);
                               color:var(--text-primary); font-size:var(--font-md); margin-bottom:var(--space-lg);
                               box-sizing:border-box">
                    <button type="submit" class="btn btn-primary" style="width:100%">
                        Se connecter
                    </button>
                </form>
                <div id="login-error" style="margin-top:var(--space-md); color:var(--danger); font-size:var(--font-sm); display:none"></div>
            </div>
        </div>
    `;

    document.getElementById('login-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const key = document.getElementById('login-key').value.trim();
        const errorDiv = document.getElementById('login-error');
        if (!key) {
            errorDiv.style.display = 'block';
            errorDiv.textContent = 'Veuillez entrer une clé API.';
            return;
        }

        const btn = e.target.querySelector('button');
        btn.disabled = true;
        btn.textContent = 'Connexion...';

        const result = await loginWithApiKey(key);
        if (result.ok) {
            onSuccess();
        } else {
            errorDiv.style.display = 'block';
            errorDiv.textContent = result.error;
            btn.disabled = false;
            btn.textContent = 'Se connecter';
        }
    });
}
