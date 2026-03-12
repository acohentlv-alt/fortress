/**
 * Login Page — username + password session authentication.
 *
 * The backend sets an HttpOnly cookie (fortress_session) — no token
 * management needed on the JS side.
 */

import { loginUser } from '../api.js';

export function renderLogin(container, onSuccess) {
    container.innerHTML = `
        <div style="display:flex; align-items:center; justify-content:center; min-height:100vh; padding:var(--space-xl)">
            <div class="card" style="width:100%; max-width:420px; text-align:center; padding:var(--space-2xl) var(--space-xl)">
                <div style="font-size:3.5rem; margin-bottom:var(--space-md)">🏰</div>
                <h1 style="font-size:var(--font-xl); font-weight:700; margin-bottom:var(--space-xs)">Fortress</h1>
                <p style="color:var(--text-muted); font-size:var(--font-sm); margin-bottom:var(--space-2xl)">
                    Connectez-vous pour accéder au tableau de bord
                </p>
                <form id="login-form" autocomplete="on">
                    <div style="margin-bottom:var(--space-lg)">
                        <input type="text" id="login-username" placeholder="Nom d'utilisateur"
                            autocomplete="username" autofocus
                            style="width:100%; padding:var(--space-md); background:var(--bg-input);
                                   border:1px solid var(--border-default); border-radius:var(--radius-md);
                                   color:var(--text-primary); font-size:var(--font-md);
                                   box-sizing:border-box; transition:border-color 0.2s"
                            onfocus="this.style.borderColor='var(--accent)'"
                            onblur="this.style.borderColor='var(--border-default)'">
                    </div>
                    <div style="margin-bottom:var(--space-xl); position:relative">
                        <input type="password" id="login-password" placeholder="Mot de passe"
                            autocomplete="current-password"
                            style="width:100%; padding:var(--space-md); padding-right:44px; background:var(--bg-input);
                                   border:1px solid var(--border-default); border-radius:var(--radius-md);
                                   color:var(--text-primary); font-size:var(--font-md);
                                   box-sizing:border-box; transition:border-color 0.2s"
                            onfocus="this.style.borderColor='var(--accent)'"
                            onblur="this.style.borderColor='var(--border-default)'">
                        <button type="button" id="toggle-password"
                            style="position:absolute; right:8px; top:50%; transform:translateY(-50%);
                                   background:none; border:none; cursor:pointer; font-size:16px;
                                   color:var(--text-muted); padding:4px 6px; border-radius:var(--radius-sm)"
                            title="Afficher le mot de passe"
                            aria-label="Afficher ou masquer le mot de passe">👁️</button>
                    </div>
                    <button type="submit" id="login-submit" class="btn btn-primary" style="width:100%; padding:var(--space-md); font-size:var(--font-md)">
                        Se connecter
                    </button>
                </form>
                <div id="login-error" style="margin-top:var(--space-lg); color:var(--danger); font-size:var(--font-sm); display:none">
                    ❌ <span id="login-error-text"></span>
                </div>
            </div>
        </div>
    `;

    // Show/hide password toggle
    const passwordInput = document.getElementById('login-password');
    const toggleBtn = document.getElementById('toggle-password');
    toggleBtn.addEventListener('click', () => {
        const isPassword = passwordInput.type === 'password';
        passwordInput.type = isPassword ? 'text' : 'password';
        toggleBtn.textContent = isPassword ? '🙈' : '👁️';
        toggleBtn.title = isPassword ? 'Masquer le mot de passe' : 'Afficher le mot de passe';
    });

    // Form submission
    document.getElementById('login-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const username = document.getElementById('login-username').value.trim();
        const password = document.getElementById('login-password').value;
        const errorDiv = document.getElementById('login-error');
        const errorText = document.getElementById('login-error-text');
        const btn = document.getElementById('login-submit');

        // Validate inputs
        if (!username || !password) {
            errorDiv.style.display = 'block';
            errorText.textContent = 'Veuillez remplir tous les champs.';
            return;
        }

        // Show loading state
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner" style="width:16px;height:16px;border-width:2px;display:inline-block;vertical-align:middle;margin-right:8px"></span> Connexion…';
        errorDiv.style.display = 'none';

        const result = await loginUser(username, password);

        if (result.ok) {
            onSuccess(result.user);
        } else {
            errorDiv.style.display = 'block';
            errorText.textContent = result.error;
            btn.disabled = false;
            btn.textContent = 'Se connecter';
            // Shake animation
            const card = btn.closest('.card');
            card.style.animation = 'shake 0.4s ease';
            setTimeout(() => { card.style.animation = ''; }, 400);
        }
    });
}
