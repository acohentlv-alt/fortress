/**
 * Login Page — Clean authentication form
 *
 * User arrives here from the Introduction page (#/login).
 * Simple, focused form with Fortress branding.
 */

import { loginUser } from '../api.js';

export function renderLogin(container, onSuccess) {
    container.innerHTML = `
        <div class="login-page">
            <div class="login-form-card">
                <!-- Header -->
                <div class="login-form-header">
                    <div class="login-logo" style="justify-content:center; margin-bottom:var(--space-lg)">
                        <span class="login-logo-icon">🏰</span>
                        <span class="login-logo-text">Fortress</span>
                    </div>
                    <h2 class="login-form-title">Connexion</h2>
                    <p class="login-form-subtitle">Accédez à votre tableau de bord</p>
                </div>

                <!-- Form -->
                <form id="login-form" autocomplete="on">
                    <div class="login-field">
                        <label class="login-label" for="login-username">Nom d'utilisateur</label>
                        <div class="login-input-wrap">
                            <span class="login-input-icon">👤</span>
                            <input type="text" id="login-username"
                                placeholder="Entrez votre identifiant"
                                autocomplete="username" autofocus
                                class="login-input">
                        </div>
                    </div>

                    <div class="login-field">
                        <label class="login-label" for="login-password">Mot de passe</label>
                        <div class="login-input-wrap">
                            <span class="login-input-icon">🔒</span>
                            <input type="password" id="login-password"
                                placeholder="Entrez votre mot de passe"
                                autocomplete="current-password"
                                class="login-input" style="padding-right:44px">
                            <button type="button" id="toggle-password" class="login-toggle-password"
                                title="Afficher le mot de passe"
                                aria-label="Afficher ou masquer le mot de passe">👁️</button>
                        </div>
                    </div>

                    <button type="submit" id="login-submit" class="login-submit-btn">
                        Se connecter →
                    </button>
                </form>

                <div id="login-error" class="login-error" style="display:none">
                    ❌ <span id="login-error-text"></span>
                </div>

                <!-- Back to intro -->
                <div style="text-align:center; margin-top:var(--space-xl)">
                    <a href="#/intro" style="color:var(--text-muted); font-size:var(--font-sm); text-decoration:none">
                        ← Retour à la page d'accueil
                    </a>
                </div>
            </div>
        </div>
    `;

    // ── Show/hide password toggle ────────────────────────────────
    const passwordInput = document.getElementById('login-password');
    const toggleBtn = document.getElementById('toggle-password');
    toggleBtn.addEventListener('click', () => {
        const isPassword = passwordInput.type === 'password';
        passwordInput.type = isPassword ? 'text' : 'password';
        toggleBtn.textContent = isPassword ? '🙈' : '👁️';
        toggleBtn.title = isPassword ? 'Masquer le mot de passe' : 'Afficher le mot de passe';
    });

    // ── Form submission ─────────────────────────────────────────
    document.getElementById('login-form').addEventListener('submit', async (e) => {
        e.preventDefault();
        const username = document.getElementById('login-username').value.trim();
        const password = document.getElementById('login-password').value;
        const errorDiv = document.getElementById('login-error');
        const errorText = document.getElementById('login-error-text');
        const btn = document.getElementById('login-submit');

        if (!username || !password) {
            errorDiv.style.display = 'block';
            errorText.textContent = 'Veuillez remplir tous les champs.';
            return;
        }

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
            btn.textContent = 'Se connecter →';
            const card = btn.closest('.login-form-card');
            card.style.animation = 'shake 0.4s ease';
            setTimeout(() => { card.style.animation = ''; }, 400);
        }
    });
}
