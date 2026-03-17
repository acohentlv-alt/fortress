/**
 * Login Landing Page — Branded split-screen with value pitch + login form
 *
 * Left panel: Fortress brand, 3-step flow, key differentiators
 * Right panel: Glass card with username/password
 */

import { loginUser } from '../api.js';

export function renderLogin(container, onSuccess) {
    container.innerHTML = `
        <div class="login-landing">
            <!-- ═══════ Left: Brand & Value ═══════ -->
            <div class="login-brand">
                <div class="login-brand-inner">
                    <!-- Logo + Name -->
                    <div class="login-logo">
                        <span class="login-logo-icon">🏰</span>
                        <span class="login-logo-text">Fortress</span>
                    </div>

                    <!-- Headline -->
                    <h1 class="login-headline">
                        Intelligence B2B<br>du marché français
                    </h1>
                    <p class="login-subheadline">
                        Trouvez, enrichissez et exportez des leads B2B qualifiés à partir de la base SIRENE et Google Maps.
                    </p>

                    <!-- 3-Step Flow -->
                    <div class="login-steps">
                        <div class="login-step">
                            <div class="login-step-number">1</div>
                            <div class="login-step-content">
                                <div class="login-step-title">🔍 Recherche</div>
                                <div class="login-step-desc">Ciblez un secteur et une zone géographique. Le moteur parcourt Google Maps pour trouver les entreprises actives.</div>
                            </div>
                        </div>
                        <div class="login-step-connector"></div>
                        <div class="login-step">
                            <div class="login-step-number">2</div>
                            <div class="login-step-content">
                                <div class="login-step-title">⚡ Enrichissement</div>
                                <div class="login-step-desc">Téléphone, email, site web, réseaux sociaux — chaque entreprise est enrichie automatiquement et croisée avec le registre SIRENE.</div>
                            </div>
                        </div>
                        <div class="login-step-connector"></div>
                        <div class="login-step">
                            <div class="login-step-number">3</div>
                            <div class="login-step-content">
                                <div class="login-step-title">📥 Export</div>
                                <div class="login-step-desc">Exportez vos leads en CSV ou XLSX. Données structurées, prêtes pour votre CRM ou vos campagnes commerciales.</div>
                            </div>
                        </div>
                    </div>

                    <!-- Stats -->
                    <div class="login-stats">
                        <div class="login-stat">
                            <div class="login-stat-value">14.7M+</div>
                            <div class="login-stat-label">entreprises indexées</div>
                        </div>
                        <div class="login-stat">
                            <div class="login-stat-value">2</div>
                            <div class="login-stat-label">sources de données</div>
                        </div>
                        <div class="login-stat">
                            <div class="login-stat-value">0€</div>
                            <div class="login-stat-label">coût d'utilisation</div>
                        </div>
                    </div>

                    <div class="login-brand-footer">
                        Fortress v1.0 · Données SIRENE + Google Maps + Crawl
                    </div>
                </div>
            </div>

            <!-- ═══════ Right: Login Form ═══════ -->
            <div class="login-form-panel">
                <div class="login-form-card">
                    <div class="login-form-header">
                        <span class="login-form-icon">🔐</span>
                        <h2 class="login-form-title">Connexion</h2>
                        <p class="login-form-subtitle">Accédez à votre tableau de bord</p>
                    </div>

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
