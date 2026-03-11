/**
 * New Batch Page — Launch form for new scrape jobs
 *
 * Provides a structured form for operators to configure
 * and launch a new data collection batch directly from the UI.
 */

import { escapeHtml } from '../components.js';
import { runBatch, extractApiError } from '../api.js';
import { DEPARTMENTS } from '../constants.js';

export async function renderNewBatch(container) {
    container.innerHTML = `
        <h1 class="page-title">🚀 Nouveau Batch</h1>
        <p class="page-subtitle">Configurer et lancer une nouvelle collecte de données</p>

        <div class="batch-form">
            <!-- Sector / Job Name -->
            <div class="form-group">
                <label class="form-label" for="batch-sector">Secteur d'activité</label>
                <input type="text" id="batch-sector" class="form-input"
                    placeholder="ex: agriculture, transport, boulangerie..."
                    autocomplete="off">
                <div class="form-hint">Le nom du secteur sera utilisé pour organiser les données</div>
            </div>

            <!-- Location -->
            <div class="form-row">
                <div class="form-group">
                    <label class="form-label" for="batch-dept">Département</label>
                    <select id="batch-dept" class="form-select">
                        <option value="">Sélectionner...</option>
                        ${DEPARTMENTS.map(([code, name]) =>
        `<option value="${code}">${code} — ${escapeHtml(name)}</option>`
    ).join('')}
                    </select>
                </div>
                <div class="form-group">
                    <label class="form-label" for="batch-city">Ville (optionnel)</label>
                    <input type="text" id="batch-city" class="form-input"
                        placeholder="ex: Toulouse, Perpignan..."
                        autocomplete="off">
                    <div class="form-hint">Affine la recherche à une ville spécifique</div>
                </div>
            </div>

            <!-- Batch Size -->
            <div class="form-group">
                <label class="form-label" for="batch-size">Nombre d'entités à collecter</label>
                <input type="number" id="batch-size" class="form-input"
                    value="20" min="5" max="500" step="5">
                <div class="form-hint">Recommandé : 20 par batch pour un taux de succès optimal</div>
            </div>

            <!-- NAF Code (optional) -->
            <div class="form-group">
                <label class="form-label" for="batch-naf">Code NAF précis (optionnel)</label>
                <input type="text" id="batch-naf" class="form-input"
                    placeholder="ex: 49.41A, 52.29A..."
                    autocomplete="off" style="max-width:200px">
                <div class="form-hint">Laissez vide pour chercher tout le secteur. Format : XX.XXX (ex: 49.41A)</div>
            </div>

            <!-- Agents — reflects actual 2-step pipeline -->
            <div class="form-group">
                <label class="form-label">Pipeline d'enrichissement</label>
                <div style="display:flex; flex-direction:column; gap:var(--space-sm); margin-top:var(--space-sm)">
                    <label style="display:flex; align-items:center; gap:var(--space-md); cursor:pointer; padding:var(--space-sm); border-radius:var(--radius-sm); transition:background var(--transition-fast)"
                        onmouseover="this.style.background='var(--bg-hover)'" onmouseout="this.style.background='transparent'">
                        <input type="checkbox" id="agent-maps" checked disabled style="accent-color:var(--accent); width:16px; height:16px">
                        <span style="font-weight:600; color:var(--text-primary)">🗺️ Google Maps</span>
                        <span style="font-size:var(--font-xs); color:var(--text-muted)">— Téléphone, adresse vérifiée, avis, site web</span>
                    </label>
                    <label style="display:flex; align-items:center; gap:var(--space-md); cursor:pointer; padding:var(--space-sm); border-radius:var(--radius-sm); transition:background var(--transition-fast)"
                        onmouseover="this.style.background='var(--bg-hover)'" onmouseout="this.style.background='transparent'">
                        <input type="checkbox" id="agent-crawl" checked disabled style="accent-color:var(--accent); width:16px; height:16px">
                        <span style="font-weight:600; color:var(--text-primary)">🌐 Website Crawl</span>
                        <span style="font-size:var(--font-xs); color:var(--text-muted)">— Emails, réseaux sociaux (LinkedIn, Facebook…)</span>
                    </label>
                </div>
                <div class="form-hint" style="margin-top:var(--space-sm)">Pipeline fixe : Maps recherche l'entreprise, puis crawl du site web pour compléter</div>
            </div>

            <!-- Mode -->
            <div class="form-group">
                <label class="form-label" for="batch-mode">Mode de collecte</label>
                <select id="batch-mode" class="form-select">
                    <option value="discovery">🔍 Découverte — Trouver de nouvelles entités</option>
                    <option value="enrichment">🔬 Enrichissement — Compléter les existantes</option>
                </select>
            </div>

            <!-- Summary Preview — Clean SaaS card, NOT a terminal -->
            <div id="batch-summary" style="background:var(--bg-secondary); border:1px solid var(--accent-subtle); border-left:3px solid var(--accent); border-radius:var(--radius); padding:var(--space-xl); margin-top:var(--space-xl)">
                <div style="font-size:var(--font-xs); font-weight:700; color:var(--accent-hover); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-md)">
                    📋 Aperçu du batch
                </div>
                <div id="batch-summary-content" style="font-size:var(--font-sm); color:var(--text-secondary); line-height:1.6">
                    Configurez les paramètres ci-dessus pour voir un aperçu du batch
                </div>
            </div>

            <!-- Actions -->
            <div class="form-actions">
                <button class="btn btn-primary" id="btn-launch-batch" style="padding:var(--space-md) var(--space-2xl)">
                    🚀 Lancer le Batch
                </button>
                <button class="btn btn-secondary" onclick="window.location.hash='#/'">
                    Annuler
                </button>
            </div>

            <!-- Info note -->
            <div style="margin-top:var(--space-xl); padding:var(--space-lg); background:var(--info-subtle); border-radius:var(--radius-sm); font-size:var(--font-sm); color:var(--info)">
                ⓘ Tout est piloté par API — aucun terminal nécessaire. Le batch sera lancé
                automatiquement et vous serez redirigé vers Pipeline Live.
            </div>
        </div>
    `;

    // Live summary update
    const updateSummary = () => {
        const sector = document.getElementById('batch-sector').value.trim();
        const dept = document.getElementById('batch-dept').value;
        const city = document.getElementById('batch-city').value.trim();
        const size = document.getElementById('batch-size').value;
        const mode = document.getElementById('batch-mode').value;
        const nafCode = document.getElementById('batch-naf').value.trim();

        if (!sector || !dept) {
            document.getElementById('batch-summary-content').innerHTML =
                'Configurez les paramètres ci-dessus pour voir un aperçu du batch';
            return;
        }

        const location = city ? `${city} (${dept})` : `département ${dept}`;
        const modeLabel = mode === 'discovery' ? 'Découverte' : 'Enrichissement';

        document.getElementById('batch-summary-content').innerHTML = `
            <strong>${escapeHtml(sector.toUpperCase())}</strong> — ${escapeHtml(location)}<br>
            Mode : ${modeLabel} · ${size} entités<br>
            ${nafCode ? `Code NAF : <strong>${escapeHtml(nafCode)}</strong><br>` : ''}
            Pipeline : Google Maps → Website Crawl
        `;
    };

    // Attach listeners
    ['batch-sector', 'batch-dept', 'batch-city', 'batch-size', 'batch-mode', 'batch-naf'].forEach(id => {
        document.getElementById(id).addEventListener('input', updateSummary);
        document.getElementById(id).addEventListener('change', updateSummary);
    });

    // Launch button — calls the API and spawns the runner automatically
    document.getElementById('btn-launch-batch').addEventListener('click', async () => {
        const sector = document.getElementById('batch-sector').value.trim();
        const dept = document.getElementById('batch-dept').value;
        if (!sector || !dept) {
            alert('⚠️ Veuillez remplir le secteur et le département');
            return;
        }
        const size = document.getElementById('batch-size').value;
        const mode = document.getElementById('batch-mode').value;
        const city = document.getElementById('batch-city').value.trim();
        const nafCode = document.getElementById('batch-naf').value.trim();

        // Disable button and show loading state
        const btn = document.getElementById('btn-launch-batch');
        btn.disabled = true;
        btn.innerHTML = '⏳ Lancement en cours...';

        document.getElementById('batch-summary-content').innerHTML = `
            <div style="color:var(--warning); font-weight:700">⏳ Envoi de la configuration au serveur...</div>
        `;

        try {
            const payload = { sector, department: dept, size, mode, city };
            if (nafCode) payload.naf_code = nafCode;
            const result = await runBatch(payload);

            if (result && result._ok && result.status === 'launched') {
                document.getElementById('batch-summary-content').innerHTML = `
                    <div style="color:var(--success); font-weight:700; margin-bottom:var(--space-sm)">
                        ✅ Batch lancé avec succès !
                    </div>
                    <div style="font-size:var(--font-sm); color:var(--text-secondary); line-height:1.8">
                        <strong>ID du batch :</strong> ${escapeHtml(result.query_id || '—')}<br>
                        <strong>Statut :</strong> En cours d'exécution
                    </div>
                    <div style="margin-top:var(--space-md); font-size:var(--font-xs); color:var(--text-muted)">
                        Redirection vers Pipeline Live dans 3 secondes...
                    </div>
                `;

                // Redirect to Pipeline Live after 3 seconds
                setTimeout(() => {
                    window.location.hash = '#/monitor';
                }, 3000);
            } else {
                const errorMsg = extractApiError(result);
                document.getElementById('batch-summary-content').innerHTML = `
                    <div style="color:var(--danger); font-weight:700">❌ Erreur : ${escapeHtml(errorMsg)}</div>
                `;
                btn.disabled = false;
                btn.innerHTML = '🚀 Lancer le Batch';
            }
        } catch (err) {
            document.getElementById('batch-summary-content').innerHTML = `
                <div style="color:var(--danger); font-weight:700">❌ Erreur de connexion : ${escapeHtml(err.message)}</div>
            `;
            btn.disabled = false;
            btn.innerHTML = '🚀 Lancer le Batch';
        }
    });
}
