/**
 * New Batch Page — Simplified Maps-first search form
 *
 * User provides:
 *   1. Search queries (e.g. "camping Perpignan", "transport 66")
 *   2. Batch size (how many companies to collect)
 *
 * Removed: strategy toggle, department dropdown, city input, NAF code
 * (These are embedded in the natural language query or moved to Base SIRENE)
 */

import { escapeHtml } from '../components.js';
import { runBatch, extractApiError } from '../api.js';

export async function renderNewBatch(container) {
    container.innerHTML = `
        <h1 class="page-title">🚀 Nouvelle Recherche</h1>
        <p class="page-subtitle">Lancez une collecte de données B2B via Google Maps</p>

        <div class="batch-form" style="max-width:640px">
            <!-- Search Queries -->
            <div class="form-group">
                <label class="form-label">🔍 Que cherchez-vous ?</label>
                <div class="form-hint" style="margin-bottom:var(--space-md)">
                    Décrivez votre recherche en incluant le <strong>secteur</strong> et la <strong>localisation</strong>.
                    Chaque terme sera recherché séparément sur Google Maps.
                </div>
                <div id="search-queries-container">
                    <div class="search-query-row" style="display:flex; gap:var(--space-sm); margin-bottom:var(--space-sm); align-items:center">
                        <input type="text" class="form-input search-query-input"
                            placeholder="ex: camping Perpignan"
                            autocomplete="off" style="flex:1">
                        <button type="button" class="btn-remove-query"
                            style="background:none; border:none; color:var(--text-muted); cursor:pointer; font-size:18px; padding:4px 8px; opacity:0.3"
                            disabled title="Minimum 1 recherche">✕</button>
                    </div>
                </div>
                <button type="button" id="btn-add-query"
                    style="display:flex; align-items:center; gap:var(--space-sm); padding:var(--space-sm) var(--space-md); background:var(--bg-secondary); border:2px dashed var(--border); border-radius:var(--radius-sm); color:var(--accent); cursor:pointer; font-size:var(--font-sm); font-weight:600; transition:all var(--transition-fast); width:100%"
                    onmouseover="this.style.borderColor='var(--accent)';this.style.background='var(--bg-hover)'"
                    onmouseout="this.style.borderColor='var(--border)';this.style.background='var(--bg-secondary)'"
                >
                    ＋ Ajouter un terme de recherche
                </button>
                <div class="form-hint" style="margin-top:var(--space-sm); color:var(--info)">
                    💡 Ajoutez des variations (camping + ville, camping + code postal) pour maximiser la couverture
                </div>
            </div>

            <!-- Batch Size -->
            <div class="form-group">
                <label class="form-label" for="batch-size">📊 Nombre d'entreprises souhaité</label>
                <input type="number" id="batch-size" class="form-input"
                    value="20" min="5" max="200" step="5"
                    style="max-width:140px">
                <div class="form-hint">Recommandé : 20 par batch. Maximum : 200</div>
            </div>

            <!-- Safeguard Warning Area -->
            <div id="batch-warning" style="display:none; margin-bottom:var(--space-lg);
                padding:var(--space-md); background:var(--warning-subtle);
                border:1px solid rgba(245,158,11,0.3); border-radius:var(--radius-sm);
                font-size:var(--font-sm); color:var(--warning)">
            </div>

            <!-- Summary Preview -->
            <div id="batch-summary" style="background:var(--bg-secondary); border:1px solid var(--accent-subtle); border-left:3px solid var(--accent); border-radius:var(--radius); padding:var(--space-xl); margin-top:var(--space-xl)">
                <div style="font-size:var(--font-xs); font-weight:700; color:var(--accent-hover); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-md)">
                    📋 Aperçu de la recherche
                </div>
                <div id="batch-summary-content" style="font-size:var(--font-sm); color:var(--text-secondary); line-height:1.6">
                    Ajoutez au moins un terme de recherche pour voir l'aperçu
                </div>
            </div>

            <!-- Actions -->
            <div class="form-actions">
                <button class="btn btn-primary" id="btn-launch-batch" style="padding:var(--space-md) var(--space-2xl)">
                    🚀 Lancer la Recherche
                </button>
                <button class="btn btn-secondary" onclick="window.location.hash='#/'">
                    Annuler
                </button>
            </div>

            <!-- Info note -->
            <div style="margin-top:var(--space-xl); padding:var(--space-lg); background:var(--info-subtle); border-radius:var(--radius-sm); font-size:var(--font-sm); color:var(--info)">
                ⓘ Le moteur va rechercher chaque terme sur Google Maps, puis croiser les résultats avec la base SIRENE
                pour les données légales. Les sites web trouvés seront scannés pour les emails et réseaux sociaux.
            </div>
        </div>
    `;

    // ── Dynamic search query management ───────────────────────────────
    const queriesContainer = document.getElementById('search-queries-container');
    const btnAddQuery = document.getElementById('btn-add-query');

    function createQueryRow(value = '') {
        const row = document.createElement('div');
        row.className = 'search-query-row';
        row.style.cssText = 'display:flex; gap:var(--space-sm); margin-bottom:var(--space-sm); align-items:center';
        row.innerHTML = `
            <input type="text" class="form-input search-query-input"
                placeholder="ex: transport Lyon"
                autocomplete="off" style="flex:1" value="${escapeHtml(value)}">
            <button type="button" class="btn-remove-query"
                style="background:none; border:1px solid var(--danger); border-radius:var(--radius-sm); color:var(--danger); cursor:pointer; font-size:14px; padding:6px 10px; transition:all var(--transition-fast)"
                title="Supprimer cette recherche"
                onmouseover="this.style.background='var(--danger)';this.style.color='white'"
                onmouseout="this.style.background='none';this.style.color='var(--danger)'"
            >✕</button>
        `;
        row.querySelector('.btn-remove-query').addEventListener('click', () => {
            row.remove();
            updateRemoveButtons();
            updateSummary();
        });
        row.querySelector('.search-query-input').addEventListener('input', updateSummary);
        return row;
    }

    function updateRemoveButtons() {
        const removes = queriesContainer.querySelectorAll('.btn-remove-query');
        if (removes.length <= 1) {
            removes.forEach(btn => {
                btn.disabled = true;
                btn.style.opacity = '0.3';
                btn.style.cursor = 'default';
                btn.style.border = 'none';
            });
        } else {
            removes.forEach(btn => {
                btn.disabled = false;
                btn.style.opacity = '1';
                btn.style.cursor = 'pointer';
                btn.style.border = '1px solid var(--danger)';
            });
        }
    }

    btnAddQuery.addEventListener('click', () => {
        const newRow = createQueryRow();
        queriesContainer.appendChild(newRow);
        newRow.querySelector('.search-query-input').focus();
        updateRemoveButtons();
        updateSummary();
    });

    // ── Safeguard: check for overly broad queries ─────────────────────
    function validateQueries(queries) {
        const warnings = [];
        for (const q of queries) {
            const words = q.trim().split(/\s+/);
            // Single word without location = too broad
            if (words.length === 1 && !/^\d{2,5}$/.test(words[0])) {
                warnings.push(`"${q}" — Ajoutez une localisation (ville ou département) pour éviter trop de résultats`);
            }
        }
        return warnings;
    }

    // ── Live summary update ───────────────────────────────────────────
    const updateSummary = () => {
        const queryInputs = queriesContainer.querySelectorAll('.search-query-input');
        const queries = Array.from(queryInputs)
            .map(i => i.value.trim())
            .filter(q => q.length > 0);

        const batchSize = document.getElementById('batch-size').value;
        const warningDiv = document.getElementById('batch-warning');

        // Safeguard check
        const warnings = validateQueries(queries);
        if (warnings.length > 0) {
            warningDiv.style.display = 'block';
            warningDiv.innerHTML = '⚠️ ' + warnings.join('<br>⚠️ ');
        } else {
            warningDiv.style.display = 'none';
        }

        if (queries.length === 0) {
            document.getElementById('batch-summary-content').innerHTML =
                'Ajoutez au moins un terme de recherche pour voir l\'aperçu';
            return;
        }

        document.getElementById('batch-summary-content').innerHTML = `
            <strong>🗺️ Recherche Maps</strong> — ${queries.length} terme${queries.length > 1 ? 's' : ''}<br>
            <ul style="margin:var(--space-xs) 0 0 var(--space-lg); padding:0">
                ${queries.map(q => `<li style="color:var(--text-primary)">${escapeHtml(q)}</li>`).join('')}
            </ul>
            <span style="color:var(--text-muted)">
                ${batchSize} entreprises souhaitées · Pipeline : Google Maps → SIRENE → Crawl
            </span>
        `;
    };

    // ── Attach listeners ──────────────────────────────────────────────
    document.getElementById('batch-size').addEventListener('input', updateSummary);
    document.getElementById('batch-size').addEventListener('change', updateSummary);

    // ── Launch button ─────────────────────────────────────────────────
    document.getElementById('btn-launch-batch').addEventListener('click', async () => {
        const queryInputs = queriesContainer.querySelectorAll('.search-query-input');
        const queries = Array.from(queryInputs)
            .map(i => i.value.trim())
            .filter(q => q.length > 0);

        if (queries.length === 0) {
            alert('⚠️ Ajoutez au moins un terme de recherche');
            return;
        }

        // Safeguard: block if ALL queries are too broad (single word, no location)
        const warnings = validateQueries(queries);
        if (warnings.length === queries.length) {
            alert('⚠️ Toutes vos recherches sont trop larges.\nAjoutez une ville ou un département à chaque terme.\n\nExemple : "camping Perpignan" au lieu de "camping"');
            return;
        }

        const btn = document.getElementById('btn-launch-batch');
        const batchSize = parseInt(document.getElementById('batch-size').value) || 20;

        // Extract sector name from first query (first word)
        const firstQuery = queries[0];
        const sector = firstQuery.split(/\s+/)[0] || 'RECHERCHE';

        // Try to extract department from queries
        let department = '';
        for (const q of queries) {
            const deptMatch = q.match(/\b(\d{2})\b/);
            if (deptMatch) { department = deptMatch[1]; break; }
            // Check for common department names
            const cityMatch = q.match(/\s+(.+)$/);
            if (cityMatch) { department = cityMatch[1].trim(); break; }
        }

        const payload = {
            sector: sector.toUpperCase(),
            department: department || '00',
            mode: 'discovery',
            strategy: 'maps',
            search_queries: queries,
            size: batchSize,
        };

        btn.disabled = true;
        btn.innerHTML = '⏳ Lancement en cours...';

        document.getElementById('batch-summary-content').innerHTML = `
            <div style="color:var(--warning); font-weight:700">⏳ Envoi de la configuration au serveur...</div>
        `;

        try {
            const result = await runBatch(payload);

            if (result && result._ok && result.status === 'launched') {
                document.getElementById('batch-summary-content').innerHTML = `
                    <div style="color:var(--success); font-weight:700; margin-bottom:var(--space-sm)">
                        ✅ Recherche lancée avec succès !
                    </div>
                    <div style="font-size:var(--font-sm); color:var(--text-secondary); line-height:1.8">
                        <strong>ID :</strong> ${escapeHtml(result.query_id || '—')}<br>
                        <strong>Statut :</strong> En cours d'exécution
                    </div>
                    <div style="margin-top:var(--space-md); font-size:var(--font-xs); color:var(--text-muted)">
                        Redirection vers Pipeline Live dans 3 secondes...
                    </div>
                `;

                setTimeout(() => {
                    window.location.hash = '#/monitor';
                }, 3000);
            } else {
                const errorMsg = extractApiError(result);
                document.getElementById('batch-summary-content').innerHTML = `
                    <div style="color:var(--danger); font-weight:700">❌ Erreur : ${escapeHtml(errorMsg)}</div>
                `;
                btn.disabled = false;
                btn.innerHTML = '🚀 Lancer la Recherche';
            }
        } catch (err) {
            document.getElementById('batch-summary-content').innerHTML = `
                <div style="color:var(--danger); font-weight:700">❌ Erreur de connexion : ${escapeHtml(err.message)}</div>
            `;
            btn.disabled = false;
            btn.innerHTML = '🚀 Lancer la Recherche';
        }
    });
}
