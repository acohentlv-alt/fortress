/**
 * New Batch Page — Launch form for new scrape jobs
 *
 * Supports two discovery strategies:
 *   - Base SIRENE: query the SIRENE DB, then try to find entities on Maps
 *   - Base Maps: search Google Maps directly, then match to SIRENE
 */

import { escapeHtml } from '../components.js';
import { runBatch, extractApiError } from '../api.js';
import { DEPARTMENTS } from '../constants.js';

export async function renderNewBatch(container) {
    container.innerHTML = `
        <h1 class="page-title">🚀 Nouveau Batch</h1>
        <p class="page-subtitle">Configurer et lancer une nouvelle collecte de données</p>

        <div class="batch-form">
            <!-- Strategy Toggle -->
            <div class="form-group">
                <label class="form-label">Stratégie de découverte</label>
                <div id="strategy-toggle" style="display:flex; gap:0; margin-top:var(--space-sm); border-radius:var(--radius); overflow:hidden; border:2px solid var(--accent)">
                    <button type="button" id="btn-strategy-sirene" class="strategy-btn strategy-active"
                        style="flex:1; padding:var(--space-md) var(--space-lg); border:none; cursor:pointer; font-weight:700; font-size:var(--font-sm); transition:all var(--transition-fast); display:flex; align-items:center; justify-content:center; gap:var(--space-sm)">
                        🏢 Base SIRENE
                    </button>
                    <button type="button" id="btn-strategy-maps" class="strategy-btn"
                        style="flex:1; padding:var(--space-md) var(--space-lg); border:none; cursor:pointer; font-weight:700; font-size:var(--font-sm); transition:all var(--transition-fast); display:flex; align-items:center; justify-content:center; gap:var(--space-sm)">
                        🗺️ Base Maps
                    </button>
                </div>
                <div id="strategy-hint" class="form-hint" style="margin-top:var(--space-sm)">
                    Recherche dans la base SIRENE (14.7M entreprises), puis enrichissement via Google Maps
                </div>
            </div>

            <!-- Sector / Job Name -->
            <div class="form-group">
                <label class="form-label" for="batch-sector">Secteur d'activité</label>
                <input type="text" id="batch-sector" class="form-input"
                    placeholder="ex: agriculture, transport, camping..."
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
                <div class="form-group" id="city-group">
                    <label class="form-label" for="batch-city">Ville (optionnel)</label>
                    <input type="text" id="batch-city" class="form-input"
                        placeholder="ex: Toulouse, Perpignan..."
                        autocomplete="off">
                    <div class="form-hint">Affine la recherche à une ville spécifique</div>
                </div>
            </div>

            <!-- SIRENE-only fields -->
            <div id="sirene-fields">
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
            </div>

            <!-- Maps-only fields (hidden by default) -->
            <div id="maps-fields" style="display:none">
                <div class="form-group">
                    <label class="form-label">Termes de recherche Maps</label>
                    <div class="form-hint" style="margin-bottom:var(--space-sm)">
                        Ajoutez un ou plusieurs termes de recherche Google Maps. Chaque terme sera recherché séparément et tous les résultats seront collectés.
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
                        💡 Astuce : Ajoutez des variations (camping + ville, camping + code postal) pour maximiser la couverture
                    </div>
                </div>
            </div>



            <!-- Summary Preview -->
            <div id="batch-summary" style="background:var(--bg-secondary); border:1px solid var(--accent-subtle); border-left:3px solid var(--accent); border-radius:var(--radius); padding:var(--space-xl); margin-top:var(--space-xl)">
                <div style="font-size:var(--font-xs); font-weight:700; color:var(--accent-hover); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-md)">
                    📋 Aperçu du batch
                </div>
                <div id="batch-summary-content" style="font-size:var(--font-sm); color:var(--text-secondary); line-height:1.6">
                    Remplissez le secteur et le département pour voir le résumé de votre batch
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

    // ── State ─────────────────────────────────────────────────────────
    let currentStrategy = 'sirene';

    // ── Strategy toggle logic ─────────────────────────────────────────
    const btnSirene = document.getElementById('btn-strategy-sirene');
    const btnMaps = document.getElementById('btn-strategy-maps');
    const strategyHint = document.getElementById('strategy-hint');
    const sireneFields = document.getElementById('sirene-fields');
    const mapsFields = document.getElementById('maps-fields');


    function setActiveStrategy(btn) {
        // Style active
        btn.style.background = 'var(--accent)';
        btn.style.color = 'var(--bg-primary)';
    }
    function setInactiveStrategy(btn) {
        btn.style.background = 'var(--bg-secondary)';
        btn.style.color = 'var(--text-secondary)';
    }

    function switchStrategy(strategy) {
        currentStrategy = strategy;
        if (strategy === 'sirene') {
            setActiveStrategy(btnSirene);
            setInactiveStrategy(btnMaps);
            sireneFields.style.display = '';
            mapsFields.style.display = 'none';
            strategyHint.textContent = 'Recherche dans la base SIRENE (14.7M entreprises), puis enrichissement via Google Maps';

        } else {
            setActiveStrategy(btnMaps);
            setInactiveStrategy(btnSirene);
            sireneFields.style.display = 'none';
            mapsFields.style.display = '';
            strategyHint.textContent = 'Recherche directe sur Google Maps, puis matching avec la base SIRENE pour données légales';

        }
        updateSummary();
    }

    // Initial state
    setActiveStrategy(btnSirene);
    setInactiveStrategy(btnMaps);

    btnSirene.addEventListener('click', () => switchStrategy('sirene'));
    btnMaps.addEventListener('click', () => switchStrategy('maps'));

    // ── Dynamic search query management ───────────────────────────────
    const queriesContainer = document.getElementById('search-queries-container');
    const btnAddQuery = document.getElementById('btn-add-query');

    function createQueryRow(value = '') {
        const row = document.createElement('div');
        row.className = 'search-query-row';
        row.style.cssText = 'display:flex; gap:var(--space-sm); margin-bottom:var(--space-sm); align-items:center';
        row.innerHTML = `
            <input type="text" class="form-input search-query-input" 
                placeholder="ex: camping Argelès-sur-Mer"
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
            // Can't remove the last one
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

    // Auto-populate first search query from sector + department
    function autoFillFirstQuery() {
        if (currentStrategy !== 'maps') return;
        const sector = document.getElementById('batch-sector').value.trim();
        const dept = document.getElementById('batch-dept').value;
        const city = document.getElementById('batch-city').value.trim();
        const firstInput = queriesContainer.querySelector('.search-query-input');
        if (firstInput && !firstInput.value.trim() && sector) {
            firstInput.value = city ? `${sector} ${city}` : (dept ? `${sector} ${dept}` : sector);
        }
    }

    // ── Live summary update ───────────────────────────────────────────
    const updateSummary = () => {
        const sector = document.getElementById('batch-sector').value.trim();
        const dept = document.getElementById('batch-dept').value;
        const city = document.getElementById('batch-city').value.trim();

        if (!sector || !dept) {
            document.getElementById('batch-summary-content').innerHTML =
                'Remplissez le secteur et le département pour voir le résumé de votre batch';
            return;
        }

        const location = city ? `${city} (${dept})` : `département ${dept}`;

        if (currentStrategy === 'sirene') {
            const size = document.getElementById('batch-size').value;
            const nafCode = document.getElementById('batch-naf').value.trim();
            document.getElementById('batch-summary-content').innerHTML = `
                <strong>🏢 SIRENE-first</strong> — ${escapeHtml(sector.toUpperCase())} · ${escapeHtml(location)}<br>
                ${size} entités à collecter<br>
                ${nafCode ? `Code NAF : <strong>${escapeHtml(nafCode)}</strong><br>` : ''}
                Pipeline : SIRENE → Google Maps → Website Crawl
            `;
        } else {
            const queryInputs = queriesContainer.querySelectorAll('.search-query-input');
            const queries = Array.from(queryInputs)
                .map(i => i.value.trim())
                .filter(q => q.length > 0);

            document.getElementById('batch-summary-content').innerHTML = `
                <strong>🗺️ Maps-first</strong> — ${escapeHtml(sector.toUpperCase())} · ${escapeHtml(location)}<br>
                ${queries.length} recherche${queries.length > 1 ? 's' : ''} Maps :
                ${queries.length > 0
                    ? '<ul style="margin:var(--space-xs) 0 0 var(--space-lg); padding:0">' +
                      queries.map(q => `<li style="color:var(--text-primary)">${escapeHtml(q)}</li>`).join('') +
                      '</ul>'
                    : '<em style="color:var(--text-muted)">Ajoutez des termes de recherche</em>'
                }
                Pipeline : Google Maps → SIRENE matching → Website Crawl
            `;
        }
    };

    // ── Attach listeners ──────────────────────────────────────────────
    ['batch-sector', 'batch-dept', 'batch-city', 'batch-size', 'batch-naf'].forEach(id => {
        const el = document.getElementById(id);
        if (el) {
            el.addEventListener('input', updateSummary);
            el.addEventListener('change', updateSummary);
        }
    });

    // Auto-fill maps query when sector/dept change
    ['batch-sector', 'batch-dept', 'batch-city'].forEach(id => {
        document.getElementById(id).addEventListener('change', autoFillFirstQuery);
    });

    // ── Launch button ─────────────────────────────────────────────────
    document.getElementById('btn-launch-batch').addEventListener('click', async () => {
        const sector = document.getElementById('batch-sector').value.trim();
        const dept = document.getElementById('batch-dept').value;
        if (!sector || !dept) {
            alert('⚠️ Veuillez remplir le secteur et le département');
            return;
        }

        const city = document.getElementById('batch-city').value.trim();
        const btn = document.getElementById('btn-launch-batch');

        // Build payload based on strategy
        const payload = {
            sector,
            department: dept,
            mode: 'discovery',
            strategy: currentStrategy,
            city: city || undefined,
        };

        if (currentStrategy === 'sirene') {
            payload.size = parseInt(document.getElementById('batch-size').value) || 20;
            const nafCode = document.getElementById('batch-naf').value.trim();
            if (nafCode) payload.naf_code = nafCode;
        } else {
            // Maps-first: collect search queries
            const queryInputs = queriesContainer.querySelectorAll('.search-query-input');
            const queries = Array.from(queryInputs)
                .map(i => i.value.trim())
                .filter(q => q.length > 0);

            if (queries.length === 0) {
                alert('⚠️ Ajoutez au moins un terme de recherche Maps');
                return;
            }
            payload.search_queries = queries;
            payload.size = 1; // Placeholder — maps runner updates batch_size dynamically
        }

        // Disable button and show loading state
        btn.disabled = true;
        btn.innerHTML = '⏳ Lancement en cours...';

        document.getElementById('batch-summary-content').innerHTML = `
            <div style="color:var(--warning); font-weight:700">⏳ Envoi de la configuration au serveur...</div>
        `;

        try {
            const result = await runBatch(payload);

            if (result && result._ok && result.status === 'launched') {
                const strategyLabel = currentStrategy === 'maps' ? '🗺️ Maps-first' : '🏢 SIRENE-first';
                document.getElementById('batch-summary-content').innerHTML = `
                    <div style="color:var(--success); font-weight:700; margin-bottom:var(--space-sm)">
                        ✅ Batch lancé avec succès ! (${strategyLabel})
                    </div>
                    <div style="font-size:var(--font-sm); color:var(--text-secondary); line-height:1.8">
                        <strong>ID du batch :</strong> ${escapeHtml(result.query_id || '—')}<br>
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
