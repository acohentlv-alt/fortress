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

import { escapeHtml, showToast } from '../components.js';
import { runBatch, extractApiError } from '../api.js';

// French department names → codes (lowercase, accent-insensitive keys)
const DEPT_NAMES = {
    'ain':'01','aisne':'02','allier':'03','alpes de haute provence':'04','hautes alpes':'05',
    'alpes maritimes':'06','ardeche':'07','ardennes':'08','ariege':'09','aube':'10',
    'aude':'11','aveyron':'12','bouches du rhone':'13','calvados':'14','cantal':'15',
    'charente':'16','charente maritime':'17','cher':'18','correze':'19','corse du sud':'2a',
    'haute corse':'2b','cote d or':'21','cotes d armor':'22','creuse':'23','dordogne':'24',
    'doubs':'25','drome':'26','eure':'27','eure et loir':'28','finistere':'29',
    'gard':'30','haute garonne':'31','gers':'32','gironde':'33','herault':'34',
    'ille et vilaine':'35','indre':'36','indre et loire':'37','isere':'38','jura':'39',
    'landes':'40','loir et cher':'41','loire':'42','haute loire':'43','loire atlantique':'44',
    'loiret':'45','lot':'46','lot et garonne':'47','lozere':'48','maine et loire':'49',
    'manche':'50','marne':'51','haute marne':'52','mayenne':'53','meurthe et moselle':'54',
    'meuse':'55','morbihan':'56','moselle':'57','nievre':'58','nord':'59',
    'oise':'60','orne':'61','pas de calais':'62','puy de dome':'63','pyrenees atlantiques':'64',
    'hautes pyrenees':'65','pyrenees orientales':'66','bas rhin':'67','haut rhin':'68','rhone':'69',
    'haute saone':'70','saone et loire':'71','sarthe':'72','savoie':'73','haute savoie':'74',
    'paris':'75','seine maritime':'76','seine et marne':'77','yvelines':'78',
    'deux sevres':'79','somme':'80','tarn':'81','tarn et garonne':'82','var':'83',
    'vaucluse':'84','vendee':'85','vienne':'86','haute vienne':'87','vosges':'88',
    'yonne':'89','territoire de belfort':'90','essonne':'91','hauts de seine':'92',
    'seine saint denis':'93','val de marne':'94','val d oise':'95',
    'guadeloupe':'971','martinique':'972','guyane':'973','reunion':'974','mayotte':'976',
    // Major cities → department
    'perpignan':'66','montpellier':'34','toulouse':'31','marseille':'13','lyon':'69',
    'nice':'06','bordeaux':'33','nantes':'44','strasbourg':'67','lille':'59',
    'rennes':'35','grenoble':'38','toulon':'83','narbonne':'11','carcassonne':'11',
    'beziers':'34','nimes':'30','avignon':'84','cannes':'06','antibes':'06',
    'pau':'64','bayonne':'64','biarritz':'64','lourdes':'65','tarbes':'65',
};

export async function renderNewBatch(container) {
    container.innerHTML = `
        <h1 class="page-title">🚀 Nouvelle Recherche</h1>
        <p class="page-subtitle">Lancez une collecte de données B2B via Google Maps</p>

        <div class="batch-form">
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
                    value="20" min="5" max="50" step="5"
                    style="max-width:140px">
                <div class="form-hint">Recommandé : 20 par batch. Maximum : 50</div>
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
            <strong>🗺️ Votre recherche</strong> — ${queries.length} terme${queries.length > 1 ? 's' : ''}<br>
            <ul style="margin:var(--space-xs) 0 0 var(--space-lg); padding:0">
                ${queries.map(q => `<li style="color:var(--text-primary)">${escapeHtml(q)}</li>`).join('')}
            </ul>
            <span style="color:var(--text-muted)">
                ${batchSize} entreprises souhaitées · Les résultats seront enrichis automatiquement
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
            showToast('Ajoutez au moins un terme de recherche', 'error');
            return;
        }

        // Safeguard: block if ALL queries are too broad (single word, no location)
        const warnings = validateQueries(queries);
        if (warnings.length === queries.length) {
            showToast('Toutes vos recherches sont trop larges. Ajoutez une ville ou un département à chaque terme.', 'error');
            return;
        }

        const btn = document.getElementById('btn-launch-batch');
        const batchSize = parseInt(document.getElementById('batch-size').value) || 20;

        // Extract sector name from first query (first word)
        const firstQuery = queries[0];
        const sector = firstQuery.split(/\s+/)[0] || 'RECHERCHE';

        // Try to extract department from queries
        // Priority: 2-digit code → 5-digit postal → department/city name → 'FR'
        let department = '';
        const _normalize = s => s.toLowerCase().normalize('NFD').replace(/[\u0300-\u036f]/g, '').trim();
        for (const q of queries) {
            // Match exact 2-digit department code
            const dept2 = q.match(/\b(\d{2})\b/);
            if (dept2) { department = dept2[1]; break; }
            // Match 5-digit postal code → take first 2 as department
            const postal = q.match(/\b(\d{5})\b/);
            if (postal) { department = postal[1].substring(0, 2); break; }
            // Match department or city name from DEPT_NAMES
            const norm = _normalize(q);
            // Try longest matches first (e.g. "pyrenees orientales" before "pyrenees")
            const sortedKeys = Object.keys(DEPT_NAMES).sort((a, b) => b.length - a.length);
            for (const name of sortedKeys) {
                if (norm.includes(name)) { department = DEPT_NAMES[name]; break; }
            }
            if (department) break;
        }
        if (!department) department = 'FR';

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
                        <strong>ID :</strong> ${escapeHtml(result.batch_id || '—')}<br>
                        <strong>Statut :</strong> En cours d'exécution
                    </div>
                    <div style="margin-top:var(--space-md); font-size:var(--font-xs); color:var(--text-muted)">
                        Redirection vers Pipeline Live dans 3 secondes...
                    </div>
                `;

                setTimeout(() => {
                    window.location.hash = `#/monitor/${encodeURIComponent(result.batch_id)}`;
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
