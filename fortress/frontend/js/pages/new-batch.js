/**
 * New Batch Page — Gemini-style centered prompt
 *
 * User provides:
 *   1. Search queries (e.g. "camping Perpignan", "transport 66")
 *
 * Exhaustive-by-default: every batch runs until Google Maps is exhausted
 * or the 2000-entity safety ceiling is hit.
 *
 * Rebuilt as Gemini-style prompt interface with suggestion chips,
 * inline warnings, live summary, and "Comment ça marche" expandable card.
 */

import { escapeHtml, showToast, showConfirmModal } from '../components.js';
import { runBatch, extractApiError, fetchTopQueries } from '../api.js';
import { t, getLang } from '../i18n.js';

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

/**
 * Parse a query string into a dept code.
 * Mirrors fortress/discovery.py:104 _parse_dept_hint_from_query.
 */
function parseDeptHint(query) {
    if (!query) return null;
    const postal = query.match(/\b(\d{5})\b/);
    if (postal) {
        const p = postal[1];
        // Corsica: 20000-20190 → 2A, 20200+ → 2B
        if (p.startsWith('20')) {
            const num = parseInt(p, 10);
            return num <= 20190 ? '2A' : '2B';
        }
        if (p.startsWith('97') || p.startsWith('98')) return p.substring(0, 3);
        return p.substring(0, 2);
    }
    const dept = query.match(/\b(\d{2,3})\b/);
    if (dept) return dept[1];
    const norm = query.toLowerCase();
    const sortedKeys = Object.keys(DEPT_NAMES).sort((a, b) => b.length - a.length);
    for (const name of sortedKeys) {
        if (norm.includes(name)) return DEPT_NAMES[name];
    }
    return null;
}

/**
 * Strip dept-hint tokens from a primary query, preserving sector tokens.
 */
function extractSectorTokens(primaryQuery) {
    if (!primaryQuery) return '';
    const queryDept = parseDeptHint(primaryQuery);
    return primaryQuery
        .split(/\s+/)
        .filter(token => {
            if (/^\d{2,5}$/.test(token)) return false;
            const lower = token.toLowerCase();
            if (queryDept && DEPT_NAMES[lower] === queryDept) return false;
            return true;
        })
        .join(' ')
        .trim();
}

/**
 * Paris arrondissement filter regex. Covers all 6 observed formats:
 * PARIS 1, PARIS 12, PARIS 04, PARIS 11E, PARIS 16E ARRONDISSEMENT,
 * PARIS 1ER ARRONDISSEMENT, PARIS CEDEX 19. Does NOT match bare "PARIS".
 */
const PARIS_ARR_RE = /^PARIS\s+(\d{1,2}(?:E|ER)?(\s+ARRONDISSEMENT)?|CEDEX\s+\d+)$/i;

export async function renderNewBatch(container) {
    container.innerHTML = `
        <div class="gemini-wrapper">
            <div class="gemini-container">

                <h1 class="gemini-headline">${t('newBatch.headline')}</h1>

                <div class="gemini-prompt-box">
                    <div class="gemini-query-row primary" data-query-index="0">
                        <input type="text"
                            class="gemini-query-input primary"
                            placeholder="${t('newBatch.promptPlaceholder')}"
                            autocomplete="off">
                    </div>
                    <div id="naf-variants-row" style="display:none; margin-top:var(--space-sm)">
                        <span class="gemini-chips-label" style="font-size:var(--font-sm); color:var(--text-muted)">${t('newBatch.nafVariantsLabel')}</span>
                        <div class="gemini-chips" id="naf-variants-chips" style="margin-top:var(--space-xs)"></div>
                    </div>
                    <div id="additional-queries"></div>
                    <div class="city-picker-panel" id="city-picker-panel" style="display:none">
                      <div class="city-picker-header">
                        <h4 class="city-picker-title">
                          Communes du département <span id="city-picker-dept">--</span>
                          <span class="city-picker-count" id="city-picker-count"></span>
                        </h4>
                        <input type="text" id="city-picker-search"
                               placeholder="${t('newBatch.cityPicker.searchPlaceholder')}"
                               class="city-picker-search">
                      </div>
                      <div class="city-picker-chips" id="city-picker-chips"></div>
                      <div class="city-picker-actions">
                        <button type="button" id="city-picker-expand" class="btn btn-secondary city-picker-expand">
                          ${t('newBatch.cityPicker.expandPrefix')} <span id="city-picker-more">0</span> ${t('newBatch.cityPicker.expandSuffix')}
                        </button>
                        <button type="button" id="city-picker-clear" class="btn btn-secondary city-picker-action">
                          ${t('newBatch.cityPicker.clearAll')}
                        </button>
                        <button type="button" id="city-picker-discover-all" class="btn btn-primary city-picker-action" disabled>
                          🌍 ${t('newBatch.cityPicker.discoverAll')}
                        </button>
                      </div>
                      <div class="city-picker-info">
                        <small>${t('newBatch.cityPicker.info')}</small>
                      </div>
                      <div class="city-picker-paris-hint" id="city-picker-paris-hint" style="display:none">
                        <small>${t('newBatch.cityPicker.parisHint')}</small>
                      </div>
                    </div>
                    <button type="button" id="btn-add-query" class="gemini-add-query-btn">
                        ${t('newBatch.addQueryBtn')}
                    </button>
                </div>

                <div class="gemini-chips-wrapper">
                    <span class="gemini-chips-label">${t('newBatch.suggestionsLabel')}</span>
                    <div class="gemini-chips" id="suggestion-chips"></div>
                </div>

                <div class="gemini-controls">
                    <span class="gemini-controls-label">${t('newBatch.nafLabel')}</span>
                    <div class="naf-picker naf-picker-multi" id="naf-picker">
                        <div class="naf-chips" id="naf-chips"></div>
                        <input type="text" class="naf-picker-input" id="naf-picker-input"
                               placeholder="${t('newBatch.nafPlaceholder')}" autocomplete="off">
                        <div class="naf-picker-dropdown" id="naf-picker-dropdown"></div>
                        <div class="naf-picker-error" id="naf-picker-error" role="alert" aria-live="polite" style="display:none"></div>
                    </div>
                </div>

                <div id="naf-siblings-row" style="display:none; margin-top:var(--space-sm)">
                    <span class="gemini-chips-label" style="font-size:var(--font-sm); color:var(--text-muted)">${t('newBatch.nafSiblingsLabel')}</span>
                    <div class="gemini-chips" id="naf-siblings-chips" style="margin-top:var(--space-xs)"></div>
                </div>

                <div class="strict-mode-block">
                    <label class="strict-mode-toggle">
                        <input type="checkbox" id="strict-naf-toggle" disabled title="${t('newBatch.strictMode.disabledTooltip')}">
                        <span>${t('newBatch.strictMode.label')}</span>
                    </label>
                    <div class="strict-mode-explanation strict-explanation-off" id="strict-mode-explanation-off">${t('newBatch.strictMode.explanation.off')}</div>
                    <div class="strict-mode-explanation strict-explanation-on" id="strict-mode-explanation-on" style="display:none">${t('newBatch.strictMode.explanation.on')}</div>
                </div>

                <div style="text-align: center; margin: var(--space-md) 0;">
                    <div style="color: var(--text-secondary); font-size: var(--font-sm); margin-bottom: var(--space-sm);">
                        ${t('newBatch.timeCapLabel')}
                    </div>
                    <div class="time-cap-pills" id="time-cap-pills" role="radiogroup">
                        <button type="button" class="cap-pill" data-cap-min="5">5 min</button>
                        <button type="button" class="cap-pill" data-cap-min="10">10 min</button>
                        <button type="button" class="cap-pill" data-cap-min="15">15 min</button>
                        <button type="button" class="cap-pill" data-cap-min="30">30 min</button>
                        <button type="button" class="cap-pill" data-cap-min="45">45 min</button>
                        <button type="button" class="cap-pill active" data-cap-min="60">60 min</button>
                        <button type="button" class="cap-pill" data-cap-min="120">120 min</button>
                        <button type="button" class="cap-pill" data-cap-min="240">240 min</button>
                        <button type="button" class="cap-pill" data-cap-min="0">${t('newBatch.timeCapNone')}</button>
                    </div>
                </div>

                <div style="text-align: center; margin: var(--space-md) 0;">
                    <div style="color: var(--text-secondary); font-size: var(--font-sm); margin-bottom: var(--space-sm);">
                        ${t('newBatch.timeCapTotalLabel')}
                    </div>
                    <div class="time-cap-pills" id="time-cap-total-pills" role="radiogroup">
                        <button type="button" class="cap-pill" data-cap-min="5">5 min</button>
                        <button type="button" class="cap-pill" data-cap-min="10">10 min</button>
                        <button type="button" class="cap-pill" data-cap-min="15">15 min</button>
                        <button type="button" class="cap-pill" data-cap-min="30">30 min</button>
                        <button type="button" class="cap-pill" data-cap-min="45">45 min</button>
                        <button type="button" class="cap-pill" data-cap-min="60">60 min</button>
                        <button type="button" class="cap-pill" data-cap-min="120">120 min</button>
                        <button type="button" class="cap-pill" data-cap-min="240">240 min</button>
                        <button type="button" class="cap-pill active" data-cap-min="0">${t('newBatch.timeCapNone')}</button>
                    </div>
                </div>

                <div style="text-align: center; margin: var(--space-md) 0;">
                    <div style="color: var(--text-secondary); font-size: var(--font-sm); margin-bottom: var(--space-sm);">
                        ${t('newBatch.entityCap.label')}
                    </div>
                    <div class="time-cap-pills" id="entity-cap-pills" role="radiogroup">
                        <button type="button" class="cap-pill active" data-entity-cap="0">${t('newBatch.entityCap.none')}</button>
                        <button type="button" class="cap-pill" data-entity-cap="50">50</button>
                        <button type="button" class="cap-pill" data-entity-cap="100">100</button>
                        <button type="button" class="cap-pill" data-entity-cap="200">200</button>
                        <button type="button" class="cap-pill" data-entity-cap="500">500</button>
                        <button type="button" class="cap-pill" data-entity-cap="1000">1000</button>
                    </div>
                </div>

                <div id="duration-estimator" style="display:none; text-align:center; color:var(--text-secondary); font-size:var(--font-sm); margin-top:var(--space-sm);">
                    <span id="duration-estimator-text"></span>
                </div>

                <div class="duration-hint">
                    <span class="duration-hint-icon">⏱</span>
                    <span>${t('newBatch.durationHint')}</span>
                </div>

                <button type="button" class="btn-launch-hero" id="btn-launch-batch">
                    ${t('newBatch.launchHero')}
                </button>

                <div class="gemini-summary-line" id="gemini-summary-line">
                    ${t('newBatch.summaryEmptyState')}
                </div>

                <details class="how-it-works" id="how-it-works">
                    <summary>${t('howItWorks.title')}</summary>
                    <ol class="how-it-works-steps">
                        <li><strong>1. ${t('howItWorks.step1Title')}</strong> — ${t('howItWorks.step1Body')}</li>
                        <li><strong>2. ${t('howItWorks.step2Title')}</strong> — ${t('howItWorks.step2Body')}</li>
                        <li><strong>3. ${t('howItWorks.step3Title')}</strong> — ${t('howItWorks.step3Body')}</li>
                        <li><strong>4. ${t('howItWorks.step4Title')}</strong> — ${t('howItWorks.step4Body')}</li>
                    </ol>
                </details>

            </div>
        </div>
    `;

    // ── City picker state — per-render-instance (cleared on SPA navigation away/back) ──
    const _cityPickerCache = {};
    let _cityPickerCurrentDept = null;
    let _cityPickerCurrentPrimary = null;
    let _cityPickerSelectedCities = new Set();
    let _cityPickerExpanded = false;
    let _cityPickerInputDebounce = null;
    // ── Bulk discovery state ──────────────────────────────────────────
    let _cityPickerBulkSelection = null;
    // shape: { dept, communes: [], metadata: {recommended, skipped, expected_entities, estimated_minutes, dept_name, communes_with_sirene_match}, forceCovered: bool }

    function hideCityPanel() {
        const panel = document.getElementById('city-picker-panel');
        if (panel) panel.style.display = 'none';
        _cityPickerCurrentDept = null;
        _cityPickerCurrentPrimary = null;
    }

    // ── E3: form draft helpers (sessionStorage) ─────────────────────
    const DRAFT_KEY = 'fortress_new_batch_draft';
    let _draftSaveTimer = null;
    // Refs that get filled after _pickedCodes / _selectedTimeCap are created below
    const _pickedCodesRef = { value: [] };
    const _timeCapRef = { value: 60 };

    function saveDraft() {
        try {
            const queries = [
                document.querySelector('.gemini-query-input.primary'),
                ...document.querySelectorAll('#additional-queries .gemini-query-input'),
            ].filter(Boolean).map(i => i.value).filter(v => v && v.trim().length > 0);
            const draft = {
                v: 1,
                ts: Date.now(),
                queries,
                naf_codes: [..._pickedCodesRef.value],
                time_cap_min: _timeCapRef.value,
                entity_cap: _selectedEntityCap,
            };
            sessionStorage.setItem(DRAFT_KEY, JSON.stringify(draft));
        } catch (_e) { /* private mode or quota */ }
    }
    function scheduleSaveDraft() {
        if (_draftSaveTimer) clearTimeout(_draftSaveTimer);
        _draftSaveTimer = setTimeout(saveDraft, 500);
    }
    function clearDraft() {
        try { sessionStorage.removeItem(DRAFT_KEY); } catch (_e) {}
    }

    // ── Time cap state ────────────────────────────────────────────────
    let _selectedTimeCap = 60;
    let _selectedTotalCap = 0;
    let _selectedEntityCap = 0;
    let _timingBaseline = null;
    _timeCapRef.value = _selectedTimeCap;  // E3 init

    async function getTimingBaseline() {
        if (_timingBaseline) return _timingBaseline;
        try {
            const resp = await fetch('/api/jobs/timing-baseline');
            if (resp.ok) _timingBaseline = await resp.json();
        } catch (e) { /* ignore — fallback to 18 */ }
        if (!_timingBaseline) _timingBaseline = { avg_min_per_query: 18, sample_size: 0, fallback: true };
        return _timingBaseline;
    }

    document.querySelectorAll('#time-cap-pills .cap-pill').forEach(p => {
        p.addEventListener('click', () => {
            document.querySelectorAll('#time-cap-pills .cap-pill').forEach(x => x.classList.remove('active'));
            p.classList.add('active');
            _selectedTimeCap = parseInt(p.dataset.capMin, 10);
            _updateDurationEstimator();
            _timeCapRef.value = _selectedTimeCap;
            scheduleSaveDraft();
        });
    });

    document.querySelectorAll('#time-cap-total-pills .cap-pill').forEach(p => {
        p.addEventListener('click', () => {
            document.querySelectorAll('#time-cap-total-pills .cap-pill').forEach(x => x.classList.remove('active'));
            p.classList.add('active');
            _selectedTotalCap = parseInt(p.dataset.capMin, 10);
            _updateDurationEstimator();
            scheduleSaveDraft();
        });
    });

    document.querySelectorAll('#entity-cap-pills .cap-pill').forEach(p => {
        p.addEventListener('click', () => {
            document.querySelectorAll('#entity-cap-pills .cap-pill').forEach(x => x.classList.remove('active'));
            p.classList.add('active');
            _selectedEntityCap = parseInt(p.dataset.entityCap, 10);
            scheduleSaveDraft();
        });
    });

    // ── Strict mode toggle ────────────────────────────────────────────
    const strictToggle = document.getElementById('strict-naf-toggle');
    if (strictToggle) {
        strictToggle.addEventListener('change', () => {
            _showStrictExplanation(strictToggle.checked);
        });
    }

    // ── Suggestion chips ──────────────────────────────────────────────
    const chipsContainer = document.getElementById('suggestion-chips');
    try {
        const result = await fetchTopQueries();
        let queries = (result && result.queries) || [];
        if (queries.length < 3) {
            const fallback = [
                { query_text: 'camping 66' },
                { query_text: 'transport Paris' },
                { query_text: 'restaurant Lyon' }
            ];
            queries = [...queries, ...fallback].slice(0, 3);
        }
        chipsContainer.innerHTML = queries.map(q => `
            <button type="button" class="chip" data-query="${escapeHtml(q.query_text)}">
                ${escapeHtml(q.query_text)}
            </button>
        `).join('');
        chipsContainer.querySelectorAll('.chip').forEach(chip => {
            chip.addEventListener('click', () => {
                const primaryInput = document.querySelector('.gemini-query-input.primary');
                if (primaryInput) {
                    primaryInput.value = chip.dataset.query;
                    primaryInput.focus();
                    updateSummary();
                }
            });
        });
    } catch (e) {
        chipsContainer.innerHTML = '';
    }

    // ── NAF picker (multi-select with chips) ─────────────────────────
    let _nafData = null;
    try {
        const resp = await fetch('/api/batch/naf-codes', { credentials: 'include' });
        if (resp.ok) _nafData = await resp.json();
    } catch (_e) {}

    const _allNafEntries = _nafData ? [
        ..._nafData.sections,
        ..._nafData.divisions,
        ..._nafData.codes,
    ] : [];

    // Build lookup map: code → label (for chip rendering + paste handling)
    const _nafLabelByCode = {};
    for (const e of _allNafEntries) _nafLabelByCode[e.code] = e.label;

    // Frontend mirror of SECTOR_EXPANSIONS — fetched fresh every page load via
    // /api/batch/naf-codes. Source of truth is fortress/config/naf_sector_expansion.py.
    const _sectorExpansions = _nafData?.sector_expansions || {};
    // Per-sector Maps query phrasing variants — Lever D (sector_query_variants.py).
    const _sectorQueryVariants = _nafData?.sector_query_variants || {};
    // Human-readable keyword → NAF prefix list (industry_aliases.py).
    const _industryAliases = _nafData?.industry_aliases || {};
    // Manual-pick guard: set to true when user explicitly edits the NAF picker.
    let _userEditedPicker = false;

    // Helper: resolve a single prefix to its leaf code (or null)
    function _resolvePrefixToLeaf(prefix) {
        // Case A: prefix is itself a valid leaf entry (e.g. "10.71C", "55.30Z")
        if (_nafLabelByCode[prefix]) {
            return prefix;
        }
        // Case B: prefix is a wildcard (division "49" or section letter)
        // Find first leaf whose code starts with `prefix + "."`
        const leaf = _allNafEntries.find(e =>
            e.code.includes('.') && e.code.startsWith(prefix + '.')
        );
        return leaf ? leaf.code : null;
    }

    function _autoPickFromPrimary(primaryQuery) {
        if (_pickedCodes.length > 0) return;       // Don't overwrite manual picks
        if (_userEditedPicker) return;             // Don't auto-pick after user edit
        if (!primaryQuery) return;

        const sectorTokens = extractSectorTokens(primaryQuery).toLowerCase().trim();
        if (!sectorTokens) return;

        // Exact match first; per-token fallback
        let prefixes = _industryAliases[sectorTokens];
        if (!prefixes || !prefixes.length) {
            for (const tok of sectorTokens.split(/\s+/)) {
                if (_industryAliases[tok]) {
                    prefixes = _industryAliases[tok];
                    break;
                }
            }
        }
        if (!prefixes || !prefixes.length) return;

        // STEP A — Resolve all prefixes to leaf codes
        const resolvedCodes = [];
        for (const prefix of prefixes) {
            const leaf = _resolvePrefixToLeaf(prefix);
            if (leaf) resolvedCodes.push(leaf);
        }
        const uniqueResolved = [...new Set(resolvedCodes)];
        if (uniqueResolved.length === 0) return;

        // STEP C — Anchor (prefer one with _sectorExpansions entry);
        // filter remaining via allSameSectorGroup. Applied BEFORE cap.
        const anchor = uniqueResolved.find(c => _sectorExpansions[c]) || uniqueResolved[0];
        const safeToAdd = [anchor];
        for (const code of uniqueResolved) {
            if (code === anchor) continue;
            if (allSameSectorGroup([...safeToAdd, code])) {
                safeToAdd.push(code);
            }
        }

        // STEP B — Cap at 3 codes (applied AFTER anchor filter)
        const finalCodes = safeToAdd.slice(0, 3);

        // STEP D — Auto-add silently. Don't render naf-picker-error on failure.
        for (const code of finalCodes) {
            const result = tryAddCode(code, _nafLabelByCode[code] || code, /*_isAutoPick=*/ true);
            if (!result.ok) break;
        }
    }

    function sameSectorGroup(a, b) {
        if (a === b) return true;
        const ea = _sectorExpansions[a];
        if (ea && ea.includes(b)) return true;
        const eb = _sectorExpansions[b];
        if (eb && eb.includes(a)) return true;
        return false;
    }

    function allSameSectorGroup(codes) {
        if (codes.length <= 1) return true;
        const anchor = codes[0];
        return codes.slice(1).every(c => sameSectorGroup(anchor, c));
    }

    const MAX_NAF_CHIPS = 10;
    const _pickedCodes = [];  // Source of truth for submit payload
    const _pickedLabels = {}; // code → label, for rendering

    const nafPicker = document.getElementById('naf-picker');
    const nafInput = document.getElementById('naf-picker-input');
    const nafDropdown = document.getElementById('naf-picker-dropdown');
    const nafChips = document.getElementById('naf-chips');
    const nafError = document.getElementById('naf-picker-error');

    function showNafError(msg) {
        nafError.textContent = msg;
        nafError.style.display = 'block';
        clearTimeout(showNafError._t);
        showNafError._t = setTimeout(() => {
            nafError.style.display = 'none';
            nafError.textContent = '';
        }, 4000);
    }

    const _siblingsRow = document.getElementById('naf-siblings-row');
    const _siblingsChips = document.getElementById('naf-siblings-chips');
    const _SIBLINGS_MAX_VISIBLE = 8;
    let _siblingsExpanded = false;

    function renderSiblingsRow() {
        if (!_siblingsRow || !_siblingsChips) return;
        if (_pickedCodes.length === 0) {
            _siblingsRow.style.display = 'none';
            return;
        }
        // Compute intersection of sibling sets for all picked codes, excluding already-picked
        let intersection = null;
        for (const code of _pickedCodes) {
            const siblings = _sectorExpansions[code];
            if (!siblings) {
                intersection = [];
                break;
            }
            const sibSet = new Set(Array.isArray(siblings) ? siblings : Object.keys(siblings));
            if (intersection === null) {
                intersection = [...sibSet];
            } else {
                intersection = intersection.filter(c => sibSet.has(c));
            }
        }
        // Remove already-picked codes
        const suggestions = (intersection || []).filter(c => !_pickedCodes.includes(c));
        if (suggestions.length === 0) {
            _siblingsRow.style.display = 'none';
            return;
        }
        _siblingsRow.style.display = '';
        const visible = _siblingsExpanded ? suggestions : suggestions.slice(0, _SIBLINGS_MAX_VISIBLE);
        const overflow = suggestions.length - _SIBLINGS_MAX_VISIBLE;
        let html = visible.map(code => `
            <button type="button" class="chip naf-sibling-chip" data-code="${escapeHtml(code)}" title="${escapeHtml(_nafLabelByCode[code] || code)}">
                ${escapeHtml(_nafLabelByCode[code] || code)}
            </button>
        `).join('');
        if (!_siblingsExpanded && overflow > 0) {
            html += `<button type="button" class="chip naf-siblings-more" style="opacity:0.7">${t('newBatch.nafSiblingsShowMore').replace('{{count}}', overflow)}</button>`;
        } else if (_siblingsExpanded && suggestions.length > _SIBLINGS_MAX_VISIBLE) {
            html += `<button type="button" class="chip naf-siblings-less" style="opacity:0.7">${t('newBatch.nafSiblingsShowLess')}</button>`;
        }
        _siblingsChips.innerHTML = html;
    }

    const _variantsRow = document.getElementById('naf-variants-row');
    const _variantsChips = document.getElementById('naf-variants-chips');

    function renderVariantsRow() {
        if (!_variantsRow || !_variantsChips) return;
        if (_pickedCodes.length === 0) {
            _variantsRow.style.display = 'none';
            return;
        }
        // Apr 30 (Cindy bug fix): UNION across all picked NAFs, not intersection.
        // Previously intersection broke when a sibling NAF without its own
        // variants entry was added (e.g., picking 55.30Z camping then adding
        // 55.10Z hotels via the sibling chip → empty intersection → variants
        // vanished). Same-sector-group rule already prevents truly cross-sector
        // picks at the chip-add stage, so union is safe.
        const seen = new Set();
        const unionVariants = [];
        for (const code of _pickedCodes) {
            const list = _sectorQueryVariants[code];
            if (!list || !Array.isArray(list)) continue;
            for (const v of list) {
                if (!seen.has(v.phrasing)) {
                    seen.add(v.phrasing);
                    unionVariants.push(v);
                }
            }
        }
        if (unionVariants.length === 0) {
            _variantsRow.style.display = 'none';
            return;
        }
        _variantsRow.style.display = '';
        _variantsChips.innerHTML = unionVariants.map(v => `
            <button type="button" class="chip naf-variant-chip"
                    data-phrasing="${escapeHtml(v.phrasing)}"
                    title="${escapeHtml(t('newBatch.nafVariantTooltip').replace('{{phrasing}}', v.phrasing))}">
                ${escapeHtml(v.label)}
            </button>
        `).join('');
    }

    function renderChips() {
        nafChips.innerHTML = _pickedCodes.map(code => `
            <span class="naf-chip" data-code="${escapeHtml(code)}">
                <span class="naf-chip-label">${escapeHtml(_pickedLabels[code] || code)}</span>
                <button type="button" class="naf-chip-remove" aria-label="${t('newBatch.nafChipRemove')}">×</button>
            </span>
        `).join('');
        renderSiblingsRow();
        renderVariantsRow();
        _updateStrictToggle();
        _updateDiscoverAllButtonState();
    }

    function _updateStrictToggle() {
        const toggle = document.getElementById('strict-naf-toggle');
        if (!toggle) return;
        const hasNaf = _pickedCodes.length > 0;
        toggle.disabled = !hasNaf;
        if (!hasNaf && toggle.checked) {
            toggle.checked = false;
            _showStrictExplanation(false);
        }
        toggle.title = hasNaf ? '' : t('newBatch.strictMode.disabledTooltip');
    }

    function _showStrictExplanation(isOn) {
        const offEl = document.getElementById('strict-mode-explanation-off');
        const onEl = document.getElementById('strict-mode-explanation-on');
        if (offEl) offEl.style.display = isOn ? 'none' : '';
        if (onEl) onEl.style.display = isOn ? '' : 'none';
    }

    function tryAddCode(code, label, _isAutoPick = false) {
        // Normalize: accept both raw code "10.71C" and label-style "10.71C — Boulangerie"
        const rawCode = code.split('—')[0].trim().toUpperCase();
        if (!rawCode) return { ok: false };

        // Must exist in NAF data
        if (!_nafLabelByCode[rawCode]) return { ok: false, err: null };  // Silent: just no match

        // Duplicate
        if (_pickedCodes.includes(rawCode)) {
            return { ok: false, err: t('newBatch.nafErrorDuplicate') };
        }

        // Cap
        if (_pickedCodes.length >= MAX_NAF_CHIPS) {
            return { ok: false, err: t('newBatch.nafErrorCap') };
        }

        // Section letters must stand alone
        const isSection = rawCode.length === 1 && /[A-U]/.test(rawCode);
        if (isSection && _pickedCodes.length > 0) {
            return { ok: false, err: t('newBatch.nafErrorSectionAlone') };
        }
        if (!isSection && _pickedCodes.some(c => c.length === 1)) {
            return { ok: false, err: t('newBatch.nafErrorSectionAlone') };
        }

        // Same-sector-group check (only when adding 2nd+ leaf code)
        if (_pickedCodes.length > 0) {
            const candidate = [..._pickedCodes, rawCode];
            if (!allSameSectorGroup(candidate)) {
                return { ok: false, err: t('newBatch.nafErrorSameCategory') };
            }
        }

        _pickedCodes.push(rawCode);
        _pickedCodesRef.value = [..._pickedCodes];
        scheduleSaveDraft();
        _pickedLabels[rawCode] = label || _nafLabelByCode[rawCode] || rawCode;
        if (!_isAutoPick) {
            _userEditedPicker = true;
        }
        renderChips();
        _updatePickerVisualHint();
        return { ok: true };
    }

    function removeCode(code) {
        const idx = _pickedCodes.indexOf(code);
        if (idx >= 0) {
            _pickedCodes.splice(idx, 1);
            _pickedCodesRef.value = [..._pickedCodes];
            scheduleSaveDraft();
            delete _pickedLabels[code];
            renderChips();
            _userEditedPicker = true;
        }
    }

    function renderNafDropdown(query) {
        const q = (query || '').toLowerCase().trim();
        // Hide codes already picked from the dropdown
        const matches = q
            ? _allNafEntries.filter(e => e.label.toLowerCase().includes(q) && !_pickedCodes.includes(e.code)).slice(0, 30)
            : [];
        nafDropdown.innerHTML = matches.map(m =>
            `<div class="naf-picker-option" data-code="${escapeHtml(m.code)}" data-label="${escapeHtml(m.label)}">${escapeHtml(m.label)}</div>`
        ).join('');
        nafDropdown.style.display = matches.length ? 'block' : 'none';
    }

    nafInput.addEventListener('input', () => { renderNafDropdown(nafInput.value); _updatePickerVisualHint(); });
    nafInput.addEventListener('focus', () => renderNafDropdown(nafInput.value));

    // Paste handler: split on comma/semicolon/newline and try each.
    // Note: the regex /^[A-U]$|^\d{2}$|^\d{2}\.\d{2}[A-Z]$/ runs on `p`, which is
    // already trimmed by the .map(s => s.trim()) above. No leading/trailing
    // whitespace will reach the regex.
    nafInput.addEventListener('paste', (e) => {
        const text = (e.clipboardData || window.clipboardData).getData('text');
        if (!text || !text.includes(',')) return;  // Let normal paste happen for single value
        e.preventDefault();
        const parts = text.split(/[,;\n]+/).map(s => s.trim()).filter(Boolean);
        let firstError = null;
        for (const p of parts) {
            // Match by code prefix first (p is already trimmed), then by label contains
            const codeMatch = p.toUpperCase().match(/^[A-U]$|^\d{2}$|^\d{2}\.\d{2}[A-Z]$/);
            let targetCode = null;
            if (codeMatch && _nafLabelByCode[codeMatch[0]]) {
                targetCode = codeMatch[0];
            } else {
                const entry = _allNafEntries.find(en => en.label.toLowerCase().includes(p.toLowerCase()));
                if (entry) targetCode = entry.code;
            }
            if (!targetCode) continue;
            const res = tryAddCode(targetCode, _nafLabelByCode[targetCode]);
            if (!res.ok && res.err && !firstError) firstError = res.err;
        }
        if (firstError) showNafError(firstError);
        nafInput.value = '';
        nafDropdown.style.display = 'none';
    });

    nafDropdown.addEventListener('click', (e) => {
        const opt = e.target.closest('.naf-picker-option');
        if (!opt) return;
        const res = tryAddCode(opt.dataset.code, opt.dataset.label);
        if (!res.ok && res.err) showNafError(res.err);
        nafInput.value = '';
        _updatePickerVisualHint();
        nafDropdown.style.display = 'none';
        nafInput.focus();
    });

    // Enter key: attempt to add exact-code match if input equals a code
    nafInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            e.preventDefault();
            const val = nafInput.value.trim().toUpperCase();
            if (val && _nafLabelByCode[val]) {
                const res = tryAddCode(val, _nafLabelByCode[val]);
                if (!res.ok && res.err) showNafError(res.err);
                nafInput.value = '';
                nafDropdown.style.display = 'none';
            }
        }
    });

    // Chip remove handler (delegated)
    nafChips.addEventListener('click', (e) => {
        const btn = e.target.closest('.naf-chip-remove');
        if (!btn) return;
        const chip = btn.closest('.naf-chip');
        if (chip && chip.dataset.code) removeCode(chip.dataset.code);
    });

    // ── NAF sibling chip click handler ────────────────────────────────
    if (_siblingsChips) {
        _siblingsChips.addEventListener('click', (e) => {
            const more = e.target.closest('.naf-siblings-more');
            if (more) {
                _siblingsExpanded = true;
                renderSiblingsRow();
                return;
            }
            const less = e.target.closest('.naf-siblings-less');
            if (less) {
                _siblingsExpanded = false;
                renderSiblingsRow();
                return;
            }
            const chip = e.target.closest('.naf-sibling-chip');
            if (chip && chip.dataset.code) {
                const res = tryAddCode(chip.dataset.code, _nafLabelByCode[chip.dataset.code]);
                if (!res.ok && res.err) showNafError(res.err);
            }
        });
    }

    // ── NAF variant chip click handler ────────────────────────────────
    // Clicking a variant pill spawns a NEW query row prefilled with
    // `<phrasing> <dept>`. Department is extracted via parseDeptHint (module-scope).
    if (_variantsChips) {
        _variantsChips.addEventListener('click', (e) => {
            const chip = e.target.closest('.naf-variant-chip');
            if (!chip || !chip.dataset.phrasing) return;
            const phrasing = chip.dataset.phrasing;

            // Extract dept from existing query inputs using module-scope parseDeptHint.
            const inputs = [
                document.querySelector('.gemini-query-input.primary'),
                ...document.querySelectorAll('#additional-queries .gemini-query-input')
            ].filter(Boolean);
            const queries = inputs.map(i => i.value.trim()).filter(q => q.length > 0);
            let department = '';
            for (const q of queries) {
                const hint = parseDeptHint(q);
                if (hint) { department = hint; break; }
            }
            const value = department ? `${phrasing} ${department}` : phrasing;

            const newRow = createSubordinateQueryRow(value);
            document.getElementById('additional-queries').appendChild(newRow);
            updateSummary();
            const newInput = newRow.querySelector('.gemini-query-input');
            if (newInput) newInput.focus();
        });
    }

    document.addEventListener('click', (e) => {
        if (nafPicker && !nafPicker.contains(e.target)) {
            nafDropdown.style.display = 'none';
        }
    });

    // ── Subordinate query row creation ────────────────────────────────
    function createSubordinateQueryRow(value = '') {
        const row = document.createElement('div');
        row.className = 'gemini-query-row';
        row.innerHTML = `
            <input type="text"
                class="gemini-query-input"
                placeholder="${t('newBatch.queryPlaceholderSecondary')}"
                autocomplete="off"
                value="${escapeHtml(value)}">
            <button type="button" class="gemini-query-remove" title="${t('newBatch.removeQuery')}">✕</button>
        `;
        row.querySelector('.gemini-query-remove').addEventListener('click', () => {
            const warning = row.nextElementSibling;
            if (warning && warning.classList.contains('gemini-inline-warning')) warning.remove();
            row.remove();
            updateSummary();
            scheduleSaveDraft();
        });
        row.querySelector('.gemini-query-input').addEventListener('input', () => {
            validateSingleRow(row);
            updateSummary();
            scheduleSaveDraft();
        });
        return row;
    }

    document.getElementById('btn-add-query').addEventListener('click', () => {
        const newRow = createSubordinateQueryRow();
        document.getElementById('additional-queries').appendChild(newRow);
        newRow.querySelector('.gemini-query-input').focus();
        updateSummary();
        scheduleSaveDraft();
    });

    // ── Inline warnings (per row) ─────────────────────────────────────
    function validateSingleRow(row) {
        if (row.hasAttribute('data-bulk-source')) return;  // skip bulk summary rows
        const existingWarning = row.nextElementSibling;
        if (existingWarning && existingWarning.classList.contains('gemini-inline-warning')) {
            existingWarning.remove();
        }
        const input = row.querySelector('.gemini-query-input');
        const value = input.value.trim();
        if (!value) return;
        const words = value.split(/\s+/);
        if (words.length === 1 && !/^\d{2,5}$/.test(words[0])) {
            const warning = document.createElement('div');
            warning.className = 'gemini-inline-warning';
            warning.textContent = '⚠️ ' + t('newBatch.warningInline');
            row.insertAdjacentElement('afterend', warning);
        }
    }

    // ── Safeguard: check for overly broad queries ─────────────────────
    function validateQueries(queries) {
        const warnings = [];
        for (const q of queries) {
            const words = q.trim().split(/\s+/);
            // Single word without location = too broad
            if (words.length === 1 && !/^\d{2,5}$/.test(words[0])) {
                warnings.push(`"${q}" — ${t('newBatch.warningInline')}`);
            }
        }
        return warnings;
    }

    // ── Duration estimator ────────────────────────────────────────────
    function _updateDurationEstimator() {
        const inputs = [
            document.querySelector('.gemini-query-input.primary'),
            ...document.querySelectorAll('#additional-queries .gemini-query-input')
        ].filter(Boolean);
        let nQueries = inputs.map(i => (i.value || '').trim()).filter(q => q.length > 0).length;
        if (_cityPickerBulkSelection) nQueries += _cityPickerBulkSelection.communes.length;

        const el = document.getElementById('duration-estimator');
        const txt = document.getElementById('duration-estimator-text');
        if (!el || !txt) return;
        if (!nQueries) { el.style.display = 'none'; return; }

        const avgPerQuery = (_timingBaseline && _timingBaseline.avg_min_per_query) ? _timingBaseline.avg_min_per_query : 18;
        const expectedMin = Math.round(nQueries * avgPerQuery);

        const perCap = _selectedTimeCap > 0 ? _selectedTimeCap : null;
        const totalCap = _selectedTotalCap > 0 ? _selectedTotalCap : null;
        let upperMin = null;
        if (perCap && totalCap) upperMin = Math.min(perCap * nQueries, totalCap);
        else if (perCap) upperMin = perCap * nQueries;
        else if (totalCap) upperMin = totalCap;

        if (upperMin) {
            txt.textContent = t('newBatch.durationEstimator.withCap', { n: nQueries, est: expectedMin, cap: upperMin });
        } else {
            txt.textContent = t('newBatch.durationEstimator.noCap', { n: nQueries, est: expectedMin });
        }
        el.style.display = 'block';
    }

    // ── Discover-all button state ─────────────────────────────────────
    function _updateDiscoverAllButtonState() {
        const btn = document.getElementById('city-picker-discover-all');
        if (!btn) return;
        btn.disabled = !_pickedCodesRef.value || _pickedCodesRef.value.length === 0;
    }

    // ── Discover-all modal close helper ──────────────────────────────
    function closeDiscoverAllModal() {
        const existing = document.getElementById('discover-all-modal-overlay');
        if (existing) existing.remove();
    }

    // ── confirmBulkSelection ──────────────────────────────────────────
    function confirmBulkSelection(data, forceCovered) {
        const communes = forceCovered
            ? [...data.recommended.map(r => r.commune), ...data.skipped_already_covered.map(s => s.commune)]
            : data.recommended.map(r => r.commune);

        _cityPickerBulkSelection = {
            dept: data.dept,
            communes,
            metadata: {
                recommended: data.communes_recommended,
                skipped: data.communes_already_covered,
                expected_entities: data.expected_new_entities,
                estimated_minutes: data.estimated_minutes,
                dept_name: data.dept_name,
                communes_with_sirene_match: data.communes_with_sirene_match
            },
            forceCovered
        };

        const panel = document.getElementById('city-picker-panel');
        if (panel) panel.style.display = 'none';

        renderBulkSummaryRow();
        closeDiscoverAllModal();
        _updateDurationEstimator();
        scheduleSaveDraft();
    }

    // ── renderBulkSummaryRow ──────────────────────────────────────────
    function renderBulkSummaryRow() {
        if (!_cityPickerBulkSelection) return;
        document.querySelectorAll('#additional-queries [data-bulk-source]').forEach(el => el.remove());

        const meta = _cityPickerBulkSelection.metadata;
        const div = document.createElement('div');
        div.className = 'gemini-query-row bulk-summary';
        div.setAttribute('data-bulk-dept', _cityPickerBulkSelection.dept);
        div.setAttribute('data-bulk-source', 'true');
        div.innerHTML = `
          <span class="bulk-icon">🌍</span>
          <span class="bulk-label">${t('newBatch.cityPicker.bulkSummary', {
              dept: _cityPickerBulkSelection.dept,
              communes: _cityPickerBulkSelection.communes.length,
              entities: meta.expected_entities,
              minutes: meta.estimated_minutes
          })}</span>
          <button type="button" class="btn-bulk-edit">${t('newBatch.cityPicker.bulkEdit')}</button>
          <button type="button" class="btn-bulk-remove" aria-label="${t('newBatch.cityPicker.bulkRemove')}">×</button>
        `;
        document.getElementById('additional-queries').appendChild(div);

        div.querySelector('.btn-bulk-edit').addEventListener('click', onBulkEdit);
        div.querySelector('.btn-bulk-remove').addEventListener('click', onBulkRemove);
    }

    // ── onBulkEdit + onBulkRemove ─────────────────────────────────────
    async function onBulkEdit() {
        if (!_cityPickerBulkSelection) return;
        const params = _pickedCodesRef.value.map(c => `naf_codes=${encodeURIComponent(c)}`).join('&');
        const resp = await fetch(`/api/departments/${_cityPickerBulkSelection.dept}/coverage?${params}`);
        if (resp.ok) {
            const data = await resp.json();
            renderDiscoverAllModal(data);
        }
    }

    function onBulkRemove() {
        _cityPickerBulkSelection = null;
        document.querySelectorAll('#additional-queries [data-bulk-source]').forEach(el => el.remove());
        const panel = document.getElementById('city-picker-panel');
        if (panel) panel.style.display = '';
        _updateDurationEstimator();
        scheduleSaveDraft();
    }

    // ── renderDiscoverAllModal ────────────────────────────────────────
    function renderDiscoverAllModal(data) {
        closeDiscoverAllModal();
        const overlay = document.createElement('div');
        overlay.id = 'discover-all-modal-overlay';
        overlay.className = 'discover-all-modal-overlay';
        overlay.innerHTML = `
          <div class="discover-all-modal">
            <h3>🌍 ${t('newBatch.discoverAll.title')} — ${data.dept} ${data.dept_name || ''}</h3>
            ${data.communes_with_sirene_match === 0 ? `
              <p>${t('newBatch.discoverAll.empty')}</p>
              <div class="discover-all-actions">
                <button type="button" class="btn btn-secondary" id="discover-all-cancel">${t('common.cancel')}</button>
              </div>
            ` : `
              <div class="discover-all-stats">
                <div>📊 <strong>${data.communes_with_sirene_match}</strong> ${t('newBatch.discoverAll.communesWithSirene')}</div>
                <div>✓ <strong>${data.communes_already_covered}</strong> ${t('newBatch.discoverAll.alreadyCovered')}</div>
                <div>→ <strong>${data.communes_recommended}</strong> ${t('newBatch.discoverAll.recommended')}</div>
                <div>≈ <strong>${data.expected_new_entities}</strong> ${t('newBatch.discoverAll.expectedEntities')}</div>
                <div>≈ <strong>${data.estimated_minutes} min</strong> ${t('newBatch.discoverAll.estimatedTime')}</div>
              </div>
              <div class="discover-all-actions">
                <button type="button" class="btn btn-primary" id="discover-all-launch">
                  ${t('newBatch.discoverAll.launchBulk')} (${data.communes_recommended})
                </button>
                <button type="button" class="btn btn-secondary" id="discover-all-cancel">${t('common.cancel')}</button>
              </div>
              ${data.communes_already_covered > 0 ? `
                <div class="discover-all-force-section">
                  <a href="#" class="discover-all-force-link" id="discover-all-force-link">
                    ${t('newBatch.discoverAll.forceRediscoverLink', { n: data.communes_already_covered })} ▼
                  </a>
                  <div class="discover-all-force-confirm" id="discover-all-force-confirm" style="display:none">
                    <p>${t('newBatch.discoverAll.forceConfirmText')}</p>
                    <button type="button" class="btn btn-warning" id="discover-all-force-yes">
                      ${t('newBatch.discoverAll.forceConfirmYes')}
                    </button>
                    <button type="button" class="btn btn-secondary" id="discover-all-force-cancel">
                      ${t('common.cancel')}
                    </button>
                  </div>
                </div>
              ` : ''}
            `}
          </div>
        `;

        document.body.appendChild(overlay);

        overlay.querySelector('#discover-all-cancel')?.addEventListener('click', closeDiscoverAllModal);
        overlay.querySelector('#discover-all-launch')?.addEventListener('click', () => confirmBulkSelection(data, false));
        overlay.querySelector('#discover-all-force-link')?.addEventListener('click', (e) => {
            e.preventDefault();
            const fc = overlay.querySelector('#discover-all-force-confirm');
            if (fc) fc.style.display = '';
        });
        overlay.querySelector('#discover-all-force-yes')?.addEventListener('click', () => confirmBulkSelection(data, true));
        overlay.querySelector('#discover-all-force-cancel')?.addEventListener('click', () => {
            const fc = overlay.querySelector('#discover-all-force-confirm');
            if (fc) fc.style.display = 'none';
        });
        overlay.addEventListener('click', (e) => {
            if (e.target === overlay) closeDiscoverAllModal();
        });
    }

    // ── onDiscoverAll ─────────────────────────────────────────────────
    async function onDiscoverAll() {
        const btn = document.getElementById('city-picker-discover-all');
        if (!btn || btn.disabled) return;
        if (!_cityPickerCurrentDept) return;

        btn.disabled = true;
        const origLabel = btn.innerHTML;
        btn.innerHTML = `<span class="liquid-spinner"></span>`;
        try {
            const params = _pickedCodesRef.value.map(c => `naf_codes=${encodeURIComponent(c)}`).join('&');
            const resp = await fetch(`/api/departments/${_cityPickerCurrentDept}/coverage?${params}`);
            if (!resp.ok) {
                console.error('coverage fetch failed', resp.status);
                return;
            }
            const data = await resp.json();
            renderDiscoverAllModal(data);
        } finally {
            btn.innerHTML = origLabel;
            _updateDiscoverAllButtonState();
        }
    }

    // ── Live summary ──────────────────────────────────────────────────
    function updateSummary() {
        _updateDurationEstimator();
        const inputs = [
            document.querySelector('.gemini-query-input.primary'),
            ...document.querySelectorAll('#additional-queries .gemini-query-input')
        ].filter(Boolean);
        const queries = inputs.map(i => i.value.trim()).filter(q => q.length > 0);
        const summaryEl = document.getElementById('gemini-summary-line');
        if (queries.length === 0) {
            summaryEl.textContent = t('newBatch.summaryEmptyState');
            return;
        }
        summaryEl.textContent = t('newBatch.summaryLiveExhaustive', {
            count: queries.length,
            plural: queries.length > 1 ? 's' : '',
        });
    }

    // Wire primary row validation and summary
    document.querySelector('.gemini-query-input.primary').addEventListener('input', () => {
        validateSingleRow(document.querySelector('.gemini-query-row.primary'));
        updateSummary();
        scheduleSaveDraft();
    });

    // ── "Comment ça marche" localStorage (private-mode safe) ─────────
    const howItWorks = document.getElementById('how-it-works');
    try {
        const seen = localStorage.getItem('fortress_howItWorks_seen');
        if (!seen) howItWorks.open = true;
        howItWorks.addEventListener('toggle', () => {
            try { localStorage.setItem('fortress_howItWorks_seen', 'true'); } catch (e) {}
        });
    } catch (e) { /* private mode */ }

    // ── Visual hint helper for unconfirmed NAF input ─────────────────
    function _updatePickerVisualHint() {
        const has = (nafInput.value || '').trim().length > 0;
        nafInput.classList.toggle('naf-picker-input--unconfirmed', has);
    }

    // ── City picker functions (Section 2.D – 2.J) ────────────────────

    async function handlePrimaryQueryChange(primaryQuery) {
        primaryQuery = primaryQuery.trim();
        if (!primaryQuery) {
            hideCityPanel();
            return;
        }
        const dept = parseDeptHint(primaryQuery);
        if (!dept) {
            hideCityPanel();
            return;
        }
        if (dept === _cityPickerCurrentDept) {
            _cityPickerCurrentPrimary = primaryQuery;
            return;
        }
        _cityPickerCurrentDept = dept;
        _cityPickerCurrentPrimary = primaryQuery;
        _cityPickerSelectedCities.clear();
        _cityPickerExpanded = false;
        // Clear any bulk selection when dept changes (G12 side-fix)
        _cityPickerBulkSelection = null;
        document.querySelectorAll('#additional-queries [data-city-picker-source]').forEach(el => el.remove());
        document.querySelectorAll('#additional-queries [data-bulk-source]').forEach(el => el.remove());

        if (!_cityPickerCache[dept]) {
            try {
                const resp = await fetch(`/api/departments/${dept}/communes`);
                if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
                const data = await resp.json();
                _cityPickerCache[dept] = data.communes || [];
            } catch (err) {
                console.error('city panel fetch failed', err);
                hideCityPanel();
                return;
            }
        }
        renderCityPanel();
    }

    function getDefaultVisibleCommunes(allCommunes) {
        return allCommunes.filter(c => !PARIS_ARR_RE.test(c.name)).slice(0, 20);
    }

    function applySearchFilter(communes) {
        const term = (document.getElementById('city-picker-search')?.value || '').trim().toLowerCase();
        if (!term) return communes;
        const normalize = s => s.toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '');
        const normalizedTerm = normalize(term);
        return communes.filter(c => normalize(c.name).includes(normalizedTerm));
    }

    function renderCityPanel() {
        const all = _cityPickerCache[_cityPickerCurrentDept] || [];
        const filteredDefault = getDefaultVisibleCommunes(all);
        const visible = _cityPickerExpanded ? all : filteredDefault;
        const filtered = applySearchFilter(visible);

        document.getElementById('city-picker-dept').textContent = _cityPickerCurrentDept;
        document.getElementById('city-picker-count').textContent = `(${all.length} communes)`;

        const chipsEl = document.getElementById('city-picker-chips');
        if (filtered.length === 0) {
            chipsEl.innerHTML = `<div class="city-picker-empty">${escapeHtml(t('newBatch.cityPicker.empty'))}</div>`;
        } else {
            chipsEl.innerHTML = filtered.map(c => `
                <button type="button"
                        class="city-chip ${_cityPickerSelectedCities.has(c.name) ? 'selected' : ''}"
                        data-commune="${escapeHtml(c.name)}">
                    ${escapeHtml(c.name)}
                    <span class="city-chip-count">(${c.company_count.toLocaleString('fr-FR')})</span>
                </button>
            `).join('');
        }

        const expandBtn = document.getElementById('city-picker-expand');
        if (!_cityPickerExpanded && all.length > filteredDefault.length) {
            expandBtn.style.display = '';
            document.getElementById('city-picker-more').textContent = (all.length - filteredDefault.length).toLocaleString('fr-FR');
        } else {
            expandBtn.style.display = 'none';
        }

        document.getElementById('city-picker-paris-hint').style.display = (_cityPickerCurrentDept === '75') ? '' : 'none';

        document.getElementById('city-picker-panel').style.display = '';
    }

    function addCityRow(commune) {
        const sector = extractSectorTokens(_cityPickerCurrentPrimary);
        const newQuery = sector ? `${sector} ${commune}` : commune;
        const newRow = createSubordinateQueryRow(newQuery);
        newRow.setAttribute('data-city-picker-source', commune);
        document.getElementById('additional-queries').appendChild(newRow);
    }

    function removeCityRow(commune) {
        const row = document.querySelector(
            `#additional-queries .gemini-query-row[data-city-picker-source="${CSS.escape(commune)}"]`
        );
        if (row) row.remove();
    }

    function onChipClick(e) {
        const chip = e.target.closest('.city-chip');
        if (!chip) return;
        const commune = chip.dataset.commune;
        if (_cityPickerSelectedCities.has(commune)) {
            _cityPickerSelectedCities.delete(commune);
            chip.classList.remove('selected');
            removeCityRow(commune);
        } else {
            _cityPickerSelectedCities.add(commune);
            chip.classList.add('selected');
            addCityRow(commune);
        }
    }

    function onClearAll() {
        for (const commune of Array.from(_cityPickerSelectedCities)) {
            removeCityRow(commune);
        }
        _cityPickerSelectedCities.clear();
        renderCityPanel();
    }

    function onExpand() {
        _cityPickerExpanded = true;
        renderCityPanel();
    }

    function onSearchInput() {
        renderCityPanel();
    }

    function wireCityPickerListeners() {
        const primaryInput = document.querySelector('.gemini-query-input.primary');
        if (primaryInput) {
            primaryInput.addEventListener('input', (e) => {
                clearTimeout(_cityPickerInputDebounce);
                _cityPickerInputDebounce = setTimeout(() => {
                    const q = e.target.value;
                    handlePrimaryQueryChange(q);
                    _autoPickFromPrimary(q);   // NEW
                }, 300);
            });
        }
        document.getElementById('city-picker-chips')?.addEventListener('click', onChipClick);
        document.getElementById('city-picker-search')?.addEventListener('input', onSearchInput);
        document.getElementById('city-picker-clear')?.addEventListener('click', onClearAll);
        document.getElementById('city-picker-expand')?.addEventListener('click', onExpand);
        document.getElementById('city-picker-discover-all')?.addEventListener('click', onDiscoverAll);

        // Initialize state from current primary input value (handles rerun/draft restore)
        if (primaryInput && primaryInput.value.trim()) {
            handlePrimaryQueryChange(primaryInput.value);
        }
    }

    // ── Launch button ─────────────────────────────────────────────────
    document.getElementById('btn-launch-batch').addEventListener('click', async () => {
        // Auto-confirm: user typed a valid NAF code in the input but didn't click the
        // suggestion or press Enter. Accept it now so the batch launches with the
        // intended filter. (Fix for customer false-positive incident 2026-04-30.)
        const _typed = (nafInput.value || '').trim().toUpperCase();
        if (_typed && _nafLabelByCode[_typed] && !_pickedCodes.includes(_typed)) {
            const res = tryAddCode(_typed, _nafLabelByCode[_typed]);
            if (!res.ok) {
                if (res.err) showNafError(res.err);
                return;
            }
            nafInput.value = '';
            _updatePickerVisualHint();
        } else if (_typed && !_nafLabelByCode[_typed]) {
            showNafError(t('newBatch.nafErrorUnconfirmedInvalid'));
            return;
        }

        const queryInputs = [
            document.querySelector('.gemini-query-input.primary'),
            ...document.querySelectorAll('#additional-queries .gemini-query-input')
        ].filter(Boolean);
        let queries = queryInputs
            .map(i => i.value.trim())
            .filter(q => q.length > 0);

        if (queries.length === 0 && !_cityPickerBulkSelection) {
            showToast(t('newBatch.errorNoQuery'), 'error');
            return;
        }

        // Safeguard: block if ALL queries are too broad (single word, no location) — skip for bulk
        if (!_cityPickerBulkSelection) {
            const warnings = validateQueries(queries);
            if (warnings.length === queries.length) {
                showToast(t('newBatch.errorAllBroad'), 'error');
                return;
            }
        }

        const btn = document.getElementById('btn-launch-batch');

        // Extract sector name from first query (first word)
        const firstQuery = queries[0] || '';
        const sector = firstQuery.split(/\s+/)[0] || 'RECHERCHE';

        // Try to extract department from queries using module-scope parseDeptHint.
        // Priority: 2-digit code → 5-digit postal → department/city name → 'FR'
        let department = '';
        for (const q of queries) {
            const hint = parseDeptHint(q);
            if (hint) { department = hint; break; }
        }
        if (!department) department = 'FR';

        const _strictToggleEl = document.getElementById('strict-naf-toggle');

        // Refresh tracking from current primary input value
        const _primaryEl = document.querySelector('.gemini-query-input.primary');
        if (_primaryEl) _cityPickerCurrentPrimary = _primaryEl.value.trim();

        // Bulk expansion: replace queries with commune-expanded set
        let _bulkMeta = null;
        let _noWidenQueries = null;
        if (_cityPickerBulkSelection) {
            const sectorPrefix = extractSectorTokens(_cityPickerCurrentPrimary || firstQuery);
            const bulkExpanded = _cityPickerBulkSelection.communes.map(c =>
                sectorPrefix ? `${sectorPrefix} ${c}` : c
            );
            queries = bulkExpanded;
            _noWidenQueries = bulkExpanded;
            _bulkMeta = _cityPickerBulkSelection.metadata;
        }

        const payload = {
            sector: sector.toUpperCase(),
            department: department || '00',
            mode: 'discovery',
            strategy: 'maps',
            search_queries: queries,
            naf_codes: _pickedCodes.length > 0 ? [..._pickedCodes] : null,
            time_cap_per_query_min: _selectedTimeCap > 0 ? _selectedTimeCap : null,
            time_cap_total_min: _selectedTotalCap > 0 ? _selectedTotalCap : null,
            strict_naf: _strictToggleEl ? Boolean(_strictToggleEl.checked && !_strictToggleEl.disabled) : false,
            entity_cap_confirmed: _selectedEntityCap > 0 ? _selectedEntityCap : null,
            bulk_discovery_meta: _bulkMeta || null,
        };

        // NEW — include the primary in no_widen_queries IF user picked any cities (non-bulk)
        if (!_cityPickerBulkSelection && _cityPickerSelectedCities.size > 0 && _cityPickerCurrentPrimary) {
            payload.no_widen_queries = [_cityPickerCurrentPrimary];
        } else if (_noWidenQueries) {
            payload.no_widen_queries = _noWidenQueries;
        }

        btn.disabled = true;
        btn.innerHTML = t('newBatch.launching');

        document.getElementById('gemini-summary-line').innerHTML = `
            <div style="color:var(--warning); font-weight:700">${t('newBatch.sendingConfig')}</div>
        `;

        try {
            const result = await runBatch(payload);

            if (result && result._ok && result.status === 'launched') {
                document.getElementById('gemini-summary-line').innerHTML = `
                    <div style="color:var(--success); font-weight:700; margin-bottom:var(--space-sm)">
                        ${t('newBatch.launched')}
                    </div>
                    <div style="font-size:var(--font-sm); color:var(--text-secondary); line-height:1.8">
                        ${t('newBatch.successId', { id: escapeHtml(result.batch_id || '—') })}<br>
                        ${t('newBatch.successStatus')}
                    </div>
                    <div style="margin-top:var(--space-md); font-size:var(--font-xs); color:var(--text-muted)">
                        ${t('newBatch.redirecting')}
                    </div>
                `;

                clearDraft();
                setTimeout(() => {
                    window.location.hash = `#/monitor/${encodeURIComponent(result.batch_id)}`;
                }, 3000);
            } else {
                // Queue offer: server told us a batch is already running and we can queue
                if (result && result._status === 409 && result.can_queue === true) {
                    const ok = await showConfirmModal({
                        title: t('newBatch.queueModalTitle'),
                        body: t('newBatch.queueModalBody'),
                        confirmLabel: t('newBatch.queueModalConfirm'),
                        danger: false,
                    });
                    if (ok) {
                        const queuedResult = await runBatch({ ...payload, queue: true });
                        if (queuedResult && queuedResult._ok && queuedResult.queued) {
                            document.getElementById('gemini-summary-line').innerHTML = `
                                <div style="color:var(--success); font-weight:700; margin-bottom:var(--space-sm)">
                                    ${t('newBatch.queuedSuccess', { position: queuedResult.queue_position || '?' })}
                                </div>
                                <div style="font-size:var(--font-sm); color:var(--text-secondary)">
                                    ${t('newBatch.queuedSuccessDetail', { id: escapeHtml(queuedResult.batch_id || '—') })}
                                </div>
                            `;
                            try { sessionStorage.removeItem('fortress_new_batch_draft'); } catch {}
                            setTimeout(() => { window.location.hash = `#/monitor/${encodeURIComponent(queuedResult.batch_id)}`; }, 3000);
                            return;
                        } else {
                            const qErr = extractApiError(queuedResult);
                            document.getElementById('gemini-summary-line').innerHTML = `
                                <div style="color:var(--danger); font-weight:700">${t('newBatch.errorLaunch', { message: escapeHtml(qErr) })}</div>
                            `;
                            btn.disabled = false;
                            btn.innerHTML = t('newBatch.launchHero');
                            return;
                        }
                    } else {
                        document.getElementById('gemini-summary-line').innerHTML = `
                            <div style="color:var(--text-muted); font-size:var(--font-sm)">${t('newBatch.queueCancelled')}</div>
                        `;
                        btn.disabled = false;
                        btn.innerHTML = t('newBatch.launchHero');
                        return;
                    }
                }
                const errorMsg = extractApiError(result);
                document.getElementById('gemini-summary-line').innerHTML = `
                    <div style="color:var(--danger); font-weight:700">${t('newBatch.errorLaunch', { message: escapeHtml(errorMsg) })}</div>
                `;
                btn.disabled = false;
                btn.innerHTML = t('newBatch.launchHero');
            }
        } catch (err) {
            document.getElementById('gemini-summary-line').innerHTML = `
                <div style="color:var(--danger); font-weight:700">${t('newBatch.errorNetwork', { message: escapeHtml(err.message) })}</div>
            `;
            btn.disabled = false;
            btn.innerHTML = t('newBatch.launchHero');
        }
    });

    // ── Pre-fill priority: rerun (sessionStorage) > draft (sessionStorage) ──
    const prefillRaw = sessionStorage.getItem('fortress_expansion_prefill');
    if (prefillRaw) {
        sessionStorage.removeItem('fortress_expansion_prefill');
        // Rerun is an explicit user action — clear any stale draft
        clearDraft();
        try {
            const prefill = JSON.parse(prefillRaw);
            if (prefill.queries && Array.isArray(prefill.queries) && prefill.queries.length > 0) {
                const primaryInput = document.querySelector('.gemini-query-input.primary');
                if (primaryInput && prefill.queries[0]) primaryInput.value = prefill.queries[0];
                for (let i = 1; i < prefill.queries.length; i++) {
                    const newRow = createSubordinateQueryRow(prefill.queries[i]);
                    document.getElementById('additional-queries').appendChild(newRow);
                }
            }
            updateSummary();
        } catch {}
    } else {
        // E3: try to restore draft
        try {
            const draftRaw = sessionStorage.getItem(DRAFT_KEY);
            if (draftRaw) {
                const draft = JSON.parse(draftRaw);
                if (draft && draft.v === 1) {
                    if (Array.isArray(draft.queries) && draft.queries.length > 0) {
                        const primaryInput = document.querySelector('.gemini-query-input.primary');
                        if (primaryInput && draft.queries[0]) primaryInput.value = draft.queries[0];
                        for (let i = 1; i < draft.queries.length; i++) {
                            const newRow = createSubordinateQueryRow(draft.queries[i]);
                            document.getElementById('additional-queries').appendChild(newRow);
                        }
                    }
                    if (Array.isArray(draft.naf_codes)) {
                        for (const code of draft.naf_codes) {
                            const lbl = _nafLabelByCode[code];
                            if (lbl) tryAddCode(code, lbl);
                        }
                    }
                    if (typeof draft.time_cap_min === 'number') {
                        const pill = document.querySelector(`#time-cap-pills .cap-pill[data-cap-min="${draft.time_cap_min}"]`);
                        if (pill) {
                            document.querySelectorAll('#time-cap-pills .cap-pill').forEach(x => x.classList.remove('active'));
                            pill.classList.add('active');
                            _selectedTimeCap = draft.time_cap_min;
                            _timeCapRef.value = _selectedTimeCap;
                        }
                    }
                    if (typeof draft.entity_cap === 'number') {
                        const ecpill = document.querySelector(`#entity-cap-pills .cap-pill[data-entity-cap="${draft.entity_cap}"]`);
                        if (ecpill) {
                            document.querySelectorAll('#entity-cap-pills .cap-pill').forEach(x => x.classList.remove('active'));
                            ecpill.classList.add('active');
                            _selectedEntityCap = draft.entity_cap;
                        }
                    }
                    // Soft banner
                    const sumLine = document.getElementById('gemini-summary-line');
                    if (sumLine) {
                        sumLine.innerHTML = `
                            <div style="color:var(--text-muted); font-size:var(--font-sm)">
                                ${t('newBatch.draftRestored')}
                                <button type="button" id="btn-clear-draft" style="margin-left:8px; background:none; border:none; color:var(--accent); cursor:pointer; text-decoration:underline; font-size:inherit">${t('newBatch.draftClear')}</button>
                            </div>
                        `;
                        const clearBtn = document.getElementById('btn-clear-draft');
                        if (clearBtn) clearBtn.addEventListener('click', () => {
                            clearDraft();
                            // Reset all inputs
                            const primaryInput = document.querySelector('.gemini-query-input.primary');
                            if (primaryInput) primaryInput.value = '';
                            document.getElementById('additional-queries').innerHTML = '';
                            // Clear NAF chips
                            while (_pickedCodes.length > 0) _pickedCodes.pop();
                            _pickedCodesRef.value = [];
                            renderChips();
                            // Reset time cap to default 60
                            document.querySelectorAll('#time-cap-pills .cap-pill').forEach(x => x.classList.remove('active'));
                            const def = document.querySelector('#time-cap-pills .cap-pill[data-cap-min="60"]');
                            if (def) def.classList.add('active');
                            _selectedTimeCap = 60;
                            _timeCapRef.value = 60;
                            sumLine.innerHTML = t('newBatch.summaryEmptyState');
                            updateSummary();
                        });
                    }
                    updateSummary();
                }
            }
        } catch (_e) { /* private mode or malformed */ }
    }

    // ── Wire city picker listeners (after draft/rerun restoration) ───
    wireCityPickerListeners();

    // ── Pre-load timing baseline for estimator ─────────────────────
    getTimingBaseline().then(() => { _updateDurationEstimator(); });
}
