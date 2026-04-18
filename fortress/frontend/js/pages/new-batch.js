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

import { escapeHtml, showToast } from '../components.js';
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
                    <div id="additional-queries"></div>
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
                    <div class="naf-picker" id="naf-picker">
                        <input type="text" class="naf-picker-input" id="naf-picker-input"
                               placeholder="${t('newBatch.nafPlaceholder')}" autocomplete="off">
                        <input type="hidden" id="naf-picker-value">
                        <div class="naf-picker-dropdown" id="naf-picker-dropdown"></div>
                    </div>
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

    // ── NAF picker ────────────────────────────────────────────────────
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

    const nafInput = document.getElementById('naf-picker-input');
    const nafValue = document.getElementById('naf-picker-value');
    const nafDropdown = document.getElementById('naf-picker-dropdown');

    function renderNafDropdown(query) {
        const q = (query || '').toLowerCase().trim();
        const matches = q
            ? _allNafEntries.filter(e => e.label.toLowerCase().includes(q)).slice(0, 30)
            : [];
        nafDropdown.innerHTML = matches.map(m =>
            `<div class="naf-picker-option" data-code="${escapeHtml(m.code)}" data-label="${escapeHtml(m.label)}">${escapeHtml(m.label)}</div>`
        ).join('');
        nafDropdown.style.display = matches.length ? 'block' : 'none';
    }

    nafInput.addEventListener('input', () => renderNafDropdown(nafInput.value));
    nafInput.addEventListener('focus', () => renderNafDropdown(nafInput.value));
    nafDropdown.addEventListener('click', (e) => {
        const opt = e.target.closest('.naf-picker-option');
        if (!opt) return;
        nafInput.value = opt.dataset.label;
        nafValue.value = opt.dataset.code;
        nafDropdown.style.display = 'none';
    });
    document.addEventListener('click', (e) => {
        const picker = document.getElementById('naf-picker');
        if (picker && !picker.contains(e.target)) {
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
        });
        row.querySelector('.gemini-query-input').addEventListener('input', () => {
            validateSingleRow(row);
            updateSummary();
        });
        return row;
    }

    document.getElementById('btn-add-query').addEventListener('click', () => {
        const newRow = createSubordinateQueryRow();
        document.getElementById('additional-queries').appendChild(newRow);
        newRow.querySelector('.gemini-query-input').focus();
        updateSummary();
    });

    // ── Inline warnings (per row) ─────────────────────────────────────
    function validateSingleRow(row) {
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

    // ── Live summary ──────────────────────────────────────────────────
    function updateSummary() {
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

    // ── Launch button ─────────────────────────────────────────────────
    document.getElementById('btn-launch-batch').addEventListener('click', async () => {
        const queryInputs = [
            document.querySelector('.gemini-query-input.primary'),
            ...document.querySelectorAll('#additional-queries .gemini-query-input')
        ].filter(Boolean);
        const queries = queryInputs
            .map(i => i.value.trim())
            .filter(q => q.length > 0);

        if (queries.length === 0) {
            showToast(t('newBatch.errorNoQuery'), 'error');
            return;
        }

        // Safeguard: block if ALL queries are too broad (single word, no location)
        const warnings = validateQueries(queries);
        if (warnings.length === queries.length) {
            showToast(t('newBatch.errorAllBroad'), 'error');
            return;
        }

        const btn = document.getElementById('btn-launch-batch');

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
            naf_code: nafValue.value || null,
        };

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

                setTimeout(() => {
                    window.location.hash = `#/monitor/${encodeURIComponent(result.batch_id)}`;
                }, 3000);
            } else {
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

    // ── Pre-fill from expansion suggestion (sessionStorage) ──────────
    const prefillRaw = sessionStorage.getItem('fortress_expansion_prefill');
    if (prefillRaw) {
        sessionStorage.removeItem('fortress_expansion_prefill');
        try {
            const prefill = JSON.parse(prefillRaw);
            if (prefill.queries && Array.isArray(prefill.queries) && prefill.queries.length > 0) {
                const primaryInput = document.querySelector('.gemini-query-input.primary');
                if (primaryInput && prefill.queries[0]) {
                    primaryInput.value = prefill.queries[0];
                }
                for (let i = 1; i < prefill.queries.length; i++) {
                    const newRow = createSubordinateQueryRow(prefill.queries[i]);
                    document.getElementById('additional-queries').appendChild(newRow);
                }
            }
            updateSummary();
        } catch {}
    }
}
