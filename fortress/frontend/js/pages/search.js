/**
 * Base SIRENE Page — Search the raw 14.7M French company database
 *
 * Uses /api/sirene/search — queries the full companies table directly.
 * Shows SIRENE data only (no enriched fields like phone/email/website).
 * For enriched data, use the Dashboard.
 */

import { searchSirene, checkHealth, extractApiError } from '../api.js';
import { escapeHtml, showToast } from '../components.js';
import { DEPARTMENTS } from '../constants.js';

export async function renderSearch(container) {
    // Extract query from hash if present
    const hashParts = window.location.hash.split('?');
    const params = new URLSearchParams(hashParts[1] || '');
    const initialQuery = params.get('q') || '';

    // Current filter + pagination state
    let currentDepartment = '';
    let currentNafCode = '';
    let currentOffset = 0;
    const PAGE_SIZE = 50;

    container.innerHTML = `
        <h1 class="page-title">🏦 Base SIRENE</h1>
        <p class="page-subtitle">Rechercher dans la base de données de 14.7M d'entreprises françaises — données brutes du registre SIRENE</p>

        <div style="margin-bottom: var(--space-xl); max-width: 700px">
            <!-- Search Input -->
            <div style="position:relative; margin-bottom: var(--space-lg)">
                <span style="position:absolute; left:14px; top:50%; transform:translateY(-50%); color:var(--text-muted); font-size:1.1rem">🔍</span>
                <input type="text" id="search-input"
                    value="${escapeHtml(initialQuery)}"
                    placeholder="Rechercher par Nom, SIREN (9 chiffres), ou Code NAF..."
                    style="width:100%; padding:var(--space-md) var(--space-xl); padding-left:40px;
                           background:var(--bg-input); border:1px solid var(--border-default);
                           border-radius:var(--radius); color:var(--text-primary);
                           font-family:var(--font-family); font-size:var(--font-md); outline:none;
                           transition:border-color var(--transition-fast), box-shadow var(--transition-fast);"
                    onfocus="this.style.borderColor='var(--accent)'; this.style.boxShadow='0 0 0 3px var(--accent-subtle)'"
                    onblur="this.style.borderColor='var(--border-default)'; this.style.boxShadow='none'"
                    autocomplete="off"
                >
            </div>

            <!-- Filter Panel -->
            <div class="filter-panel">
                <div class="filter-group">
                    <label class="filter-label" for="filter-dept">📍 Département</label>
                    <select class="sort-select" id="filter-dept">
                        <option value="">Tous les départements</option>
                        ${DEPARTMENTS.map(([code, name]) =>
        `<option value="${code}">${code} — ${escapeHtml(name)}</option>`
    ).join('')}
                    </select>
                </div>
                <div class="filter-group">
                    <label class="filter-label" for="filter-naf">📋 Code NAF</label>
                    <input type="text" id="filter-naf" class="sort-select"
                        placeholder="ex: 49.41, 56.10..."
                        style="background:var(--bg-input); border:1px solid var(--border-default);
                               border-radius:var(--radius-sm); color:var(--text-primary);
                               font-family:var(--font-family); padding:var(--space-sm) var(--space-md);
                               font-size:var(--font-sm);"
                    >
                </div>
            </div>

            <!-- Active Filter Pills -->
            <div class="filter-pills" id="filter-pills" style="display:none"></div>
        </div>

        <div id="search-results"></div>
    `;

    const input = document.getElementById('search-input');
    const resultsEl = document.getElementById('search-results');
    const filterDept = document.getElementById('filter-dept');
    const filterNaf = document.getElementById('filter-naf');
    const filterPills = document.getElementById('filter-pills');
    let debounceTimer;

    // ── Update filter pills ──────────────────────────────────────
    function updateFilterPills() {
        const pills = [];
        if (currentDepartment) {
            const deptName = DEPARTMENTS.find(d => d[0] === currentDepartment);
            const label = deptName ? `${deptName[0]} — ${deptName[1]}` : currentDepartment;
            pills.push(`<span class="filter-pill" data-filter="dept">📍 ${escapeHtml(label)} <button class="filter-pill-remove" data-clear="dept">✕</button></span>`);
        }
        if (currentNafCode) {
            pills.push(`<span class="filter-pill" data-filter="naf">📋 NAF: ${escapeHtml(currentNafCode)} <button class="filter-pill-remove" data-clear="naf">✕</button></span>`);
        }

        if (pills.length > 0) {
            filterPills.style.display = 'flex';
            filterPills.innerHTML = pills.join('');

            filterPills.querySelectorAll('.filter-pill-remove').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const target = e.target.dataset.clear;
                    if (target === 'dept') {
                        currentDepartment = '';
                        filterDept.value = '';
                    } else if (target === 'naf') {
                        currentNafCode = '';
                        filterNaf.value = '';
                    }
                    updateFilterPills();
                    const q = input.value.trim();
                    if (q) doSearch(q);
                });
            });
        } else {
            filterPills.style.display = 'none';
            filterPills.innerHTML = '';
        }
    }

    // ── Search function ──────────────────────────────────────────
    async function doSearch(q, offset = 0) {
        if (!q || q.length < 2) {
            resultsEl.innerHTML = '';
            return;
        }

        currentOffset = offset;
        resultsEl.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

        const data = await searchSirene(q, {
            limit: PAGE_SIZE,
            offset: currentOffset,
            department: currentDepartment,
            nafCode: currentNafCode,
        });

        // API error (503, 408, 500, network failure)
        if (!data || (data._status && !data._ok)) {
            const errorMsg = extractApiError(data);
            resultsEl.innerHTML = `
                <div class="error-state text-center" style="padding:var(--space-2xl)">
                    <div style="font-size:3rem; margin-bottom:var(--space-lg)">🔌</div>
                    <div style="font-size:var(--font-md); color:var(--text-secondary); margin-bottom:var(--space-lg)">
                        ${escapeHtml(errorMsg)}
                    </div>
                    <button id="retry-search" class="btn btn-primary" style="margin-top:var(--space-md)">
                        🔄 Réessayer
                    </button>
                </div>
            `;
            document.getElementById('retry-search').addEventListener('click', async (e) => {
                e.target.disabled = true;
                e.target.textContent = '⏳ Vérification...';
                const health = await checkHealth();
                if (health.ok) {
                    doSearch(q, offset);
                } else {
                    showToast('Le serveur est toujours inaccessible.', 'error');
                    e.target.disabled = false;
                    e.target.textContent = '🔄 Réessayer';
                }
            });
            return;
        }

        if (!data.results || data.results.length === 0) {
            resultsEl.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">🔍</div>
                    <div class="empty-state-text">Aucun résultat pour "${escapeHtml(q)}"</div>
                    <p style="color: var(--text-muted)">Essayez un autre terme, un numéro SIREN (9 chiffres) ou un code NAF</p>
                </div>
            `;
            return;
        }

        const total = data.total || data.count || data.results.length;
        const currentPage = Math.floor(currentOffset / PAGE_SIZE) + 1;
        const totalPages = Math.ceil(total / PAGE_SIZE);
        const hasNext = currentOffset + PAGE_SIZE < total;
        const hasPrev = currentOffset > 0;

        resultsEl.innerHTML = `
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:var(--space-lg)">
                <p style="font-size:var(--font-sm); color:var(--text-secondary); margin:0">
                    ${total} résultat${total > 1 ? 's' : ''} pour "${escapeHtml(q)}"
                    ${totalPages > 1 ? `— page ${currentPage}/${totalPages}` : ''}
                </p>
            </div>

            <!-- Results Table -->
            <div class="card" style="overflow-x:auto">
                <table style="width:100%; border-collapse:collapse; font-size:var(--font-sm)">
                    <thead>
                        <tr>
                            <th style="text-align:left; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase; white-space:nowrap">SIREN</th>
                            <th style="text-align:left; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase">Dénomination</th>
                            <th style="text-align:left; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase; white-space:nowrap">Code NAF</th>
                            <th style="text-align:left; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase">Ville</th>
                            <th style="text-align:left; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase; white-space:nowrap">Dépt</th>
                            <th style="text-align:left; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase">Forme</th>
                            <th style="text-align:center; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase">Statut</th>
                            <th style="text-align:center; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase">Enrichi</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${data.results.map(c => `
                            <tr style="cursor:pointer; transition:background 0.15s" 
                                onmouseover="this.style.background='var(--bg-hover)'"
                                onmouseout="this.style.background=''"
                                onclick="window.location.hash='#/company/${c.siren}'">
                                <td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); font-family:var(--font-mono); color:var(--accent); font-weight:600; white-space:nowrap">${escapeHtml(c.siren)}</td>
                                <td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); color:var(--text-primary); font-weight:500; max-width:300px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap">${escapeHtml(c.denomination || c.enseigne || '—')}</td>
                                <td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); color:var(--text-secondary); white-space:nowrap">
                                    <span title="${escapeHtml(c.naf_libelle || '')}">${escapeHtml(c.naf_code || '—')}</span>
                                </td>
                                <td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); color:var(--text-secondary)">${escapeHtml(c.ville || '—')}</td>
                                <td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); color:var(--text-secondary); text-align:center; white-space:nowrap">${escapeHtml(c.departement || '—')}</td>
                                <td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); color:var(--text-muted); font-size:var(--font-xs)">${escapeHtml(c.forme_juridique || '—')}</td>
                                <td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); text-align:center">
                                    ${c.statut === 'A'
                                        ? '<span style="color:var(--success); font-weight:600">●</span>'
                                        : '<span style="color:var(--text-muted)">○</span>'
                                    }
                                </td>
                                <td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); text-align:center">
                                    ${c.is_enriched
                                        ? '<span style="background:rgba(16,185,129,0.15); color:var(--success); font-size:var(--font-xs); font-weight:600; padding:2px 8px; border-radius:var(--radius-full)">✅ Enrichi</span>'
                                        : '<span style="color:var(--text-muted)">—</span>'
                                    }
                                </td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>

            ${totalPages > 1 ? `
                <div style="display:flex; justify-content:center; align-items:center; gap:var(--space-lg); margin-top:var(--space-2xl)">
                    <button class="btn btn-secondary" id="pagination-prev" ${hasPrev ? '' : 'disabled'}
                        style="${hasPrev ? '' : 'opacity:0.4; cursor:not-allowed'}">
                        ← Précédent
                    </button>
                    <span style="font-size:var(--font-sm); color:var(--text-secondary); font-weight:600">
                        ${currentPage} / ${totalPages}
                    </span>
                    <button class="btn btn-secondary" id="pagination-next" ${hasNext ? '' : 'disabled'}
                        style="${hasNext ? '' : 'opacity:0.4; cursor:not-allowed'}">
                        Suivant →
                    </button>
                </div>
            ` : ''}
        `;

        // Wire pagination buttons
        const prevBtn = document.getElementById('pagination-prev');
        const nextBtn = document.getElementById('pagination-next');
        if (prevBtn && hasPrev) {
            prevBtn.addEventListener('click', () => doSearch(q, currentOffset - PAGE_SIZE));
        }
        if (nextBtn && hasNext) {
            nextBtn.addEventListener('click', () => doSearch(q, currentOffset + PAGE_SIZE));
        }
    }

    // ── Event listeners ──────────────────────────────────────────

    // Filter dropdowns
    filterDept.addEventListener('change', () => {
        currentDepartment = filterDept.value;
        currentOffset = 0;
        updateFilterPills();
        const q = input.value.trim();
        if (q) doSearch(q, 0);
    });

    // NAF code filter (debounced)
    let nafTimer;
    filterNaf.addEventListener('input', () => {
        clearTimeout(nafTimer);
        nafTimer = setTimeout(() => {
            currentNafCode = filterNaf.value.trim();
            currentOffset = 0;
            updateFilterPills();
            const q = input.value.trim();
            if (q) doSearch(q, 0);
        }, 500);
    });

    // Search input
    input.addEventListener('input', () => {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => doSearch(input.value.trim()), 300);
    });

    input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            clearTimeout(debounceTimer);
            doSearch(input.value.trim());
        }
    });

    // Auto-search if query was in URL
    if (initialQuery) {
        doSearch(initialQuery);
        input.focus();
    } else {
        input.focus();
    }
}
