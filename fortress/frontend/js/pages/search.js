/**
 * Search Page — Search by name, SIREN, or NAF code
 * Features:
 *   - NAF sorting controls with sort-by dropdown
 *   - Département & Secteur filter dropdowns
 *   - Active filter pills
 */

import { searchCompanies, getJobs, checkHealth, extractApiError } from '../api.js';
import { companyCard, escapeHtml, showToast } from '../components.js';
import { DEPARTMENTS } from '../constants.js';

export async function renderSearch(container) {
    // Extract query from hash if present
    const hashParts = window.location.hash.split('?');
    const params = new URLSearchParams(hashParts[1] || '');
    const initialQuery = params.get('q') || '';

    // Current filter + sort + pagination state
    let currentSortBy = '';
    let currentOrder = '';
    let currentDepartment = '';
    let currentSector = '';
    let currentOffset = 0;
    const PAGE_SIZE = 50;

    // Fetch sectors (unique job names) for the filter dropdown
    let sectors = [];
    try {
        const jobs = await getJobs();
        if (jobs && Array.isArray(jobs)) {
            const seen = new Set();
            for (const j of jobs) {
                const name = (j.query_name || '').toUpperCase().trim();
                if (name && !seen.has(name)) {
                    seen.add(name);
                    sectors.push(name);
                }
            }
            sectors.sort();
        }
    } catch { /* ignore — sectors will be empty */ }

    container.innerHTML = `
        <h1 class="page-title">Recherche</h1>
        <p class="page-subtitle">Rechercher une entreprise par nom, numéro SIREN ou code NAF</p>

        <div style="margin-bottom: var(--space-xl); max-width: 700px">
            <!-- Search Input -->
            <div style="position:relative; margin-bottom: var(--space-lg)">
                <span style="position:absolute; left:14px; top:50%; transform:translateY(-50%); color:var(--text-muted); font-size:1.1rem">🔍</span>
                <input type="text" id="search-input"
                    value="${escapeHtml(initialQuery)}"
                    placeholder="Rechercher par Nom, SIREN, ou Code NAF..."
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
                    <label class="filter-label" for="filter-sector">📋 Secteur</label>
                    <select class="sort-select" id="filter-sector">
                        <option value="">Tous les secteurs</option>
                        ${sectors.map(s =>
        `<option value="${escapeHtml(s)}">${escapeHtml(s)}</option>`
    ).join('')}
                    </select>
                </div>
            </div>

            <!-- Active Filter Pills -->
            <div class="filter-pills" id="filter-pills" style="display:none"></div>
        </div>

        <!-- Sort Controls -->
        <div class="sort-controls" id="sort-controls" style="display:none">
            <span class="sort-controls-label">Trier par</span>
            <select class="sort-select" id="sort-select">
                <option value="">Pertinence</option>
                <option value="denomination:asc">Dénomination ↑ A→Z</option>
                <option value="denomination:desc">Dénomination ↓ Z→A</option>
                <option value="naf:asc">Code NAF ↑</option>
                <option value="naf:desc">Code NAF ↓</option>
            </select>
        </div>

        <div id="search-results"></div>
    `;

    const input = document.getElementById('search-input');
    const resultsEl = document.getElementById('search-results');
    const sortControls = document.getElementById('sort-controls');
    const sortSelect = document.getElementById('sort-select');
    const filterDept = document.getElementById('filter-dept');
    const filterSector = document.getElementById('filter-sector');
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
        if (currentSector) {
            pills.push(`<span class="filter-pill" data-filter="sector">📋 ${escapeHtml(currentSector)} <button class="filter-pill-remove" data-clear="sector">✕</button></span>`);
        }

        if (pills.length > 0) {
            filterPills.style.display = 'flex';
            filterPills.innerHTML = pills.join('');

            // Wire up remove buttons
            filterPills.querySelectorAll('.filter-pill-remove').forEach(btn => {
                btn.addEventListener('click', (e) => {
                    const target = e.target.dataset.clear;
                    if (target === 'dept') {
                        currentDepartment = '';
                        filterDept.value = '';
                    } else if (target === 'sector') {
                        currentSector = '';
                        filterSector.value = '';
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
            sortControls.style.display = 'none';
            return;
        }

        currentOffset = offset;
        resultsEl.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

        const data = await searchCompanies(q, {
            limit: PAGE_SIZE,
            offset: currentOffset,
            sortBy: currentSortBy,
            order: currentOrder,
            department: currentDepartment,
            sector: currentSector,
        });

        // API error (503, 500, network failure)
        if (!data || (data._status && !data._ok)) {
            sortControls.style.display = 'none';
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
            sortControls.style.display = 'none';
            resultsEl.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">🔍</div>
                    <div class="empty-state-text">Aucun résultat pour "${escapeHtml(q)}"</div>
                    <p style="color: var(--text-muted)">Essayez un autre terme, un numéro SIREN ou un code NAF</p>
                </div>
            `;
            return;
        }

        // Show sort controls when there are results
        sortControls.style.display = 'inline-flex';

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
            <div class="company-grid">
                ${data.results.map(c => companyCard(c)).join('')}
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

    // Sort dropdown
    sortSelect.addEventListener('change', () => {
        const val = sortSelect.value;
        if (val) {
            const [sortBy, order] = val.split(':');
            currentSortBy = sortBy;
            currentOrder = order;
        } else {
            currentSortBy = '';
            currentOrder = '';
        }
        currentOffset = 0;
        const q = input.value.trim();
        if (q) doSearch(q, 0);
    });

    // Filter dropdowns
    filterDept.addEventListener('change', () => {
        currentDepartment = filterDept.value;
        currentOffset = 0;
        updateFilterPills();
        const q = input.value.trim();
        if (q) doSearch(q, 0);
    });

    filterSector.addEventListener('change', () => {
        currentSector = filterSector.value;
        currentOffset = 0;
        updateFilterPills();
        const q = input.value.trim();
        if (q) doSearch(q, 0);
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
