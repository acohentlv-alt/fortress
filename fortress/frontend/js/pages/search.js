/**
 * Search Page — Search by name, SIREN, or NAF code
 * Features:
 *   - NAF sorting controls with sort-by dropdown
 *   - Département & Secteur filter dropdowns
 *   - Active filter pills
 */

import { searchCompanies, getJobs, checkHealth, extractApiError, getExportUrl } from '../api.js';
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
    let currentMinRating = '';
    let currentMinReviews = '';
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
        <h1 class="page-title">🏦 Base SIRENE</h1>
        <p class="page-subtitle">Rechercher dans la base de données de 14.7M d'entreprises françaises</p>

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
                <div class="filter-group">
                    <label class="filter-label" for="filter-rating">⭐ Note minimale</label>
                    <select class="sort-select" id="filter-rating">
                        <option value="">Toutes les notes</option>
                        <option value="3.0">≥ 3.0 ⭐</option>
                        <option value="3.5">≥ 3.5 ⭐</option>
                        <option value="4.0">≥ 4.0 ⭐</option>
                        <option value="4.5">≥ 4.5 ⭐</option>
                    </select>
                </div>
                <div class="filter-group">
                    <label class="filter-label" for="filter-reviews">💬 Avis minimum</label>
                    <select class="sort-select" id="filter-reviews">
                        <option value="">Tous</option>
                        <option value="5">≥ 5 avis</option>
                        <option value="10">≥ 10 avis</option>
                        <option value="25">≥ 25 avis</option>
                        <option value="50">≥ 50 avis</option>
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

        <!-- Floating bulk action bar -->
        <div id="bulk-bar" style="
            position:fixed; bottom:-60px; left:50%; transform:translateX(-50%);
            background:var(--bg-elevated); border:1px solid var(--accent);
            border-radius:var(--radius-lg); padding:var(--space-md) var(--space-xl);
            display:flex; align-items:center; gap:var(--space-lg);
            box-shadow:0 -4px 20px rgba(0,0,0,0.3); z-index:100;
            transition:bottom 0.3s ease;
            min-width:320px; justify-content:center;
        ">
            <span id="bulk-count" style="font-weight:700; color:var(--accent)"></span>
            <button class="btn btn-primary btn-sm" id="bulk-export">📥 Exporter CSV</button>
            <button class="btn btn-secondary btn-sm" id="bulk-clear">Désélectionner</button>
        </div>
    `;

    const input = document.getElementById('search-input');
    const resultsEl = document.getElementById('search-results');
    const sortControls = document.getElementById('sort-controls');
    const sortSelect = document.getElementById('sort-select');
    const filterDept = document.getElementById('filter-dept');
    const filterSector = document.getElementById('filter-sector');
    const filterRating = document.getElementById('filter-rating');
    const filterReviews = document.getElementById('filter-reviews');
    const filterPills = document.getElementById('filter-pills');
    const bulkBar = document.getElementById('bulk-bar');
    const bulkCount = document.getElementById('bulk-count');
    let debounceTimer;
    const selectedSirens = new Set();

    function updateBulkBar() {
        if (selectedSirens.size > 0) {
            bulkBar.style.bottom = '20px';
            bulkCount.textContent = `${selectedSirens.size} sélectionnée${selectedSirens.size > 1 ? 's' : ''}`;
        } else {
            bulkBar.style.bottom = '-60px';
        }
    }

    document.getElementById('bulk-clear').addEventListener('click', () => {
        selectedSirens.clear();
        document.querySelectorAll('.card-checkbox').forEach(cb => cb.checked = false);
        updateBulkBar();
    });

    document.getElementById('bulk-export').addEventListener('click', () => {
        if (selectedSirens.size === 0) return;
        // Build CSV export URL with selected SIRENs as query param
        const sirens = Array.from(selectedSirens).join(',');
        const url = getExportUrl('csv') + `&sirens=${encodeURIComponent(sirens)}`;
        window.open(url, '_blank');
        showToast(`Export de ${selectedSirens.size} entreprises lancé`, 'success');
    });

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
        if (currentMinRating) {
            pills.push(`<span class="filter-pill" data-filter="rating">⭐ ≥ ${currentMinRating} <button class="filter-pill-remove" data-clear="rating">✕</button></span>`);
        }
        if (currentMinReviews) {
            pills.push(`<span class="filter-pill" data-filter="reviews">💬 ≥ ${currentMinReviews} avis <button class="filter-pill-remove" data-clear="reviews">✕</button></span>`);
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
                    } else if (target === 'rating') {
                        currentMinRating = '';
                        filterRating.value = '';
                    } else if (target === 'reviews') {
                        currentMinReviews = '';
                        filterReviews.value = '';
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
            minRating: currentMinRating,
            minReviews: currentMinReviews,
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
            <div class="company-grid" id="search-grid">
                ${data.results.map(c => {
                    const checked = selectedSirens.has(c.siren);
                    return `
                        <div style="position:relative">
                            <input type="checkbox" class="card-checkbox" data-siren="${c.siren}"
                                ${checked ? 'checked' : ''}
                                style="position:absolute; top:12px; left:12px; z-index:2; width:18px; height:18px; cursor:pointer; accent-color:var(--accent);"
                                onclick="event.stopPropagation()">
                            ${companyCard(c)}
                        </div>
                    `;
                }).join('')}
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

        // Wire checkboxes for bulk selection
        const grid = document.getElementById('search-grid');
        if (grid) {
            grid.addEventListener('change', (e) => {
                if (!e.target.classList.contains('card-checkbox')) return;
                const siren = e.target.dataset.siren;
                if (e.target.checked) {
                    selectedSirens.add(siren);
                } else {
                    selectedSirens.delete(siren);
                }
                updateBulkBar();
            });
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

    filterRating.addEventListener('change', () => {
        currentMinRating = filterRating.value;
        currentOffset = 0;
        updateFilterPills();
        const q = input.value.trim();
        if (q) doSearch(q, 0);
    });

    filterReviews.addEventListener('change', () => {
        currentMinReviews = filterReviews.value;
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
