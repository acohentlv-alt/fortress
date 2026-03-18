/**
 * Contacts Page — Flat searchable list of all enriched contacts
 *
 * Shows ALL contacts + officers across all enriched companies in one table.
 * Features: search, department filter, "load more" pagination, click-to-company.
 *
 * UX: Initially loads 5 pages (250 rows). User can "Charger plus" for more.
 */

import { getContactsList, bulkExportCSV, extractApiError, checkHealth } from '../api.js';
import { escapeHtml, showToast } from '../components.js';
import { DEPARTMENTS } from '../constants.js';

export async function renderContacts(container) {
    let currentDepartment = '';
    let currentQuery = '';
    const PAGE_SIZE = 50;
    const INITIAL_PAGES = 5; // Load 5 pages (250 contacts) initially
    let allResults = [];      // Accumulated results across loads
    let hasMore = true;       // Whether there are more results to load
    let totalDisplay = '...'; // Smart count display string

    container.innerHTML = `
        <div style="display:flex; justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:var(--space-md); margin-bottom:var(--space-xl)">
            <div>
                <h1 class="page-title">📇 Contacts</h1>
                <p class="page-subtitle">Vue complète de tous les contacts enrichis — téléphones, emails, dirigeants</p>
            </div>
            <button class="btn btn-secondary" id="contacts-export-btn" style="white-space:nowrap">
                📥 Exporter CSV
            </button>
        </div>

        <!-- Search + Filters -->
        <div style="display:flex; gap:var(--space-md); margin-bottom:var(--space-lg); flex-wrap:wrap; max-width:800px">
            <div style="flex:1; min-width:260px; position:relative">
                <span style="position:absolute; left:12px; top:50%; transform:translateY(-50%); color:var(--text-muted)">🔍</span>
                <input type="text" id="contacts-search"
                    placeholder="Rechercher par nom, SIREN, tél, email, dirigeant..."
                    style="width:100%; padding:var(--space-sm) var(--space-md); padding-left:36px;
                           background:var(--bg-input); border:1px solid var(--border-default);
                           border-radius:var(--radius-sm); color:var(--text-primary);
                           font-family:var(--font-family); font-size:var(--font-sm); outline:none;
                           transition:border-color var(--transition-fast), box-shadow var(--transition-fast)"
                    onfocus="this.style.borderColor='var(--accent)'; this.style.boxShadow='0 0 0 3px var(--accent-subtle)'"
                    onblur="this.style.borderColor='var(--border-default)'; this.style.boxShadow='none'"
                    autocomplete="off"
                >
            </div>
            <select id="contacts-dept-filter" class="sort-select"
                style="min-width:180px; background:var(--bg-input); border:1px solid var(--border-default);
                       border-radius:var(--radius-sm); color:var(--text-primary); font-size:var(--font-sm);
                       padding:var(--space-sm) var(--space-md)">
                <option value="">Tous les départements</option>
                ${DEPARTMENTS.map(([code, name]) =>
                    `<option value="${code}">${code} — ${escapeHtml(name)}</option>`
                ).join('')}
            </select>
        </div>

        <!-- Results -->
        <div id="contacts-results">
            <div class="loading"><div class="spinner"></div></div>
        </div>
    `;

    const searchInput = document.getElementById('contacts-search');
    const deptFilter = document.getElementById('contacts-dept-filter');
    const resultsEl = document.getElementById('contacts-results');
    let debounceTimer;

    // ── Render the table from accumulated results ────────────────
    function renderTable() {
        if (allResults.length === 0) {
            resultsEl.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">📇</div>
                    <div class="empty-state-text">${currentQuery
                        ? `Aucun contact trouvé pour "${escapeHtml(currentQuery)}"`
                        : 'Aucun contact enrichi'}</div>
                    <p style="color:var(--text-muted); font-size:var(--font-sm)">
                        Les contacts apparaissent ici après une recherche Maps ou un import CSV
                    </p>
                </div>
            `;
            return;
        }

        resultsEl.innerHTML = `
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:var(--space-md)">
                <span style="font-size:var(--font-sm); color:var(--text-secondary)">
                    ${totalDisplay} contact${allResults.length > 1 ? 's' : ''}
                    — ${allResults.length} affiché${allResults.length > 1 ? 's' : ''}
                </span>
            </div>

            <div class="card" style="overflow-x:auto">
                <table style="width:100%; border-collapse:collapse; font-size:var(--font-sm)">
                    <thead>
                        <tr>
                            <th class="contacts-th">Entreprise</th>
                            <th class="contacts-th" style="white-space:nowrap">SIREN</th>
                            <th class="contacts-th">📞 Tél</th>
                            <th class="contacts-th">✉️ Email</th>
                            <th class="contacts-th">🌐 Site</th>
                            <th class="contacts-th">👤 Dirigeant</th>
                            <th class="contacts-th">📞 Ligne directe</th>
                            <th class="contacts-th" style="white-space:nowrap">Dépt</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${allResults.map(r => `
                            <tr class="contacts-row" onclick="window.location.hash='#/company/${r.siren}'">
                                <td class="contacts-td" style="font-weight:500; max-width:240px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap">
                                    ${escapeHtml(r.denomination || '—')}
                                </td>
                                <td class="contacts-td" style="font-family:var(--font-mono); color:var(--accent); font-weight:600; white-space:nowrap">
                                    ${escapeHtml(r.siren)}
                                </td>
                                <td class="contacts-td">
                                    ${r.phone
                                        ? `<a href="tel:${r.phone}" style="color:var(--success); font-weight:600; white-space:nowrap" onclick="event.stopPropagation()">${escapeHtml(r.phone)}</a>`
                                        : '<span style="color:var(--text-disabled)">—</span>'}
                                </td>
                                <td class="contacts-td" style="max-width:200px; overflow:hidden; text-overflow:ellipsis">
                                    ${r.email
                                        ? `<a href="mailto:${r.email}" style="color:var(--accent)" onclick="event.stopPropagation()">${escapeHtml(r.email)}</a>`
                                        : '<span style="color:var(--text-disabled)">—</span>'}
                                </td>
                                <td class="contacts-td" style="max-width:160px; overflow:hidden; text-overflow:ellipsis">
                                    ${r.website
                                        ? `<a href="${r.website.startsWith('http') ? r.website : 'https://' + r.website}" target="_blank" style="color:var(--accent)" onclick="event.stopPropagation()">${escapeHtml(r.website.replace(/^https?:\/\/(www\.)?/, '').slice(0, 25))}</a>`
                                        : '<span style="color:var(--text-disabled)">—</span>'}
                                </td>
                                <td class="contacts-td" style="white-space:nowrap">
                                    ${r.dirigeant_nom
                                        ? `${escapeHtml(r.dirigeant_prenom ? r.dirigeant_prenom + ' ' : '')}${escapeHtml(r.dirigeant_nom)}`
                                        : '<span style="color:var(--text-disabled)">—</span>'}
                                </td>
                                <td class="contacts-td">
                                    ${r.ligne_directe
                                        ? `<span style="color:var(--success); font-weight:600">${escapeHtml(r.ligne_directe)}</span>`
                                        : '<span style="color:var(--text-disabled)">—</span>'}
                                </td>
                                <td class="contacts-td" style="text-align:center; white-space:nowrap">
                                    ${escapeHtml(r.departement || '—')}
                                </td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>

            ${hasMore ? `
                <div style="display:flex; justify-content:center; margin-top:var(--space-xl)">
                    <button class="btn btn-secondary" id="contacts-load-more"
                        style="padding:var(--space-md) var(--space-2xl); font-size:var(--font-base); border-radius:var(--radius-lg)">
                        ⬇️ Charger plus
                    </button>
                </div>
            ` : `
                <div style="text-align:center; margin-top:var(--space-xl); color:var(--text-muted); font-size:var(--font-sm)">
                    ✅ Tous les contacts affichés
                </div>
            `}
        `;

        // Wire load-more button
        const loadMoreBtn = document.getElementById('contacts-load-more');
        if (loadMoreBtn) {
            loadMoreBtn.addEventListener('click', () => loadMore());
        }
    }

    // ── Initial search: load 5 pages worth ───────────────────────
    async function doSearch() {
        allResults = [];
        hasMore = true;
        resultsEl.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

        const params = { limit: PAGE_SIZE * INITIAL_PAGES, offset: 0 };
        if (currentQuery) params.q = currentQuery;
        if (currentDepartment) params.department = currentDepartment;

        const data = await getContactsList(params);

        if (!data || (data._status && !data._ok)) {
            const errorMsg = extractApiError(data);
            resultsEl.innerHTML = `
                <div class="error-state text-center" style="padding:var(--space-2xl)">
                    <div style="font-size:3rem; margin-bottom:var(--space-lg)">🔌</div>
                    <div style="color:var(--text-secondary); margin-bottom:var(--space-lg)">${escapeHtml(errorMsg)}</div>
                    <button id="contacts-retry" class="btn btn-primary">🔄 Réessayer</button>
                </div>
            `;
            document.getElementById('contacts-retry')?.addEventListener('click', async () => {
                const health = await checkHealth();
                if (health.ok) doSearch();
                else showToast('Serveur inaccessible', 'error');
            });
            return;
        }

        const results = data.results || [];
        allResults = results;
        totalDisplay = data.total_display || String(data.total || results.length);
        hasMore = results.length >= PAGE_SIZE * INITIAL_PAGES;

        renderTable();
    }

    // ── Load more: append next batch ─────────────────────────────
    async function loadMore() {
        const loadMoreBtn = document.getElementById('contacts-load-more');
        if (loadMoreBtn) {
            loadMoreBtn.disabled = true;
            loadMoreBtn.textContent = '⏳ Chargement...';
        }

        const params = { limit: PAGE_SIZE * INITIAL_PAGES, offset: allResults.length };
        if (currentQuery) params.q = currentQuery;
        if (currentDepartment) params.department = currentDepartment;

        const data = await getContactsList(params);

        if (!data || (data._status && !data._ok)) {
            showToast(extractApiError(data), 'error');
            if (loadMoreBtn) {
                loadMoreBtn.disabled = false;
                loadMoreBtn.textContent = '⬇️ Charger plus';
            }
            return;
        }

        const newResults = data.results || [];
        allResults = allResults.concat(newResults);
        hasMore = newResults.length >= PAGE_SIZE * INITIAL_PAGES;

        renderTable();
    }

    // ── Event listeners ──────────────────────────────────────────
    searchInput.addEventListener('input', () => {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => {
            currentQuery = searchInput.value.trim();
            doSearch();
        }, 400);
    });

    searchInput.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            clearTimeout(debounceTimer);
            currentQuery = searchInput.value.trim();
            doSearch();
        }
    });

    deptFilter.addEventListener('change', () => {
        currentDepartment = deptFilter.value;
        doSearch();
    });

    // Export button
    document.getElementById('contacts-export-btn')?.addEventListener('click', async () => {
        const btn = document.getElementById('contacts-export-btn');
        btn.disabled = true;
        btn.textContent = '⏳ Export...';
        try {
            // Export all enriched contacts (not just current page)
            await bulkExportCSV({ format: 'csv' });
            showToast('Export CSV téléchargé ✅', 'success');
        } catch (err) {
            showToast('Erreur d\'export: ' + err.message, 'error');
        } finally {
            btn.disabled = false;
            btn.textContent = '📥 Exporter CSV';
        }
    });

    // Initial load
    doSearch();
}
