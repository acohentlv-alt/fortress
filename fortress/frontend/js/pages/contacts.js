/**
 * Contacts Page — Flat searchable list of all enriched contacts
 *
 * Shows ALL contacts + officers across all enriched companies in one table.
 * Features: search, department filter, "load more" pagination, click-to-company.
 *
 * UX: Initially loads 5 pages (250 rows). User can "Charger plus" for more.
 */

import { getContactsList, bulkExportCSV, extractApiError, checkHealth, startDeepEnrich } from '../api.js';
import { escapeHtml, showToast, showConfirmModal } from '../components.js';
import { DEPARTMENTS } from '../constants.js';
import { showAddEntityModal } from '../components/add-entity-modal.js';

let selectionMode = false;
let selectedSirens = new Set();

export async function renderContacts(container) {
    // Reset selection state when page is entered
    selectedSirens.clear();
    selectionMode = false;

    let currentDepartment = '';
    let currentNafCode = '';
    let currentQuery = '';
    const PAGE_SIZE = 50;
    const INITIAL_PAGES = 5;
    let allResults = [];
    let hasMore = true;
    let totalDisplay = '...';

    container.innerHTML = `
        <div style="display:flex; justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:var(--space-md); margin-bottom:var(--space-xl)">
            <div>
                <h1 class="page-title">📇 Contacts</h1>
                <p class="page-subtitle">Vue complète de tous les contacts enrichis — téléphones, emails, dirigeants</p>
            </div>
            <div style="display:flex; gap:var(--space-sm); flex-wrap:wrap; align-items:center">
                <button class="btn btn-secondary" id="contacts-select-toggle" style="white-space:nowrap">
                    ☑ Sélectionner
                </button>
                <button class="btn btn-secondary" id="contacts-export-btn" style="white-space:nowrap">
                    📥 Exporter CSV
                </button>
                <button class="btn btn-secondary" id="contacts-add-entity" style="white-space:nowrap">
                    ➕ Ajouter
                </button>
            </div>
        </div>

        <!-- Search + Filters -->
        <div class="search-filters-row">
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
                style="min-width:160px; background:var(--bg-input); border:1px solid var(--border-default);
                       border-radius:var(--radius-sm); color:var(--text-primary); font-size:var(--font-sm);
                       padding:var(--space-sm) var(--space-md)">
                <option value="">Dépt:</option>
                ${DEPARTMENTS.map(([code, name]) =>
                    `<option value="${code}">${code} — ${escapeHtml(name)}</option>`
                ).join('')}
            </select>
            <input type="text" id="contacts-naf-filter"
                placeholder="NAF (ex: 55.30Z)"
                style="width:130px; background:var(--bg-input); border:1px solid var(--border-default);
                       border-radius:var(--radius-sm); color:var(--text-primary); font-size:var(--font-sm);
                       padding:var(--space-sm) var(--space-md); font-family:var(--font-family)"
                autocomplete="off"
            >
        </div>

        <!-- Results -->
        <div id="contacts-results">
            <div class="loading"><div class="spinner"></div></div>
        </div>
    `;

    const searchInput = document.getElementById('contacts-search');
    const deptFilter = document.getElementById('contacts-dept-filter');
    const nafFilter = document.getElementById('contacts-naf-filter');
    const resultsEl = document.getElementById('contacts-results');
    let debounceTimer;
    let nafTimer;

    // ── Bulk bar helpers ──────────────────────────────────────────
    function _updateContactsBulkBar() {
        let bar = document.getElementById('bulk-action-bar');
        if (selectedSirens.size === 0) {
            _removeContactsBulkBar();
            return;
        }
        if (!bar) {
            bar = document.createElement('div');
            bar.id = 'bulk-action-bar';
            bar.className = 'bulk-action-bar';
            document.body.appendChild(bar);
        }
        const n = selectedSirens.size;
        bar.innerHTML = `
            <span class="bulk-count">☑ ${n} sélectionnée${n > 1 ? 's' : ''}</span>
            <button class="btn btn-secondary" id="contacts-bulk-select-all">Tout sélectionner</button>
            <button class="btn btn-primary" id="contacts-bulk-enrich">🚀 Enrichir via site web</button>
            <button class="btn btn-danger" id="contacts-bulk-delete">🗑️ Supprimer</button>
        `;

        document.getElementById('contacts-bulk-select-all').onclick = () => {
            const boxes = document.querySelectorAll('.contact-checkbox');
            const allChecked = [...boxes].every(b => b.checked);
            boxes.forEach(b => {
                b.checked = !allChecked;
                if (!allChecked) selectedSirens.add(b.dataset.siren);
                else selectedSirens.delete(b.dataset.siren);
            });
            renderTable();
        };

        document.getElementById('contacts-bulk-enrich').onclick = async () => {
            const sirens = [...selectedSirens];
            if (!sirens.length) return;
            if (sirens.length > 20) {
                showToast('Maximum 20 entreprises par enrichissement', 'error');
                return;
            }
            showToast(`Enrichissement de ${sirens.length} entreprise(s)…`, 'info');
            const result = await startDeepEnrich(sirens);
            if (result && result._ok !== false) {
                showToast(`Enrichissement lancé pour ${sirens.length} entreprise(s)`, 'success');
                selectionMode = false;
                selectedSirens.clear();
                _removeContactsBulkBar();
                renderTable();
            } else {
                showToast("Erreur lors de l'enrichissement", 'error');
            }
        };

        document.getElementById('contacts-bulk-delete').onclick = () => {
            const sirens = [...selectedSirens];
            if (!sirens.length) return;
            showConfirmModal({
                title: `Supprimer ${sirens.length} entreprise${sirens.length > 1 ? 's' : ''} ?`,
                body: `<p>Supprimer définitivement ${sirens.length} entreprise${sirens.length > 1 ? 's' : ''} de la base de données</p>`,
                confirmLabel: 'Supprimer',
                danger: true,
                checkboxLabel: 'Également ajouter à la liste noire',
                onConfirm: async (blacklist) => {
                    let ok = 0;
                    for (const siren of sirens) {
                        const res = await fetch(`/api/companies/${encodeURIComponent(siren)}/delete`, {
                            method: 'DELETE',
                            credentials: 'same-origin',
                        });
                        if (res.ok) ok++;
                        if (blacklist) {
                            await fetch('/api/blacklist', {
                                method: 'POST',
                                headers: { 'Content-Type': 'application/json' },
                                body: JSON.stringify({ siren, reason: 'Supprimé en masse' }),
                                credentials: 'same-origin',
                            });
                        }
                    }
                    showToast(`${ok}/${sirens.length} supprimée(s)`, 'success');
                    selectionMode = false;
                    selectedSirens.clear();
                    _removeContactsBulkBar();
                    doSearch();
                }
            });
        };
    }

    function _removeContactsBulkBar() {
        const bar = document.getElementById('bulk-action-bar');
        if (bar) bar.remove();
    }

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
                            ${selectionMode ? '<th class="contacts-th" style="width:40px"></th>' : ''}
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
                            <tr class="contacts-row" onclick="window.location.hash='#/company/${r.siren}'"
                                ${selectionMode && selectedSirens.has(r.siren) ? 'style="background:rgba(99,102,241,0.08)"' : ''}>
                                ${selectionMode ? `
                                <td style="width:40px; text-align:center" onclick="event.stopPropagation()">
                                    <input type="checkbox" class="contact-checkbox" data-siren="${escapeHtml(r.siren)}"
                                           ${selectedSirens.has(r.siren) ? 'checked' : ''}>
                                </td>` : ''}
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
                                        ? `<a href="${r.website.startsWith('http') ? r.website : 'https://' + r.website}" target="_blank" rel="noopener" style="color:var(--accent)" onclick="event.stopPropagation()">${escapeHtml(r.website.replace(/^https?:\/\/(www\.)?/, '').slice(0, 25))}</a>`
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

        // Wire checkboxes when in selection mode
        if (selectionMode) {
            document.querySelectorAll('.contact-checkbox').forEach(cb => {
                cb.addEventListener('change', () => {
                    if (cb.checked) selectedSirens.add(cb.dataset.siren);
                    else selectedSirens.delete(cb.dataset.siren);
                    _updateContactsBulkBar();
                });
            });
            _updateContactsBulkBar();
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
        if (currentNafCode) params.naf_code = currentNafCode;

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
        if (currentNafCode) params.naf_code = currentNafCode;

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

    nafFilter.addEventListener('input', () => {
        clearTimeout(nafTimer);
        nafTimer = setTimeout(() => {
            currentNafCode = nafFilter.value.trim();
            doSearch();
        }, 400);
    });

    // Select toggle button
    document.getElementById('contacts-select-toggle')?.addEventListener('click', () => {
        selectionMode = !selectionMode;
        const btn = document.getElementById('contacts-select-toggle');
        if (btn) btn.textContent = selectionMode ? '✖ Annuler' : '☑ Sélectionner';
        if (!selectionMode) {
            selectedSirens.clear();
            _removeContactsBulkBar();
        }
        renderTable();
    });

    // Add entity button
    document.getElementById('contacts-add-entity')?.addEventListener('click', () => {
        showAddEntityModal({ onSuccess: () => doSearch() });
    });

    // Export button
    document.getElementById('contacts-export-btn')?.addEventListener('click', async () => {
        const btn = document.getElementById('contacts-export-btn');
        btn.disabled = true;
        btn.textContent = '⏳ Export...';
        try {
            const sirens = allResults.map(r => r.siren);
            const resp = await fetch('/api/export/bulk/csv', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ sirens }),
                credentials: 'same-origin',
            });
            const blob = await resp.blob();
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'contacts_export.csv';
            a.click();
            URL.revokeObjectURL(url);
            showToast('Export CSV téléchargé', 'success');
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
