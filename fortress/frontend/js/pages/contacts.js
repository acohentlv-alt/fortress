/**
 * Contacts Page — Flat searchable list of all enriched contacts
 *
 * Shows ALL contacts + officers across all enriched companies in one table.
 * Features: search, department filter, "load more" pagination, click-to-company.
 *
 * UX: Initially loads 5 pages (250 rows). User can "Charger plus" for more.
 */

import { getContactsList, bulkExportCSV, extractApiError, checkHealth } from '../api.js';
import { escapeHtml, showToast, showConfirmModal, sourceLabel } from '../components.js';
import { DEPARTMENTS } from '../constants.js';
import { showAddEntityModal } from '../components/add-entity-modal.js';
import { t } from '../i18n.js';

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
                <h1 class="page-title">📇 ${t('contacts.title')}</h1>
                <p class="page-subtitle">${t('contacts.subtitle')}</p>
            </div>
            <div style="display:flex; gap:var(--space-sm); flex-wrap:wrap; align-items:center">
                <button class="btn btn-secondary" id="contacts-select-toggle" style="white-space:nowrap">
                    ${t('contacts.selectToggle')}
                </button>
                <button class="btn btn-secondary" id="contacts-export-btn" style="white-space:nowrap">
                    ${t('contacts.exportCSV')}
                </button>
                <button class="btn btn-secondary" id="contacts-add-entity" style="white-space:nowrap">
                    ${t('contacts.addEntity')}
                </button>
            </div>
        </div>

        <!-- Search + Filters -->
        <div class="search-filters-row">
            <div style="flex:1; min-width:260px; position:relative">
                <span style="position:absolute; left:12px; top:50%; transform:translateY(-50%); color:var(--text-muted)">🔍</span>
                <input type="text" id="contacts-search"
                    placeholder="${t('contacts.searchPlaceholder')}"
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
                <option value="">${t('contacts.deptFilter')}</option>
                ${DEPARTMENTS.map(([code, name]) =>
                    `<option value="${code}">${code} — ${escapeHtml(name)}</option>`
                ).join('')}
            </select>
            <input type="text" id="contacts-naf-filter"
                placeholder="${t('contacts.nafPlaceholder')}"
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

    // ── Delegated checkbox handler (one listener, works for all rows) ──
    resultsEl.addEventListener('change', (e) => {
        if (!selectionMode) return;
        const cb = e.target.closest('.contact-checkbox');
        if (!cb) return;
        if (cb.checked) selectedSirens.add(cb.dataset.siren);
        else selectedSirens.delete(cb.dataset.siren);
        const row = cb.closest('tr');
        if (row) row.style.background = cb.checked ? 'rgba(99,102,241,0.08)' : '';
        _updateContactsBulkBar();
    });

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
            <span class="bulk-count">${t('contacts.bulkCount', { count: n, plural: n > 1 ? 's' : '' })}</span>
            <button class="btn btn-secondary" id="contacts-bulk-select-all">${t('contacts.bulkSelectAll')}</button>
            <button class="btn btn-danger" id="contacts-bulk-delete">${t('contacts.bulkDelete')}</button>
        `;

        document.getElementById('contacts-bulk-select-all').onclick = () => {
            const boxes = document.querySelectorAll('.contact-checkbox');
            const allChecked = [...boxes].every(b => b.checked);
            boxes.forEach(b => {
                b.checked = !allChecked;
                if (!allChecked) selectedSirens.add(b.dataset.siren);
                else selectedSirens.delete(b.dataset.siren);
                const row = b.closest('tr');
                if (row) row.style.background = !allChecked ? 'rgba(99,102,241,0.08)' : '';
            });
            _updateContactsBulkBar();
        };

        document.getElementById('contacts-bulk-delete').onclick = () => {
            const sirens = [...selectedSirens];
            if (!sirens.length) return;
            showConfirmModal({
                title: t('contacts.bulkDeleteTitle', { count: sirens.length, plural: sirens.length > 1 ? 's' : '' }),
                body: `<p>${t('contacts.bulkDeleteBody', { count: sirens.length, plural: sirens.length > 1 ? 's' : '' })}</p>`,
                confirmLabel: t('contacts.deleteConfirm'),
                danger: true,
                checkboxLabel: t('contacts.alsoBlacklist'),
                onConfirm: async (blacklist) => {
                    let ok = 0;
                    for (const siren of sirens) {
                        const res = await fetch(`/api/companies/${encodeURIComponent(siren)}/tags/`, {
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
                    showToast(t('contacts.bulkDeleteSuccess', { ok, total: sirens.length }), 'success');
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
            const hasFilter = currentDepartment || currentNafCode;
            let emptyText;
            let emptySubtext;
            if (currentQuery) {
                emptyText = t('contacts.noContactsQuery', { query: escapeHtml(currentQuery) });
                emptySubtext = t('contacts.noContactsTryOther');
            } else if (currentDepartment && currentNafCode) {
                emptyText = t('contacts.noContactsDeptNaf');
                emptySubtext = t('contacts.modifyFilters');
            } else if (currentDepartment) {
                emptyText = t('contacts.noContactsDept');
                emptySubtext = t('contacts.noContactsDeptHint');
            } else if (currentNafCode) {
                emptyText = t('contacts.noContactsNaf');
                emptySubtext = t('contacts.noContactsNafHint');
            } else {
                emptyText = t('contacts.noContactsEnriched');
                emptySubtext = t('contacts.noContactsHint');
            }
            resultsEl.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">📇</div>
                    <div class="empty-state-text">${emptyText}</div>
                    <p style="color:var(--text-muted); font-size:var(--font-sm)">
                        ${emptySubtext}
                    </p>
                </div>
            `;
            return;
        }

        resultsEl.innerHTML = `
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:var(--space-md)">
                <span style="font-size:var(--font-sm); color:var(--text-secondary)">
                    ${t('contacts.countDisplay', { total: totalDisplay, shown: allResults.length, plural: allResults.length > 1 ? 's' : '' })}
                </span>
            </div>

            <div class="card" style="overflow-x:auto">
                <table style="width:100%; border-collapse:collapse; font-size:var(--font-sm)">
                    <thead>
                        <tr>
                            ${selectionMode ? '<th class="contacts-th" style="width:40px"></th>' : ''}
                            <th class="contacts-th">${t('contacts.company')}</th>
                            <th class="contacts-th" style="white-space:nowrap">${t('contacts.siren')}</th>
                            <th class="contacts-th">${t('contacts.colPhone')}</th>
                            <th class="contacts-th">${t('contacts.colEmail')}</th>
                            <th class="contacts-th">${t('contacts.colSite')}</th>
                            <th class="contacts-th">${t('contacts.colDirector')}</th>
                            <th class="contacts-th">${t('contacts.colDirectLine')}</th>
                            <th class="contacts-th" style="white-space:nowrap">${t('contacts.dept')}</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${allResults.map(r => `
                            <tr class="contacts-row" onclick="window.location.hash='#/company/${r.siren}'"
                                ${selectionMode && selectedSirens.has(r.siren) ? 'style="background:rgba(99,102,241,0.08)"' : ''}>
                                ${selectionMode ? `
                                <td style="width:40px; text-align:center; cursor:pointer" onclick="event.stopPropagation(); var cb=this.querySelector('input'); if(event.target!==cb){cb.checked=!cb.checked; cb.dispatchEvent(new Event('change',{bubbles:true}));}">
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
                                        ? `<a href="tel:${r.phone}" style="color:var(--success); font-weight:600; white-space:nowrap" onclick="event.stopPropagation()">${escapeHtml(r.phone)}</a>${r.phone_source ? ` <span class="source-tooltip" data-tooltip="${escapeHtml(sourceLabel(r.phone_source))}">i</span>` : ''}`
                                        : '<span style="color:var(--text-disabled)">—</span>'}
                                </td>
                                <td class="contacts-td" style="max-width:200px; overflow:hidden; text-overflow:ellipsis">
                                    ${r.email
                                        ? `<a href="mailto:${r.email}" style="color:var(--accent)" onclick="event.stopPropagation()">${escapeHtml(r.email)}</a>${r.email_source ? ` <span class="source-tooltip" data-tooltip="${escapeHtml(sourceLabel(r.email_source))}">i</span>` : ''}`
                                        : '<span style="color:var(--text-disabled)">—</span>'}
                                </td>
                                <td class="contacts-td" style="max-width:160px; overflow:hidden; text-overflow:ellipsis">
                                    ${r.website
                                        ? `<a href="${r.website.startsWith('http') ? r.website : 'https://' + r.website}" target="_blank" rel="noopener" style="color:var(--accent)" onclick="event.stopPropagation()">${escapeHtml(r.website.replace(/^https?:\/\/(www\.)?/, '').slice(0, 25))}</a>${r.website_source ? ` <span class="source-tooltip" data-tooltip="${escapeHtml(sourceLabel(r.website_source))}">i</span>` : ''}`
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
                        ${t('contacts.loadMore')}
                    </button>
                </div>
            ` : `
                <div style="text-align:center; margin-top:var(--space-xl); color:var(--text-muted); font-size:var(--font-sm)">
                    ${t('contacts.allDisplayed')}
                </div>
            `}
        `;

        // Wire load-more button
        const loadMoreBtn = document.getElementById('contacts-load-more');
        if (loadMoreBtn) {
            loadMoreBtn.addEventListener('click', () => loadMore());
        }

        // Update bulk bar state (checkbox events handled by delegated listener on resultsEl)
        if (selectionMode) {
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
                    <button id="contacts-retry" class="btn btn-primary">${t('contacts.retry')}</button>
                </div>
            `;
            document.getElementById('contacts-retry')?.addEventListener('click', async () => {
                const health = await checkHealth();
                if (health.ok) doSearch();
                else showToast(t('contacts.serverUnavailable'), 'error');
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
            loadMoreBtn.textContent = t('contacts.loadingMore');
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
                loadMoreBtn.textContent = t('contacts.loadMore');
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
        if (btn) btn.textContent = selectionMode ? t('contacts.cancelSelect') : t('contacts.selectToggle');
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

    // Export button — uses filtered endpoint so the CSV matches what the user sees
    document.getElementById('contacts-export-btn')?.addEventListener('click', async () => {
        const btn = document.getElementById('contacts-export-btn');
        btn.disabled = true;
        btn.textContent = t('contacts.exportLoading');
        try {
            // Build URL with current filters — server returns ALL matching rows, not just loaded page
            const params = new URLSearchParams();
            if (currentQuery) params.set('q', currentQuery);
            if (currentDepartment) params.set('department', currentDepartment);
            if (currentNafCode) params.set('naf_code', currentNafCode);
            const url = `/api/export/contacts/csv?${params.toString()}`;
            const resp = await fetch(url, { credentials: 'same-origin' });
            if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
            const blob = await resp.blob();
            const blobUrl = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = blobUrl;
            // Use filename from Content-Disposition header if available
            const disp = resp.headers.get('Content-Disposition') || '';
            const match = disp.match(/filename=([^;]+)/);
            a.download = match ? match[1].trim() : 'contacts_export.csv';
            a.click();
            URL.revokeObjectURL(blobUrl);
            showToast(t('contacts.exportSuccess'), 'success');
        } catch (err) {
            showToast(t('contacts.exportError', { error: err.message }), 'error');
        } finally {
            btn.disabled = false;
            btn.textContent = t('contacts.exportCSV');
        }
    });

    // Initial load
    doSearch();
}
