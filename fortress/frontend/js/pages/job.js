/**
 * Job Page — Drill-down into a specific job
 */

import { getJob, getJobCompanies, getJobQuality, getExportUrl, deleteJob, untagCompany, enrichCompany, startDeepEnrich } from '../api.js';
import { renderGauge, companyCard, renderPagination, breadcrumb, statusBadge, formatDateTime, escapeHtml, showConfirmModal, showToast } from '../components.js';
import { GlobalSelection } from '../state.js';

// ── Selection state ──────────────────────────────────────────────
let selectionMode = false;
let selectedSirens = GlobalSelection;
let _currentBatchId = null;
let _currentPage = 1;
let _currentSort = 'completude';

export async function renderJob(container, batchId) {
    batchId = decodeURIComponent(batchId);

    const [job, quality] = await Promise.all([
        getJob(batchId),
        getJobQuality(batchId),
    ]);

    if (!job || job._ok === false || job.error) {
        const isServerError = job && job._ok === false && job._status >= 500;
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">${isServerError ? '⚠️' : '❌'}</div>
                <div class="empty-state-text">${isServerError ? 'Serveur temporairement indisponible' : 'Job introuvable'}</div>
                <p style="color:var(--text-muted)">${isServerError ? 'Veuillez réessayer dans quelques instants.' : ''}</p>
                <a href="#/" class="btn btn-primary">Retour au Dashboard</a>
            </div>
        `;
        return;
    }

    const batchSize = job.batch_size || job.total_companies || 1;
    const scraped = job.companies_scraped || 0;
    const qualified = job.companies_qualified || 0;
    const progressPct = Math.min(100, Math.round((qualified / batchSize) * 100));
    const q = quality || {};

    container.innerHTML = `
        ${breadcrumb([
        { label: 'Dashboard', href: '#/' },
        { label: job.batch_name },
    ])}

        <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:var(--space-xl); flex-wrap:wrap; margin-bottom:var(--space-2xl)">
            <div>
                <h1 class="page-title">
                    ${escapeHtml(job.batch_name)}
                    ${job.batch_number ? `<span style="font-size:var(--font-sm); font-weight:400; color:var(--text-muted); margin-left:var(--space-sm)">Batch #${job.batch_number}</span>` : ''}
                </h1>
                <div style="display:flex; align-items:center; gap:var(--space-md); margin-top:var(--space-sm)">
                    ${statusBadge(job.status)}
                    <span style="color:var(--text-secondary); font-size:var(--font-sm)">
                        Créé le ${formatDateTime(job.created_at)}
                    </span>
                    ${(job.triage_green || 0) > 0 ? `<span class="badge" style="background:rgba(34,197,94,0.15); color:rgb(34,197,94); border:1px solid rgba(34,197,94,0.3)">🟢 ${job.triage_green} données existantes</span>` : ''}
                </div>
            </div>
            <div style="display:flex; gap:var(--space-sm)">
                <a href="${getExportUrl(batchId, 'csv')}" class="btn btn-secondary" download>📥 CSV</a>
                <a href="${getExportUrl(batchId, 'xlsx')}" class="btn btn-secondary" download>📥 XLSX</a>
                <a href="${getExportUrl(batchId, 'jsonl')}" class="btn btn-secondary" download>📥 JSONL</a>
                ${job.status !== 'in_progress' ? `<button id="btn-rerun" class="btn btn-secondary" title="Relancer ce batch">🔄 Relancer</button>` : ''}
                <button id="btn-delete-job" class="btn btn-secondary" title="Supprimer ce batch" style="color:var(--danger)">🗑️</button>
                ${job.status === 'in_progress' ?
            `<a href="#/monitor/${encodeURIComponent(batchId)}" class="btn btn-primary">📡 Suivi Live</a>` : ''}
            </div>
        </div>

        ${job.shortfall_reason ? `
        <div style="display:flex; align-items:flex-start; gap:var(--space-md); padding:var(--space-md) var(--space-lg); background:rgba(99,179,237,0.08); border:1px solid rgba(99,179,237,0.3); border-radius:var(--radius-md); margin-bottom:var(--space-xl); font-size:var(--font-sm); color:var(--text-secondary)">
            <span style="font-size:16px; flex-shrink:0">ℹ️</span>
            <span>${escapeHtml(job.shortfall_reason)}</span>
        </div>` : ''}

        <!-- Progress -->
        <div class="card" style="margin-bottom:var(--space-xl)">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:var(--space-md)">
                <span style="font-weight:600">Progression — ${batchSize} entreprises</span>
                <div style="display:flex; align-items:center; gap:var(--space-md)">
                    <span style="color:var(--text-secondary); font-weight:500">${qualified} entreprise${qualified !== 1 ? 's' : ''} trouvée${qualified !== 1 ? 's' : ''}</span>
                    ${(job.pending_links || 0) > 0 ? `<span class="badge" style="background:rgba(245,158,11,0.15); color:rgb(245,158,11); border:1px solid rgba(245,158,11,0.3)">⏳ ${job.pending_links} lien${job.pending_links > 1 ? 's' : ''} en attente</span>` : ''}
                    <button id="toggle-provenance" title="Détails du traitement" style="background:none;border:none;cursor:pointer;font-size:14px;opacity:0.4;transition:opacity 0.2s" onmouseover="this.style.opacity=1" onmouseout="this.style.opacity=0.4">ℹ️</button>
                </div>
            </div>
            ${job.status === 'in_progress' ? `
            <div class="progress-bar" style="height:10px">
                <div class="progress-bar-fill progress-bar-accent animated" style="width:${progressPct}%"></div>
            </div>` : ''}
            <div id="provenance-panel" style="display:none; margin-top:var(--space-lg); padding:var(--space-lg); background:var(--bg-secondary); border-radius:var(--radius-sm); border:1px solid var(--border-subtle)">
                <div style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-md)">
                    Détails du traitement
                </div>
                ${scraped > 0 ? `<div style="font-size:var(--font-sm); color:var(--text-secondary); margin-bottom:var(--space-md)">
                    🔍 <strong>${scraped}</strong> entreprises évaluées — <strong>${qualified}</strong> retenues, <strong>${scraped - qualified}</strong> ignorées car déjà enrichies par Maps ou sans correspondance.
                </div>` : ''}
                ${(job.triage_green || 0) > 0 ? `<div style="font-size:var(--font-sm); color:rgb(34,197,94); margin-bottom:var(--space-md)">
                    ♻️ <strong>${job.triage_green}</strong> entreprise${job.triage_green > 1 ? 's étaient' : ' était'} déjà dans la base de données avec des données Maps complètes.
                </div>` : ''}
                <div id="provenance-sources" style="display:flex; gap:var(--space-xl); flex-wrap:wrap; font-size:var(--font-sm)">
                    <span style="color:var(--text-secondary)">Chargement…</span>
                </div>
            </div>
        </div>

        <!-- Quality Gauges -->
        <div class="card" style="margin-bottom:var(--space-xl)">
            <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                Qualité des données
            </h3>
            <div style="display:flex; gap:var(--space-2xl); justify-content:center; flex-wrap:wrap">
                ${renderGauge(q.phone_pct || 0, '📞 Téléphone')}
                ${renderGauge(q.email_pct || 0, '✉️ Email')}
                ${renderGauge(q.website_pct || 0, '🌐 Site web')}
                ${renderGauge(q.officers_pct || 0, '👤 Dirigeants')}
                ${renderGauge(q.financials_pct || 0, '💰 Financier')}
                ${renderGauge(q.siret_pct || q.social_pct || 0, '🔗 Social')}
            </div>
            <div style="text-align:center; font-size:var(--font-sm); color:var(--text-muted); margin-top:var(--space-lg)">
                ${batchSize} entreprises collectées
            </div>
        </div>

        <!-- Departments touched -->
        ${job.departments && job.departments.length > 0 ? `
            <div class="card" style="margin-bottom:var(--space-xl)">
                <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                    Départements couverts
                </h3>
                <div style="display:flex; gap:var(--space-sm); flex-wrap:wrap">
                    ${job.departments.map(d => `
                        <a href="#/department/${d.departement}" class="badge badge-accent" style="cursor:pointer; text-decoration:none">
                            ${d.departement} (${d.company_count})
                        </a>
                    `).join('')}
                </div>
            </div>
        ` : ''}

        <!-- Companies -->
        <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:var(--space-lg)">
            <h2 style="font-size:var(--font-lg); font-weight:600">Entreprises</h2>
            <div style="display:flex; gap:var(--space-sm)">
                <button id="btn-select-mode" class="btn-select-mode" title="Sélectionner">
                    ☑ Sélectionner
                </button>
                <select id="job-sort" style="background:var(--bg-input); border:1px solid var(--border-default); border-radius:var(--radius-sm); padding:var(--space-sm) var(--space-md); color:var(--text-primary); font-family:var(--font-family); font-size:var(--font-sm)">
                    <option value="completude">Tri: Complétude</option>
                    <option value="name">Tri: Nom</option>
                    <option value="date">Tri: Date création</option>
                </select>
            </div>
        </div>
        <div id="job-companies-container">
            <div class="loading"><div class="spinner"></div></div>
        </div>
    `;

    // Reset selection state for this job
    selectionMode = false;
    selectedSirens.clear();
    _currentBatchId = batchId;
    _currentPage = 1;
    _currentSort = 'completude';

    // Load companies
    await loadCompanies(batchId, 1, 'completude');

    // Provenance panel toggle
    const toggleBtn = document.getElementById('toggle-provenance');
    let provenanceLoaded = false;
    if (toggleBtn) {
        toggleBtn.addEventListener('click', () => {
            const panel = document.getElementById('provenance-panel');
            if (!panel) return;
            const visible = panel.style.display !== 'none';
            panel.style.display = visible ? 'none' : 'block';
            toggleBtn.style.opacity = visible ? '0.4' : '1';
            // Populate sources on first open
            if (!visible && !provenanceLoaded && q.sources) {
                provenanceLoaded = true;
                const container = document.getElementById('provenance-sources');
                if (!container) return;
                const icons = {
                    maps_lookup: '🗺️ Google Maps',
                    website_crawl: '🌐 Site web',
                    web_search: '🔍 Recherche web',
                    inpi_lookup: '📋 INPI',
                };
                const entries = Object.entries(q.sources);
                if (entries.length === 0) {
                    container.innerHTML = '<span style="color:var(--text-muted)">Aucune donnée de source disponible</span>';
                    return;
                }
                container.innerHTML = entries.map(([action, data]) => {
                    const label = icons[action] || action;
                    const color = data.rate >= 70 ? 'var(--success)' : data.rate >= 40 ? 'var(--warning)' : 'var(--text-muted)';
                    return `<div style="display:flex;flex-direction:column;gap:2px;padding:var(--space-sm) var(--space-md);background:var(--bg-elevated);border-radius:var(--radius-sm);min-width:180px">
                        <span style="font-weight:600">${label}</span>
                        <span style="color:${color}">${data.success}/${data.total} réussies (${data.rate}%)</span>
                    </div>`;
                }).join('');
            }
        });
    }

    // Sort change handler
    document.getElementById('job-sort').addEventListener('change', (e) => {
        loadCompanies(batchId, 1, e.target.value);
    });

    // Selection mode toggle
    _setupSelectionMode(batchId);

    // Delete button
    const deleteBtn = document.getElementById('btn-delete-job');
    if (deleteBtn) {
        deleteBtn.addEventListener('click', () => {
            showConfirmModal({
                title: '🗑️ Supprimer ce batch ?',
                body: `
                    <p><strong>Batch :</strong> ${escapeHtml(job.batch_name)}</p>
                    <p><strong>Créé le :</strong> ${formatDateTime(job.created_at)}</p>
                    <p><strong>${scraped}</strong> entreprises collectées</p>
                    <p style="color:var(--danger)">⚠️ <strong>Suppression complète :</strong> contacts enrichis, historique d'audit et tags seront effacés.</p>
                    <p style="color:var(--text-muted)">Les fiches entreprises (SIRENE) et les données importées manuellement restent dans la base.</p>
                `,
                confirmLabel: 'Supprimer définitivement',
                danger: true,
                onConfirm: async () => {
                    const result = await deleteJob(batchId);
                    if (result._ok !== false) {
                        const msg = `Batch supprimé : ${result.deleted_contacts || 0} contacts, ${result.sirens_affected || 0} entreprises nettoyées`;
                        showToast(msg, 'success');
                        window.location.hash = '#/';
                    } else {
                        showToast(result.error || 'Erreur lors de la suppression', 'error');
                    }
                },
            });
        });
    }

    // Rerun button
    const rerunBtn = document.getElementById('btn-rerun');
    if (rerunBtn) {
        rerunBtn.addEventListener('click', () => {
            // Pre-fill new batch form from original job params
            const params = new URLSearchParams();
            if (job.filters_json) {
                const f = typeof job.filters_json === 'string' ? JSON.parse(job.filters_json) : job.filters_json;
                if (f.sector) params.set('sector', f.sector);
                if (f.department) params.set('department', f.department);
                if (f.size) params.set('size', f.size);
                if (f.mode) params.set('mode', f.mode);
                if (f.naf_code) params.set('naf_code', f.naf_code);
                if (f.city) params.set('city', f.city);
            }
            window.location.hash = `#/new-batch?${params.toString()}`;
        });
    }

}

async function loadCompanies(batchId, page, sort) {
    _currentPage = page;
    _currentSort = sort;
    const companiesContainer = document.getElementById('job-companies-container');
    companiesContainer.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

    const data = await getJobCompanies(batchId, { page, pageSize: 20, sort });
    if (!data || !data.companies || data.companies.length === 0) {
        // Context-aware empty state
        const job = await getJob(batchId).catch(() => null);
        const greenCount = job?.triage_green || 0;
        const batchName = job?.batch_name || '';

        if (greenCount > 0) {
            // All-green: all companies were already Maps-enriched
            companiesContainer.innerHTML = `
                <div style="padding:var(--space-2xl); background:var(--bg-secondary); border-radius:var(--radius-md); border:1px solid rgba(34,197,94,0.3); text-align:center; max-width:560px; margin:0 auto">
                    <div style="font-size:2.5rem; margin-bottom:var(--space-lg)">✅</div>
                    <div style="font-size:var(--font-lg); font-weight:600; color:rgb(34,197,94); margin-bottom:var(--space-md)">
                        Toutes les entreprises sont déjà enrichies
                    </div>
                    <p style="color:var(--text-secondary); margin-bottom:var(--space-lg)">
                        Les <strong>${greenCount}</strong> entreprises trouvées pour <em>${escapeHtml(batchName)}</em> ont déjà été enrichies par Google Maps dans notre base de données.
                    </p>
                    <p style="color:var(--text-muted); font-size:var(--font-sm); margin-bottom:var(--space-xl)">
                        Pour rechercher de nouvelles entreprises, essayez un secteur ou un département différent,
                        ou attendez que de nouvelles entreprises s'enregistrent dans SIRENE.
                    </p>
                    <a href="#/new-batch" class="btn btn-primary">🚀 Nouvelle recherche</a>
                </div>
            `;
        } else {
            companiesContainer.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">📭</div>
                    <div class="empty-state-text">Aucune entreprise trouvée</div>
                </div>
            `;
        }
        return;
    }

    const totalPages = Math.ceil((data.total || 0) / (data.page_size || 20));

    // Group companies by search_query if present
    const hasQueryGroups = data.companies.some(c => c.search_query);
    let gridContent = '';

    if (hasQueryGroups) {
        // Group by search_query
        const groups = new Map();
        for (const c of data.companies) {
            const key = c.search_query || '(sans requête)';
            if (!groups.has(key)) groups.set(key, []);
            groups.get(key).push(c);
        }
        gridContent = [...groups.entries()].map(([query, companies]) => `
            <div style="margin-bottom:var(--space-lg)">
                <div style="display:flex; align-items:center; gap:var(--space-sm); margin-bottom:var(--space-md); padding:var(--space-sm) var(--space-md); background:var(--bg-secondary); border-radius:var(--radius-sm); border-left:3px solid var(--accent)">
                    <span style="font-size:var(--font-sm); color:var(--accent); font-weight:600">🔍 ${escapeHtml(query)}</span>
                    <span style="font-size:var(--font-xs); color:var(--text-muted); margin-left:auto">
                        ${companies.length} résultat${companies.length > 1 ? 's' : ''}
                        ${(() => { const p = companies.filter(c => c.link_confidence === 'pending').length; return p > 0 ? `<span style="color:rgb(245,158,11); margin-left:6px">⏳ ${p} en attente</span>` : ''; })()}
                    </span>
                </div>
                <div class="company-grid">
                    ${companies.map(c => companyCard(c, {
                        removable: !selectionMode,
                        selectable: selectionMode,
                        checked: selectedSirens.has(c.siren),
                    })).join('')}
                </div>
            </div>
        `).join('');
    } else {
        gridContent = `
            <div class="company-grid">
                ${data.companies.map(c => companyCard(c, {
                    removable: !selectionMode,
                    selectable: selectionMode,
                    checked: selectedSirens.has(c.siren),
                })).join('')}
            </div>
        `;
    }

    companiesContainer.innerHTML = `
        <div id="job-company-grid">
            ${gridContent}
        </div>
        ${renderPagination(data.page, totalPages, (p) => loadCompanies(batchId, p, sort))}
    `;

    // Restore checkbox state after re-render
    if (selectionMode) {
        document.querySelectorAll('.card-checkbox').forEach(cb => {
            if (selectedSirens.has(cb.dataset.siren)) {
                cb.checked = true;
                cb.closest('.company-card')?.classList.add('card-selected');
            }
        });
        _updateBulkBar();
    }

    // Event delegation for × remove buttons
    const grid = document.getElementById('job-company-grid');
    if (grid) {
        grid.addEventListener('click', (e) => {
            // Remove button handler
            const btn = e.target.closest('.card-remove-btn');
            if (btn) {
                e.stopPropagation();
                const siren = btn.dataset.siren;
                const card = btn.closest('.company-card');
                const name = card?.querySelector('.company-card-name')?.textContent || siren;

                showConfirmModal({
                    title: 'Supprimer cette entreprise ?',
                    body: `<p>Retirer <strong>${escapeHtml(name)}</strong> de ce batch</p>`,
                    confirmLabel: 'Supprimer',
                    danger: true,
                    checkboxLabel: 'Également ajouter à la liste noire (ne plus jamais scraper)',
                    onConfirm: async (checkboxChecked) => {
                        const result = await untagCompany(siren, batchId);
                        if (result._ok !== false) {
                            if (checkboxChecked) {
                                try {
                                    await fetch('/api/blacklist', {
                                        method: 'POST',
                                        headers: { 'Content-Type': 'application/json' },
                                        body: JSON.stringify({ siren, reason: 'Supprimé manuellement' }),
                                        credentials: 'same-origin',
                                    });
                                } catch { /* best effort */ }
                            }
                            showToast(`${name} retirée`, 'success');
                            if (card) {
                                card.classList.add('card-fade-out');
                                card.addEventListener('animationend', async () => {
                                    card.remove();
                                    await loadCompanies(_currentBatchId, _currentPage, _currentSort);
                                });
                            } else {
                                await loadCompanies(_currentBatchId, _currentPage, _currentSort);
                            }
                        } else {
                            showToast('Erreur lors du retrait', 'error');
                        }
                    },
                });
                return;
            }
        });

        // Checkbox handler for selection mode
        grid.addEventListener('change', (e) => {
            const cb = e.target.closest('.card-checkbox');
            if (!cb) return;
            const siren = cb.dataset.siren;
            const card = cb.closest('.company-card');
            if (cb.checked) {
                selectedSirens.add(siren);
                card?.classList.add('card-selected');
            } else {
                selectedSirens.delete(siren);
                card?.classList.remove('card-selected');
            }
            _updateBulkBar();
        });
    }
}

// ── Selection mode toggle ────────────────────────────────────────
function _setupSelectionMode(batchId) {
    const btn = document.getElementById('btn-select-mode');
    if (!btn) return;
    btn.addEventListener('click', () => {
        selectionMode = !selectionMode;
        btn.classList.toggle('active', selectionMode);
        btn.innerHTML = selectionMode ? '✖ Annuler' : '☑ Sélectionner';
        if (!selectionMode) {
            selectedSirens.clear();
            _removeBulkBar();
        }
        // Re-render cards with/without checkboxes
        loadCompanies(batchId, _currentPage, _currentSort);
    });
}

// ── Floating action bar ─────────────────────────────────────────
function _updateBulkBar() {
    let bar = document.getElementById('bulk-action-bar');
    if (selectedSirens.size === 0) {
        _removeBulkBar();
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
        <button class="btn btn-secondary" id="bulk-select-all">Tout sélectionner</button>
        <button class="btn btn-primary" id="bulk-enrich-web">🚀 Enrichir via site web</button>
        <button class="btn btn-danger" id="bulk-delete">🗑️ Supprimer</button>
    `;

    // Select all on current page
    document.getElementById('bulk-select-all').onclick = () => {
        const grid = document.getElementById('job-company-grid');
        if (!grid) return;
        const boxes = grid.querySelectorAll('.card-checkbox');
        const allChecked = [...boxes].every(b => b.checked);
        boxes.forEach(b => {
            b.checked = !allChecked;
            const siren = b.dataset.siren;
            const card = b.closest('.company-card');
            if (!allChecked) {
                selectedSirens.add(siren);
                card?.classList.add('card-selected');
            } else {
                selectedSirens.delete(siren);
                card?.classList.remove('card-selected');
            }
        });
        _updateBulkBar();
    };

    // Enrich via web
    document.getElementById('bulk-enrich-web').onclick = async () => {
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
            _removeBulkBar();
            await loadCompanies(_currentBatchId, _currentPage, _currentSort);
        } else {
            showToast("Erreur lors de l'enrichissement", 'error');
        }
    };

    // Delete
    document.getElementById('bulk-delete').onclick = () => _bulkDelete();
}

function _removeBulkBar() {
    const bar = document.getElementById('bulk-action-bar');
    if (bar) bar.remove();
}


async function _bulkDelete() {
    const sirens = [...selectedSirens];
    if (!sirens.length) return;
    showConfirmModal({
        title: `Supprimer ${sirens.length} entreprise${sirens.length > 1 ? 's' : ''} ?`,
        body: `<p>Retirer ${sirens.length} entreprise${sirens.length > 1 ? 's' : ''} de ce batch</p>`,
        confirmLabel: 'Supprimer',
        danger: true,
        checkboxLabel: 'Également ajouter à la liste noire',
        onConfirm: async (blacklist) => {
            let ok = 0;
            for (const siren of sirens) {
                const res = await untagCompany(siren, _currentBatchId);
                if (res && res._ok !== false) ok++;
                if (blacklist) {
                    await fetch('/api/blacklist', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({siren, reason: 'Supprimé en masse'}),
                        credentials: 'same-origin',
                    });
                }
            }
            showToast(`${ok}/${sirens.length} supprimée(s)`, 'success');
            selectionMode = false;
            selectedSirens.clear();
            _removeBulkBar();
            await loadCompanies(_currentBatchId, _currentPage, _currentSort);
        }
    });
}
