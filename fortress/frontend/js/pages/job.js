/**
 * Job Page — Drill-down into a specific job
 */

import { getJob, getJobCompanies, getJobQuality, getExportUrl, deleteJob, retryJob, resumeBatch, untagCompany, enrichCompany } from '../api.js';
import { renderGauge, companyCard, renderPagination, breadcrumb, statusBadge, formatDateTime, escapeHtml, showConfirmModal, showToast } from '../components.js';

// ── Selection state ──────────────────────────────────────────────
let selectionMode = false;
const selectedSirens = new Set();
let _currentQueryId = null;
let _currentPage = 1;
let _currentSort = 'completude';

export async function renderJob(container, queryId) {
    queryId = decodeURIComponent(queryId);

    const [job, quality] = await Promise.all([
        getJob(queryId),
        getJobQuality(queryId),
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
        { label: job.query_name },
    ])}

        <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:var(--space-xl); flex-wrap:wrap; margin-bottom:var(--space-2xl)">
            <div>
                <h1 class="page-title">${escapeHtml(job.query_name)}</h1>
                <div style="display:flex; align-items:center; gap:var(--space-md); margin-top:var(--space-sm)">
                    ${statusBadge(job.status)}
                    <span style="color:var(--text-secondary); font-size:var(--font-sm)">
                        Créé le ${formatDateTime(job.created_at)}
                    </span>
                    ${job.batch_number > 1 ? `<span class="badge badge-accent">Batch #${job.batch_number}</span>` : ''}
                    ${(job.triage_green || 0) > 0 ? `<span class="badge" style="background:rgba(34,197,94,0.15); color:rgb(34,197,94); border:1px solid rgba(34,197,94,0.3)">🟢 ${job.triage_green} données existantes</span>` : ''}
                </div>
            </div>
            <div style="display:flex; gap:var(--space-sm)">
                <a href="${getExportUrl(queryId, 'csv')}" class="btn btn-secondary" download>📥 CSV</a>
                <a href="${getExportUrl(queryId, 'xlsx')}" class="btn btn-secondary" download>📥 XLSX</a>
                <a href="${getExportUrl(queryId, 'jsonl')}" class="btn btn-secondary" download>📥 JSONL</a>
                ${job.status !== 'in_progress' ? `<button id="btn-rerun" class="btn btn-secondary" title="Relancer ce batch">🔄 Relancer</button>` : ''}
                ${job.status === 'failed' ? `<button id="btn-retry" class="btn btn-primary" title="Réessayer ce batch">🔁 Réessayer</button>` : ''}
                ${['interrupted', 'failed'].includes(job.status) ? `<button id="btn-resume" class="btn btn-primary" title="Reprendre là où le batch s'est arrêté">▶️ Reprendre</button>` : ''}
                <button id="btn-delete-job" class="btn btn-secondary" title="Supprimer ce batch" style="color:var(--danger)">🗑️</button>
                ${job.status === 'in_progress' ?
            `<a href="#/monitor/${encodeURIComponent(queryId)}" class="btn btn-primary">📡 Suivi Live</a>` : ''}
            </div>
        </div>

        <!-- Progress -->
        <div class="card" style="margin-bottom:var(--space-xl)">
            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:var(--space-md)">
                <span style="font-weight:600">Progression — ${batchSize} entreprises</span>
                <div style="display:flex; align-items:center; gap:var(--space-md)">
                    <span style="color:var(--text-secondary)">${qualified}/${batchSize} qualifiées (${scraped} tentées)</span>
                    <button id="toggle-provenance" title="D'où viennent ces données ?" style="background:none;border:none;cursor:pointer;font-size:14px;opacity:0.4;transition:opacity 0.2s" onmouseover="this.style.opacity=1" onmouseout="this.style.opacity=0.4">ℹ️</button>
                </div>
            </div>
            ${job.status === 'in_progress' ? `
            <div class="progress-bar" style="height:10px">
                <div class="progress-bar-fill progress-bar-accent animated" style="width:${progressPct}%"></div>
            </div>` : ''}
            <div id="provenance-panel" style="display:none; margin-top:var(--space-lg); padding:var(--space-lg); background:var(--bg-secondary); border-radius:var(--radius-sm); border:1px solid var(--border-subtle)">
                <div style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-md)">
                    Sources des données
                </div>
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
            <div style="display:flex; gap:var(--space-2xl); justify-content:center">
                ${renderGauge(q.phone_pct || 0, '📞 Téléphone')}
                ${renderGauge(q.email_pct || 0, '✉️ Email')}
                ${renderGauge(q.website_pct || 0, '🌐 Site web')}
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
    _currentQueryId = queryId;
    _currentPage = 1;
    _currentSort = 'completude';

    // Load companies
    await loadCompanies(queryId, 1, 'completude');

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
        loadCompanies(queryId, 1, e.target.value);
    });

    // Selection mode toggle
    _setupSelectionMode(queryId);

    // Delete button
    const deleteBtn = document.getElementById('btn-delete-job');
    if (deleteBtn) {
        deleteBtn.addEventListener('click', () => {
            showConfirmModal({
                title: '🗑️ Supprimer ce batch ?',
                body: `
                    <p><strong>Batch :</strong> ${escapeHtml(job.query_name)}</p>
                    <p><strong>Créé le :</strong> ${formatDateTime(job.created_at)}</p>
                    <p><strong>${scraped}</strong> entreprises collectées</p>
                    <p style="color:var(--danger)">⚠️ <strong>Suppression complète :</strong> contacts enrichis, historique d'audit et tags seront effacés.</p>
                    <p style="color:var(--text-muted)">Les fiches entreprises (SIRENE) et les données importées manuellement restent dans la base.</p>
                `,
                confirmLabel: 'Supprimer définitivement',
                danger: true,
                onConfirm: async () => {
                    const result = await deleteJob(queryId);
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

    // Retry button (for failed jobs — resets and re-runs same job)
    const retryBtn = document.getElementById('btn-retry');
    if (retryBtn) {
        retryBtn.addEventListener('click', () => {
            showConfirmModal({
                title: '🔁 Réessayer ce batch ?',
                body: `<p>Le batch <strong>${escapeHtml(job.query_name)}</strong> sera relancé avec les mêmes paramètres.</p>
                       <p>⚠️ La progression sera réinitialisée à 0.</p>
                       <p>✅ Les données déjà collectées restent dans la base.</p>`,
                confirmLabel: 'Réessayer',
                danger: false,
                onConfirm: async () => {
                    const result = await retryJob(queryId);
                    if (result && result.retried) {
                        showToast('Batch relancé avec succès', 'success');
                        window.location.hash = `#/monitor/${encodeURIComponent(queryId)}`;
                    } else {
                        showToast(result?.error || 'Erreur lors du retry', 'error');
                    }
                },
            });
        });
    }

    const resumeBtn = document.getElementById('btn-resume');
    if (resumeBtn) {
        resumeBtn.addEventListener('click', () => {
            showConfirmModal({
                title: '▶️ Reprendre ce batch ?',
                body: `<p>Le batch <strong>${escapeHtml(job.query_name)}</strong> reprendra là où il s'est arrêté.</p>
                       <p>✅ Les ${scraped} entreprises déjà traitées seront ignorées.</p>
                       <p>✅ Seules les entreprises restantes seront collectées.</p>`,
                confirmLabel: 'Reprendre',
                danger: false,
                onConfirm: async () => {
                    const result = await resumeBatch(queryId);
                    if (result && result.status === 'resumed') {
                        showToast('Batch repris avec succès', 'success');
                        window.location.hash = `#/monitor/${encodeURIComponent(queryId)}`;
                    } else {
                        showToast(result?.error || 'Erreur lors de la reprise', 'error');
                    }
                },
            });
        });
    }
}

async function loadCompanies(queryId, page, sort) {
    _currentPage = page;
    _currentSort = sort;
    const companiesContainer = document.getElementById('job-companies-container');
    companiesContainer.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

    const data = await getJobCompanies(queryId, { page, pageSize: 20, sort });
    if (!data || !data.companies || data.companies.length === 0) {
        companiesContainer.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">📭</div>
                <div class="empty-state-text">Aucune entreprise trouvée</div>
            </div>
        `;
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
                    <span style="font-size:var(--font-xs); color:var(--text-muted); margin-left:auto">${companies.length} résultat${companies.length > 1 ? 's' : ''}</span>
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
        ${renderPagination(data.page, totalPages, (p) => loadCompanies(queryId, p, sort))}
    `;

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
                    title: '× Retirer cette entreprise ?',
                    body: `
                        <p><strong>${escapeHtml(name)}</strong></p>
                        <p>SIREN: ${siren}</p>
                        <p style="color:var(--text-muted)">L'entreprise sera retirée de cette requête.
                        Ses données restent dans la base.</p>
                    `,
                    confirmLabel: 'Retirer',
                    danger: true,
                    onConfirm: async () => {
                        const result = await untagCompany(siren, queryId);
                        if (result._ok !== false) {
                            showToast(`${name} retirée`, 'success');
                            if (card) {
                                card.classList.add('card-fade-out');
                                card.addEventListener('animationend', () => card.remove());
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
function _setupSelectionMode(queryId) {
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
        loadCompanies(queryId, _currentPage, _currentSort);
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
        <button class="btn btn-primary" id="bulk-enrich-maps">🗺️ Maps</button>
        <button class="btn btn-secondary" id="bulk-enrich-crawl">🌐 Site Web</button>
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

    // Enrich Maps
    document.getElementById('bulk-enrich-maps').onclick = () => _bulkEnrich(['maps_lookup']);
    // Enrich Crawl
    document.getElementById('bulk-enrich-crawl').onclick = () => _bulkEnrich(['website_crawl']);
    // Delete
    document.getElementById('bulk-delete').onclick = () => _bulkDelete();
}

function _removeBulkBar() {
    const bar = document.getElementById('bulk-action-bar');
    if (bar) bar.remove();
}

async function _bulkEnrich(modules) {
    const sirens = [...selectedSirens];
    const label = modules.includes('maps_lookup') ? 'Maps' : 'Site Web';
    showToast(`⏳ Enrichissement ${label} de ${sirens.length} entreprise(s)…`, 'info');

    let ok = 0, fail = 0;
    for (let i = 0; i < sirens.length; i++) {
        const siren = sirens[i];
        showToast(`⏳ ${label} : ${i + 1}/${sirens.length}…`, 'info');
        try {
            const res = await enrichCompany(siren, modules);
            if (res._ok !== false) ok++; else fail++;
        } catch { fail++; }
    }

    showToast(`✅ ${ok} enrichie(s)${fail ? `, ❌ ${fail} échouée(s)` : ''}`, ok > 0 ? 'success' : 'error');
    selectedSirens.clear();
    selectionMode = false;
    _removeBulkBar();
    const btn = document.getElementById('btn-select-mode');
    if (btn) { btn.classList.remove('active'); btn.innerHTML = '☑ Sélectionner'; }
    await loadCompanies(_currentQueryId, _currentPage, _currentSort);
}

async function _bulkDelete() {
    const sirens = [...selectedSirens];
    showConfirmModal({
        title: `🗑️ Retirer ${sirens.length} entreprise(s) ?`,
        body: `<p>Les entreprises seront retirées de cette requête.<br>Leurs données restent dans la base.</p>`,
        confirmLabel: 'Retirer',
        danger: true,
        onConfirm: async () => {
            let ok = 0;
            for (const siren of sirens) {
                try {
                    const res = await untagCompany(siren, _currentQueryId);
                    if (res._ok !== false) ok++;
                } catch { /* skip */ }
            }
            showToast(`${ok} entreprise(s) retirée(s)`, 'success');
            selectedSirens.clear();
            selectionMode = false;
            _removeBulkBar();
            const btn = document.getElementById('btn-select-mode');
            if (btn) { btn.classList.remove('active'); btn.innerHTML = '☑ Sélectionner'; }
            await loadCompanies(_currentQueryId, _currentPage, _currentSort);
        },
    });
}
