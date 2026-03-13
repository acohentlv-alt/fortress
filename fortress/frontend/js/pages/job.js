/**
 * Job Page — Drill-down into a specific job
 */

import { getJob, getJobCompanies, deleteJob, untagCompany } from '../api.js';
import { renderGauge, companyCard, renderPagination, breadcrumb, statusBadge, formatDateTime, escapeHtml, showConfirmModal, showToast } from '../components.js';

export async function renderJob(container, queryId) {
    queryId = decodeURIComponent(queryId);

    const [job, quality] = await Promise.all([
        getJob(queryId),
        getJobQuality(queryId),
    ]);

    if (!job || job.error) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">❌</div>
                <div class="empty-state-text">Job introuvable</div>
                <a href="#/" class="btn btn-primary">Retour au Dashboard</a>
            </div>
        `;
        return;
    }

    const batchSize = job.batch_size || job.total_companies || 1;
    const scraped = job.companies_scraped || 0;
    const progressPct = Math.min(100, Math.round((scraped / batchSize) * 100));
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
                </div>
            </div>
            <div style="display:flex; gap:var(--space-sm)">
                <a href="${getExportUrl(queryId, 'csv')}" class="btn btn-secondary" download>📥 CSV</a>
                <a href="${getExportUrl(queryId, 'jsonl')}" class="btn btn-secondary" download>📥 JSONL</a>
                ${job.status !== 'in_progress' ? `<button id="btn-rerun" class="btn btn-secondary" title="Relancer ce batch">🔄 Relancer</button>` : ''}
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
                    <span style="color:var(--text-secondary)">${scraped}/${batchSize} (${progressPct}%)</span>
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
                ${q.total || 0} entreprises au total
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
                    <p style="color:var(--warning)">⚠️ Les tags de recherche seront supprimés.</p>
                    <p style="color:var(--success)">✅ Les fiches entreprises et contacts resteront dans la base.</p>
                `,
                confirmLabel: 'Supprimer',
                danger: true,
                onConfirm: async () => {
                    const result = await deleteJob(queryId);
                    if (result._ok !== false) {
                        showToast('Batch supprimé avec succès', 'success');
                        window.location.hash = '#/';
                    } else {
                        showToast('Erreur lors de la suppression', 'error');
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

async function loadCompanies(queryId, page, sort) {
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
    companiesContainer.innerHTML = `
        <div class="company-grid" id="job-company-grid">
            ${data.companies.map(c => companyCard(c, { removable: true })).join('')}
        </div>
        ${renderPagination(data.page, totalPages, (p) => loadCompanies(queryId, p, sort))}
    `;

    // Event delegation for × remove buttons
    const grid = document.getElementById('job-company-grid');
    if (grid) {
        grid.addEventListener('click', (e) => {
            const btn = e.target.closest('.card-remove-btn');
            if (!btn) return;
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
                        // Fade out the card
                        if (card) {
                            card.classList.add('card-fade-out');
                            card.addEventListener('animationend', () => card.remove());
                        }
                    } else {
                        showToast('Erreur lors du retrait', 'error');
                    }
                },
            });
        });
    }
}
