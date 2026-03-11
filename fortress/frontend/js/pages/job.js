/**
 * Job Page — Drill-down into a specific job
 */

import { getJob, getJobCompanies, getJobQuality, getExportUrl } from '../api.js';
import {
    breadcrumb, statusBadge, companyCard, renderGauge,
    formatDateTime, escapeHtml, renderPagination,
} from '../components.js';

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
                ${job.status === 'in_progress' ?
            `<a href="#/monitor/${encodeURIComponent(queryId)}" class="btn btn-primary">📡 Suivi Live</a>` : ''}
            </div>
        </div>

        <!-- Progress -->
        <div class="card" style="margin-bottom:var(--space-xl)">
            <div style="display:flex; justify-content:space-between; margin-bottom:var(--space-md)">
                <span style="font-weight:600">Progression</span>
                <span style="color:var(--text-secondary)">${scraped}/${batchSize} (${progressPct}%)</span>
            </div>
            <div class="progress-bar" style="height:10px">
                <div class="progress-bar-fill ${job.status === 'in_progress' ? 'animated' : ''}"
                     style="width:${progressPct}%"></div>
            </div>
            <div style="display:flex; gap:var(--space-xl); margin-top:var(--space-lg); font-size:var(--font-sm); color:var(--text-secondary)">
                <span>🟢 Green: ${job.triage_green || 0}</span>
                <span>🟡 Yellow: ${job.triage_yellow || 0}</span>
                <span>🔴 Red: ${job.triage_red || 0}</span>
                <span>⚫ Black: ${job.triage_black || 0}</span>
                ${job.triage_blue ? `<span style="color:var(--info)">🔵 Blue: ${job.triage_blue}</span>` : ''}
                ${job.wave_total ? `<span>Vague: ${job.wave_current || 0}/${job.wave_total}</span>` : ''}
                ${job.replaced_count ? `<span style="color:var(--warning)">🔄 Remplacées: ${job.replaced_count}</span>` : ''}
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
                ${renderGauge(q.siret_pct || 0, '🏢 SIRET')}
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

    // Sort change handler
    document.getElementById('job-sort').addEventListener('change', (e) => {
        loadCompanies(queryId, 1, e.target.value);
    });
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
        <div class="company-grid">
            ${data.companies.map(c => companyCard(c)).join('')}
        </div>
        ${renderPagination(data.page, totalPages, (p) => loadCompanies(queryId, p, sort))}
    `;
}
