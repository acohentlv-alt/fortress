/**
 * Dashboard Page — Dual view: By Location / By Job (grouped)
 *
 * "By Job" groups same-name queries together (case-insensitive),
 * sorted by most recent batch, with timeline of all batches.
 */

import { getDashboardStats, getDepartments, getJobs, getDashboardStatsByJob, checkHealth, extractApiError } from '../api.js';
import { renderGauge, statusBadge, formatDateTime, escapeHtml, showToast } from '../components.js';

const API_BASE = '/api';

export async function renderDashboard(container) {
    // Show loading state
    container.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

    let stats, departments, jobs;

    try {
        [stats, departments, jobs] = await Promise.all([
            getDashboardStats(),
            getDepartments(),
            getJobs(),
        ]);
    } catch {
        stats = null;
        departments = null;
        jobs = null;
    }
    // Helper: check if a response is an API error (not a valid data payload)
    const isErr = (r) => !r || (r._ok === false);

    // Check if ALL calls failed (503, network error, etc.)
    const allFailed = isErr(stats) && isErr(departments) && isErr(jobs);
    if (allFailed) {
        const errorMsg = extractApiError(stats || departments);
        container.innerHTML = `
            <h1 class="page-title">Dashboard</h1>
            <p class="page-subtitle">Vue d'ensemble de vos données B2B</p>
            <div class="error-state text-center" style="margin-top:var(--space-2xl); padding:var(--space-2xl)">
                <div style="font-size:3rem; margin-bottom:var(--space-lg)">🔌</div>
                <div style="font-size:var(--font-md); color:var(--text-secondary); margin-bottom:var(--space-lg)">
                    ${escapeHtml(errorMsg)}
                </div>
                <button id="retry-dashboard" class="btn btn-primary" style="margin-top:var(--space-md)">
                    🔄 Réessayer
                </button>
            </div>
        `;
        document.getElementById('retry-dashboard').addEventListener('click', async (e) => {
            e.target.disabled = true;
            e.target.textContent = '⏳ Vérification...';
            const health = await checkHealth();
            if (health.ok) {
                renderDashboard(container);
            } else {
                showToast('Le serveur est toujours inaccessible.', 'error');
                e.target.disabled = false;
                e.target.textContent = '🔄 Réessayer';
            }
        });
        return;
    }

    // Normalize: ensure departments/jobs are arrays (may be error objects if partially failed)
    if (!Array.isArray(departments)) departments = [];
    if (!Array.isArray(jobs)) jobs = [];

    const s = stats || {};

    container.innerHTML = `
        <h1 class="page-title">Dashboard</h1>
        <p class="page-subtitle">Vue d'ensemble de vos données B2B</p>

        <!-- Stats Bar -->
        <div class="stats-grid">
            <div class="stat-card">
                <span class="stat-card-icon">🏢</span>
                <span class="stat-card-value">${(s.total_companies || 0).toLocaleString('fr-FR')}</span>
                <span class="stat-card-label">Entreprises</span>
            </div>
            <div class="stat-card">
                <span class="stat-card-icon">📞</span>
                <span class="stat-card-value">${(s.with_phone || 0).toLocaleString('fr-FR')}</span>
                <span class="stat-card-label">Téléphones</span>
            </div>
            <div class="stat-card">
                <span class="stat-card-icon">✉️</span>
                <span class="stat-card-value">${(s.with_email || 0).toLocaleString('fr-FR')}</span>
                <span class="stat-card-label">Emails</span>
            </div>
            <div class="stat-card">
                <span class="stat-card-icon">🌐</span>
                <span class="stat-card-value">${(s.with_website || 0).toLocaleString('fr-FR')}</span>
                <span class="stat-card-label">Sites web</span>
            </div>
        </div>

        <!-- Master Export -->
        <div style="display:flex; gap:var(--space-md); margin-bottom:var(--space-xl)">
            <button class="btn btn-primary" id="btn-master-export" style="display:flex; align-items:center; gap:var(--space-sm)">
                📥 Télécharger l'export global
            </button>
        </div>

        <!-- View Toggle -->
        <div class="view-toggle">
            <button class="view-toggle-btn active" id="btn-by-location">📍 Par Localisation</button>
            <button class="view-toggle-btn" id="btn-by-job">📋 Par Job</button>
        </div>

        <!-- View Container -->
        <div id="dashboard-view"></div>
    `;

    // Render initial view
    renderByLocation(departments);

    // Master Export handler
    document.getElementById('btn-master-export').addEventListener('click', () => {
        window.open(`${API_BASE}/export/master/csv`, '_blank');
    });

    // Toggle handlers
    document.getElementById('btn-by-location').addEventListener('click', () => {
        setActiveToggle('btn-by-location');
        renderByLocation(departments);
    });

    document.getElementById('btn-by-job').addEventListener('click', async () => {
        setActiveToggle('btn-by-job');
        const byJobData = await getDashboardStatsByJob();
        if (byJobData && Array.isArray(byJobData) && byJobData.length > 0) {
            renderByJobFromAPI(byJobData);
        } else {
            renderByJob(jobs);
        }
    });
}

function setActiveToggle(activeId) {
    document.querySelectorAll('.view-toggle-btn').forEach(b => b.classList.remove('active'));
    document.getElementById(activeId).classList.add('active');
}

// ── By Location View ─────────────────────────────────────────────
function renderByLocation(departments) {
    const view = document.getElementById('dashboard-view');
    if (!departments || departments.length === 0) {
        view.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">📍</div>
                <div class="empty-state-text">Aucun département trouvé</div>
                <p style="color: var(--text-muted)">Lancez un batch pour commencer à collecter des données</p>
            </div>
        `;
        return;
    }

    view.innerHTML = `
        <div class="dept-grid">
            ${departments.map(d => `
                <div class="dept-card" onclick="window.location.hash='#/department/${d.departement}'">
                    <div class="dept-card-header">
                        <span class="dept-card-number">${escapeHtml(d.departement)}</span>
                        <span class="dept-card-count">${d.company_count} entreprise${d.company_count > 1 ? 's' : ''}</span>
                    </div>
                    <div class="dept-card-name">${escapeHtml(d.department_name)}</div>
                    <div class="dept-card-gauges">
                        ${renderGauge(d.phone_pct || 0, '📞 Tél.')}
                        ${renderGauge(d.email_pct || 0, '✉️ Email')}
                        ${renderGauge(d.website_pct || 0, '🌐 Web')}
                    </div>
                </div>
            `).join('')}
        </div>
    `;
}

// ── By Job View — From normalized API (no client-side grouping needed) ────
function renderByJobFromAPI(groups) {
    const view = document.getElementById('dashboard-view');
    if (!groups || groups.length === 0) {
        _renderJobEmptyState(view);
        return;
    }

    // The API returns pre-grouped, uppercase-normalized data
    const groupCards = groups.map((g, idx) => _renderGroupCard(g, idx));

    view.innerHTML = `
        <div class="job-groups-list">
            ${groupCards.join('')}
        </div>
    `;
}

// ── By Job View — Fallback: client-side grouping ─────────────────
function renderByJob(jobs) {
    const view = document.getElementById('dashboard-view');
    if (!jobs || jobs.length === 0) {
        _renderJobEmptyState(view);
        return;
    }

    // Group by query_name (case-insensitive)
    const groups = {};
    for (const j of jobs) {
        const key = (j.query_name || '').toUpperCase().trim();
        if (!groups[key]) groups[key] = { display_name: j.query_name, batches: [] };
        groups[key].batches.push(j);
    }

    // Sort each group's batches by date (newest first)
    for (const g of Object.values(groups)) {
        g.batches.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
        g.display_name = g.batches[0].query_name;
    }

    // Sort groups by their newest batch date
    const sortedGroups = Object.values(groups)
        .sort((a, b) => new Date(b.batches[0].created_at) - new Date(a.batches[0].created_at));

    const groupCards = sortedGroups.map((g, idx) => _renderGroupCard(g, idx));

    view.innerHTML = `
        <div class="job-groups-list">
            ${groupCards.join('')}
        </div>
    `;
}

function _renderJobEmptyState(view) {
    view.innerHTML = `
        <div class="empty-state">
            <div class="empty-state-icon">📋</div>
            <div class="empty-state-text">Aucun job trouvé</div>
            <p style="color: var(--text-muted)">Lancez un batch pour commencer</p>
        </div>
    `;
}

function _renderGroupCard(g, idx) {
    const batches = g.batches || [];
    const totalScraped = batches.reduce((s, b) => s + (b.companies_scraped || 0), 0);
    const totalTarget = (batches[0] || {}).total_companies || 0;
    const overallPct = Math.min(100, Math.round((totalScraped / Math.max(totalTarget, 1)) * 100));
    const latest = batches[0] || {};
    const hasRunning = batches.some(b => b.status === 'in_progress');
    const batchCount = batches.length;
    const groupId = `jobgroup-${idx}`;

    // Use display_name directly — already uppercase from API
    const displayName = escapeHtml(g.display_name || '');

    return `
        <div class="job-group-card">
            <div class="job-group-header" onclick="document.getElementById('${groupId}').classList.toggle('expanded')">
                <div class="job-group-info">
                    <div class="job-group-name">${displayName}</div>
                    <div class="job-group-meta">
                        <span>${batchCount} batch${batchCount > 1 ? 'es' : ''}</span>
                        <span>·</span>
                        <span>${totalScraped}/${totalTarget} scrapées</span>
                        <span>·</span>
                        <span>Dernier : ${formatDateTime(latest.created_at)}</span>
                    </div>
                </div>
                <div class="job-group-right">
                    ${hasRunning ? statusBadge('in_progress') : statusBadge(latest.status)}
                    <div style="width:100px">
                        <div class="progress-bar">
                            <div class="progress-bar-fill ${hasRunning ? 'animated' : ''}"
                                 style="width:${overallPct}%"></div>
                        </div>
                    </div>
                    <span class="job-group-chevron">▼</span>
                </div>
            </div>

            <div class="job-group-timeline" id="${groupId}">
                <div class="job-timeline-inner">
                    ${batches.map((b, bIdx) => {
        const bScraped = b.companies_scraped || 0;
        const bTotal = b.total_companies || 1;
        const bPct = Math.round((bScraped / bTotal) * 100);
        return `
                        <div class="job-timeline-item" onclick="event.stopPropagation(); window.location.hash='#/job/${encodeURIComponent(b.query_id)}'">
                            <div class="timeline-dot ${bIdx === 0 ? 'latest' : ''}"></div>
                            <div class="timeline-content">
                                <div class="timeline-batch-id">${escapeHtml(b.query_id)}</div>
                                <div class="timeline-meta">
                                    <span>${formatDateTime(b.created_at)}</span>
                                    <span>${bScraped}/${bTotal} scrapées</span>
                                    ${b.wave_total ? `<span>Vague ${b.wave_current || 0}/${b.wave_total}</span>` : ''}
                                </div>
                            </div>
                            <div class="timeline-status">
                                ${statusBadge(b.status)}
                                <div style="width:60px">
                                    <div class="progress-bar" style="height:3px">
                                        <div class="progress-bar-fill" style="width:${bPct}%"></div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    `;
    }).join('')}
                </div>
            </div>
        </div>
    `;
}
