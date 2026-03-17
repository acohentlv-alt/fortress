/**
 * Dashboard Page — Dual view: By Location / By Job (grouped)
 *
 * "By Job" groups same-name queries together (case-insensitive),
 * sorted by most recent batch, with timeline of all batches.
 */

import { getDashboardStats, getDepartments, getJobs, getDashboardStatsByJob, getDataBank, getSectorStats, getAnalysis, deleteSectorTags, deleteDeptTags, deleteJobGroup, checkHealth, extractApiError, getCachedUser } from '../api.js';
import { renderGauge, statusBadge, formatDateTime, escapeHtml, showToast, showConfirmModal } from '../components.js';

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
    const user = getCachedUser();

    container.innerHTML = `


        <!-- Welcome Banner -->
        <div style="display:flex; align-items:center; justify-content:space-between; gap:var(--space-lg); margin-bottom:var(--space-xl); flex-wrap:wrap">
            <div>
                <h1 class="page-title" style="margin-bottom:var(--space-xs)">Bonjour${user ? ' ' + escapeHtml(user.display_name || user.username) : ''} 👋</h1>
                <p class="page-subtitle" style="margin-bottom:0">
                    ${(s.total_companies || 0).toLocaleString('fr-FR')} entreprises enrichies
                </p>
            </div>
            <div style="display:flex; gap:var(--space-sm); flex-wrap:wrap">
                <a href="#/new-batch" class="btn btn-primary" style="display:flex; align-items:center; gap:var(--space-sm)">🚀 Nouvelle Recherche</a>
                <a href="#/monitor" class="btn btn-secondary" style="display:flex; align-items:center; gap:var(--space-sm)">📡 Pipeline Live</a>
                <button class="btn btn-secondary" id="btn-master-export" style="display:flex; align-items:center; gap:var(--space-sm)">📥 Exporter</button>
            </div>
        </div>



        <!-- View Toggle -->
        <div class="view-toggle">
            <button class="view-toggle-btn active" id="btn-analysis">📊 Analyse</button>
            <button class="view-toggle-btn" id="btn-by-job">📋 Par Recherche</button>
        </div>

        <!-- View Container -->
        <div id="dashboard-view"><div class="loading"><div class="spinner"></div></div></div>
    `;

    // Render initial view — Analysis is default
    _loadAnalysisView(container);

    // Master Export handler
    document.getElementById('btn-master-export').addEventListener('click', () => {
        window.open(`${API_BASE}/export/master/csv`, '_blank');
    });

    // Toggle handlers
    document.getElementById('btn-analysis').addEventListener('click', () => {
        setActiveToggle('btn-analysis');
        _loadAnalysisView(container);
    });

    document.getElementById('btn-by-job').addEventListener('click', async () => {
        setActiveToggle('btn-by-job');
        const byJobData = await getDashboardStatsByJob();
        if (byJobData && Array.isArray(byJobData) && byJobData.length > 0) {
            renderByJobFromAPI(byJobData, container);
        } else {
            renderByJob(jobs, container);
        }
    });
}

async function _loadAnalysisView(rootContainer) {
    const view = document.getElementById('dashboard-view');
    view.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
    const data = await getAnalysis();
    if (!data || data._ok === false) {
        view.innerHTML = '<div class="empty-state"><div class="empty-state-icon">⚠️</div><div class="empty-state-text">Erreur de chargement</div></div>';
        return;
    }
    const isAdmin = getCachedUser()?.role === 'admin';
    renderAnalysis(data, isAdmin, rootContainer);
}

function setActiveToggle(activeId) {
    document.querySelectorAll('.view-toggle-btn').forEach(b => b.classList.remove('active'));
    document.getElementById(activeId).classList.add('active');
}

// ── By Location View ─────────────────────────────────────────────
function renderByLocation(departments, rootContainer) {
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
                <div class="dept-card" style="position:relative" data-dept="${escapeHtml(d.departement)}">
                    <button class="card-delete-btn" data-delete-type="dept" data-delete-id="${escapeHtml(d.departement)}" data-delete-label="${escapeHtml(d.department_name)} (${d.company_count} entreprises)"
                        onclick="event.stopPropagation()" title="Supprimer ce département">✕</button>
                    <div onclick="window.location.hash='#/department/${d.departement}'" style="cursor:pointer">
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
                </div>
            `).join('')}
        </div>
    `;

    // Wire delete buttons
    view.querySelectorAll('.card-delete-btn[data-delete-type="dept"]').forEach(btn => {
        btn.addEventListener('click', () => {
            const dept = btn.dataset.deleteId;
            const label = btn.dataset.deleteLabel;
            showConfirmModal({
                title: '🗑️ Supprimer le département',
                body: `<p>Supprimer toutes les données du département <strong>${label}</strong> du dashboard ?</p>
                       <p style="color:var(--text-muted);font-size:var(--font-xs)">Les données des entreprises ne sont pas supprimées — seuls les tags du dashboard sont retirés.</p>`,
                confirmLabel: 'Supprimer',
                danger: true,
                onConfirm: async () => {
                    const result = await deleteDeptTags(dept);
                    if (result._ok) {
                        showToast(`Département ${dept} supprimé (${result.tags_removed} tags)`, 'success');
                        renderDashboard(rootContainer);
                    } else {
                        showToast(extractApiError(result), 'error');
                    }
                },
            });
        });
    });
}

// ── By Job View — From normalized API (no client-side grouping needed) ────
function renderByJobFromAPI(groups, rootContainer) {
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

    _wireJobGroupDeleteButtons(view, rootContainer);
}

// ── By Job View — Fallback: client-side grouping ─────────────────
function renderByJob(jobs, rootContainer) {
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

    _wireJobGroupDeleteButtons(view, rootContainer);
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

function _wireJobGroupDeleteButtons(view, rootContainer) {
    view.querySelectorAll('.card-delete-btn[data-delete-type="job-group"]').forEach(btn => {
        btn.addEventListener('click', () => {
            const queryName = btn.dataset.deleteId;
            const label = btn.dataset.deleteLabel;
            showConfirmModal({
                title: '🗑️ Supprimer le groupe de recherche',
                body: `<p>Supprimer le groupe <strong>${label}</strong> et tous ses batches du dashboard ?</p>
                       <p style="color:var(--text-muted);font-size:var(--font-xs)">Les données des entreprises ne sont pas supprimées — seuls les tags et les jobs sont retirés.</p>`,
                confirmLabel: 'Supprimer',
                danger: true,
                onConfirm: async () => {
                    const result = await deleteJobGroup(queryName);
                    if (result._ok) {
                        showToast(`Groupe ${queryName} supprimé (${result.jobs_deleted} jobs, ${result.tags_removed} tags)`, 'success');
                        renderDashboard(rootContainer);
                    } else {
                        showToast(extractApiError(result), 'error');
                    }
                },
            });
        });
    });
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
    const queryName = g.display_name || g.query_name || '';

    return `
        <div class="job-group-card" style="position:relative" data-group-name="${escapeHtml(queryName)}">
            <button class="card-delete-btn" data-delete-type="job-group" data-delete-id="${escapeHtml(queryName)}" data-delete-label="${displayName} (${batchCount} batch${batchCount > 1 ? 'es' : ''})"
                onclick="event.stopPropagation()" title="Supprimer ce groupe">✕</button>
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

// ── By Sector View — From accurate API ─────────────────────────────
function renderBySectorFromAPI(sectors, rootContainer) {
    const view = document.getElementById('dashboard-view');
    if (!sectors || sectors.length === 0) {
        _renderSectorEmptyState(view);
        return;
    }

    view.innerHTML = `
        <div class="dept-grid">
            ${sectors.map(s => {
                const total = s.companies || 0;
                const phonePct = total > 0 ? Math.round(100 * (s.with_phone || 0) / total) : 0;
                const emailPct = total > 0 ? Math.round(100 * (s.with_email || 0) / total) : 0;
                const webPct = total > 0 ? Math.round(100 * (s.with_website || 0) / total) : 0;
                const depts = (s.departments || []).filter(Boolean);
                const hasRunning = s.has_running || false;

                return `
                    <div class="dept-card" style="position:relative" data-sector="${escapeHtml(s.sector)}">
                        <button class="card-delete-btn" data-delete-type="sector" data-delete-id="${escapeHtml(s.sector)}" data-delete-label="${escapeHtml(s.sector)} (${total} entreprises)"
                            onclick="event.stopPropagation()" title="Supprimer ce secteur">✕</button>
                        <div onclick="window.location.hash='#/search?q=${encodeURIComponent(s.sector)}'" style="cursor:pointer">
                            <div class="dept-card-header">
                                <span class="dept-card-number">🏭</span>
                                <span class="dept-card-count">${total} entreprise${total > 1 ? 's' : ''}</span>
                            </div>
                            <div class="dept-card-name">${escapeHtml(s.sector)}</div>
                            <div style="font-size:var(--font-xs); color:var(--text-muted); margin-top:var(--space-xs)">
                                ${s.batch_count || 0} batch${(s.batch_count || 0) > 1 ? 'es' : ''}
                                ${depts.length > 0 ? ` · ${depts.slice(0, 3).join(', ')}${depts.length > 3 ? '…' : ''}` : ''}
                                ${hasRunning ? ' · ⏳ En cours' : ''}
                            </div>
                            <div class="dept-card-gauges">
                                ${renderGauge(phonePct, '📞 Tél.')}
                                ${renderGauge(emailPct, '✉️ Email')}
                                ${renderGauge(webPct, '🌐 Web')}
                            </div>
                        </div>
                    </div>
                `;
            }).join('')}
        </div>
    `;

    // Wire delete buttons
    view.querySelectorAll('.card-delete-btn[data-delete-type="sector"]').forEach(btn => {
        btn.addEventListener('click', () => {
            const sector = btn.dataset.deleteId;
            const label = btn.dataset.deleteLabel;
            showConfirmModal({
                title: '🗑️ Supprimer le secteur',
                body: `<p>Supprimer toutes les données du secteur <strong>${label}</strong> du dashboard ?</p>
                       <p style="color:var(--text-muted);font-size:var(--font-xs)">Les données des entreprises ne sont pas supprimées — seuls les tags sont retirés.</p>`,
                confirmLabel: 'Supprimer',
                danger: true,
                onConfirm: async () => {
                    const result = await deleteSectorTags(sector);
                    if (result._ok) {
                        showToast(`Secteur ${sector} supprimé (${result.tags_removed} tags)`, 'success');
                        renderDashboard(rootContainer);
                    } else {
                        showToast(extractApiError(result), 'error');
                    }
                },
            });
        });
    });
}

function _renderSectorEmptyState(view) {
    view.innerHTML = `
        <div class="empty-state">
            <div class="empty-state-icon">🏭</div>
            <div class="empty-state-text">Aucun secteur trouvé</div>
            <p style="color: var(--text-muted)">Lancez un batch pour commencer</p>
        </div>
    `;
}

// ── By Sector View — Fallback: client-side grouping ──────────────
function renderBySector(jobs, rootContainer) {
    const view = document.getElementById('dashboard-view');
    if (!jobs || jobs.length === 0) {
        _renderSectorEmptyState(view);
        return;
    }

    // Group by sector (first word of query_name, uppercased)
    const sectors = {};
    for (const j of jobs) {
        const sector = (j.sector || (j.query_name || '').split(' ')[0]).toUpperCase().trim();
        if (!sector) continue;
        if (!sectors[sector]) sectors[sector] = { name: sector, batches: [], totalScraped: 0 };
        sectors[sector].batches.push(j);
        sectors[sector].totalScraped += (j.companies_scraped || 0);
    }

    // Sort by total scraped (most data first)
    const sorted = Object.values(sectors).sort((a, b) => b.totalScraped - a.totalScraped);

    view.innerHTML = `
        <div class="dept-grid">
            ${sorted.map(s => {
                const batchCount = s.batches.length;
                const hasRunning = s.batches.some(b => b.status === 'in_progress');
                const depts = [...new Set(s.batches.map(b => {
                    const parts = (b.query_name || '').split(' ');
                    return parts.length > 1 ? parts.slice(1).join(' ') : '';
                }).filter(Boolean))];

                return `
                    <div class="dept-card" style="position:relative" data-sector="${escapeHtml(s.name)}">
                        <button class="card-delete-btn" data-delete-type="sector" data-delete-id="${escapeHtml(s.name)}" data-delete-label="${escapeHtml(s.name)} (${s.totalScraped} entreprises)"
                            onclick="event.stopPropagation()" title="Supprimer ce secteur">✕</button>
                        <div onclick="window.location.hash='#/search?q=${encodeURIComponent(s.name)}'" style="cursor:pointer">
                            <div class="dept-card-header">
                                <span class="dept-card-number">🏭</span>
                                <span class="dept-card-count">${s.totalScraped} entreprise${s.totalScraped > 1 ? 's' : ''}</span>
                            </div>
                            <div class="dept-card-name">${escapeHtml(s.name)}</div>
                            <div style="font-size:var(--font-xs); color:var(--text-muted); margin-top:var(--space-xs)">
                                ${batchCount} batch${batchCount > 1 ? 'es' : ''}
                                ${depts.length > 0 ? ` · ${depts.slice(0, 3).join(', ')}${depts.length > 3 ? '…' : ''}` : ''}
                                ${hasRunning ? ' · ⏳ En cours' : ''}
                            </div>
                        </div>
                    </div>
                `;
            }).join('')}
        </div>
    `;

    // Wire delete buttons
    view.querySelectorAll('.card-delete-btn[data-delete-type="sector"]').forEach(btn => {
        btn.addEventListener('click', () => {
            const sector = btn.dataset.deleteId;
            const label = btn.dataset.deleteLabel;
            showConfirmModal({
                title: '🗑️ Supprimer le secteur',
                body: `<p>Supprimer toutes les données du secteur <strong>${label}</strong> du dashboard ?</p>
                       <p style="color:var(--text-muted);font-size:var(--font-xs)">Les données des entreprises ne sont pas supprimées — seuls les tags sont retirés.</p>`,
                confirmLabel: 'Supprimer',
                danger: true,
                onConfirm: async () => {
                    const result = await deleteSectorTags(sector);
                    if (result._ok) {
                        showToast(`Secteur ${sector} supprimé (${result.tags_removed} tags)`, 'success');
                        renderDashboard(rootContainer);
                    } else {
                        showToast(extractApiError(result), 'error');
                    }
                },
            });
        });
    });
}

// ── Data Analysis View (default tab) ─────────────────────────────
function renderAnalysis(data, isAdmin) {
    const view = document.getElementById('dashboard-view');
    const q = data.quality || {};
    const enrichers = data.enrichers || {};
    const timeline = data.timeline || [];
    const recentJobs = data.recent_jobs || [];
    const sectors = data.sectors || [];
    const systemUsage = data.system_usage || [];

    // Info tooltip helper
    const info = (text) => `<span class="info-tooltip" title="${escapeHtml(text)}">ℹ️</span>`;

    // ── Panel 1: Quality Score Overview ──────────────────────────
    const overallScore = q.overall_score || 0;
    const qualityPanel = `
        <div class="card analysis-panel" style="grid-column: 1 / -1">
            <h3 class="analysis-panel-title">📊 Qualité des données ${info('Score global = moyenne de (téléphone% + email% + web%). Basé sur les entreprises taguées dans vos recherches.')}</h3>
            <div style="display:flex; align-items:center; gap:var(--space-2xl); flex-wrap:wrap">
                <div style="flex-shrink:0">
                    ${renderGauge(overallScore, 'Score global')}
                </div>
                <div style="flex:1; min-width:300px; display:flex; flex-direction:column; gap:var(--space-md)">
                    ${_metricBar('📞 Téléphone', q.with_phone || 0, q.total || 0, q.phone_pct || 0, 'Entreprises avec un numéro de téléphone trouvé via Google Maps')}
                    ${_metricBar('✉️ Email', q.with_email || 0, q.total || 0, q.email_pct || 0, 'Entreprises avec un email trouvé via crawl du site web')}
                    ${_metricBar('🌐 Site web', q.with_website || 0, q.total || 0, q.website_pct || 0, 'Entreprises avec un site web trouvé via Google Maps')}
                    ${_metricBar('🔗 Réseaux sociaux', q.with_social || 0, q.total || 0, q.social_pct || 0, 'Entreprises avec LinkedIn ou Facebook trouvé via crawl du site')}
                </div>
                <div style="flex-shrink:0; text-align:center; padding:var(--space-lg)">
                    <div style="font-size:2.5rem; font-weight:800; color:var(--text-primary)">${(q.total || 0).toLocaleString('fr-FR')}</div>
                    <div style="font-size:var(--font-sm); color:var(--text-muted)">entreprises au total</div>
                </div>
            </div>
        </div>
    `;

    // ── Panel 2: Enricher Performance (admin only) ──────────────
    let enricherPanel = '';
    if (isAdmin && Object.keys(enrichers).length > 0) {
        const maps = enrichers.maps_lookup || {};
        const crawl = enrichers.website_crawl || {};
        const outcomes = enrichers.outcomes || {};

        enricherPanel = `
            <div class="card analysis-panel" style="grid-column: 1 / -1">
                <h3 class="analysis-panel-title">🔧 Performance des enrichisseurs ${info('Statistiques issues de enrichment_log et scrape_audit. Montre le taux de succès et temps moyen de chaque module Python.')}</h3>
                <div style="display:grid; grid-template-columns:1fr 1fr; gap:var(--space-lg)">
                    <!-- Maps Enricher -->
                    <div style="background:var(--bg-tertiary); border-radius:var(--radius); padding:var(--space-lg)">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:var(--space-md)">
                            <span style="font-weight:700; font-size:var(--font-sm)">🗺️ Google Maps ${info('playwright_maps_scraper.py — recherche chaque entreprise sur Google Maps')}</span>
                            <span style="font-weight:700; font-size:var(--font-sm)">${maps.rate || 0}%</span>
                        </div>
                        <div style="display:flex; gap:var(--space-lg); font-size:var(--font-xs); color:var(--text-secondary); margin-bottom:var(--space-sm)">
                            <span>${(maps.total || 0).toLocaleString('fr-FR')} tentatives</span>
                            <span>${(maps.success || 0).toLocaleString('fr-FR')} succès</span>
                            <span>~${Math.round((maps.avg_time_ms || 0) / 1000)}s moy.</span>
                        </div>
                        <div style="height:8px; background:var(--bg-input); border-radius:4px; overflow:hidden; margin-bottom:var(--space-md)">
                            <div style="height:100%; width:${maps.rate || 0}%; background:var(--accent); border-radius:4px; transition:width 0.5s ease"></div>
                        </div>
                        ${_renderMethodBreakdown(maps.methods || {}, 'qualified')}
                    </div>

                    <!-- Crawl Enricher -->
                    <div style="background:var(--bg-tertiary); border-radius:var(--radius); padding:var(--space-lg)">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:var(--space-md)">
                            <span style="font-weight:700; font-size:var(--font-sm)">🌐 Website Crawl ${info('curl_client.py — crawl le site web pour extraire emails et réseaux sociaux')}</span>
                            <span style="font-weight:700; font-size:var(--font-sm)">${crawl.rate || 0}%</span>
                        </div>
                        <div style="display:flex; gap:var(--space-lg); font-size:var(--font-xs); color:var(--text-secondary); margin-bottom:var(--space-sm)">
                            <span>${(crawl.total || 0).toLocaleString('fr-FR')} tentatives</span>
                            <span>${(crawl.success || 0).toLocaleString('fr-FR')} succès</span>
                            <span>~${Math.round((crawl.avg_time_ms || 0) / 1000)}s moy.</span>
                        </div>
                        <div style="height:8px; background:var(--bg-input); border-radius:4px; overflow:hidden; margin-bottom:var(--space-md)">
                            <div style="height:100%; width:${crawl.rate || 0}%; background:var(--accent); border-radius:4px; transition:width 0.5s ease"></div>
                        </div>
                        ${_renderMethodBreakdown(crawl.methods || {}, 'with_emails')}
                    </div>
                </div>

                <!-- Outcomes summary -->
                ${Object.keys(outcomes).length > 0 ? `
                    <div style="margin-top:var(--space-lg); display:flex; gap:var(--space-md); flex-wrap:wrap">
                        ${Object.entries(outcomes).map(([k, v]) => {
                            const icons = { qualified: '✅', sirene_only: '📄', replaced: '🔄', failed: '❌' };
                            return `<span class="badge badge-muted" style="font-size:var(--font-xs)">${icons[k] || '•'} ${escapeHtml(k)}: ${v}</span>`;
                        }).join('')}
                    </div>
                ` : ''}
            </div>
        `;
    }

    // ── Panel 3: Sector Quality Table ────────────────────────────
    const sectorPanel = `
        <div class="card analysis-panel">
            <h3 class="analysis-panel-title">🏭 Qualité par secteur ${info('Chaque secteur = premier mot du nom de recherche. % = entreprises avec cette donnée / total entreprises du secteur.')}</h3>
            ${sectors.length === 0 ? '<p style="color:var(--text-muted)">Aucun secteur</p>' : `
                <div style="overflow-x:auto">
                    <table class="analysis-table">
                        <thead>
                            <tr>
                                <th style="text-align:left">Secteur</th>
                                <th style="text-align:right">Entreprises</th>
                                <th style="text-align:right">📞 Tél.</th>
                                <th style="text-align:right">✉️ Email</th>
                                <th style="text-align:right">🌐 Web</th>
                                <th style="text-align:right">Score</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${sectors.map(s => `
                                <tr class="analysis-table-row" onclick="window.location.hash='#/search?q=${encodeURIComponent(s.sector)}'">
                                    <td style="font-weight:600">${escapeHtml(s.sector)}</td>
                                    <td style="text-align:right">${(s.companies || 0).toLocaleString('fr-FR')}</td>
                                    <td style="text-align:right">${s.phone_pct}%</td>
                                    <td style="text-align:right">${s.email_pct}%</td>
                                    <td style="text-align:right">${s.web_pct}%</td>
                                    <td style="text-align:right; font-weight:600">${s.quality_score}%</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            `}
        </div>
    `;

    // ── Panel 4: Recent Jobs ─────────────────────────────────────
    const jobsPanel = `
        <div class="card analysis-panel" style="grid-column: 1 / -1">
            <h3 class="analysis-panel-title">📋 Historique des recherches ${info('Qualité = moyenne de (tél% + email% + web%) pour chaque recherche. Clique sur une ligne pour voir le détail du job.')}</h3>
            ${recentJobs.length === 0 ? '<p style="color:var(--text-muted)">Aucun job terminé</p>' : `
                <div style="overflow-x:auto">
                    <table class="analysis-table">
                        <thead>
                            <tr>
                                <th style="text-align:left">Recherche</th>
                                <th>Statut</th>
                                <th style="text-align:right">Entreprises</th>
                                <th style="text-align:right">📞</th>
                                <th style="text-align:right">✉️</th>
                                <th style="text-align:right">🌐</th>
                                <th style="text-align:right">Score</th>
                                <th>Utilisateur</th>
                                <th style="text-align:right">Date</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${recentJobs.map(j => `
                                <tr class="analysis-table-row" onclick="window.location.hash='#/job/${encodeURIComponent(j.query_id)}'">
                                    <td style="font-weight:600; max-width:200px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap" title="${escapeHtml(j.query_name)}">${escapeHtml(j.query_name)}</td>
                                    <td>${statusBadge(j.status)}</td>
                                    <td style="text-align:right">${j.unique_companies || j.companies_scraped || 0}</td>
                                    <td style="text-align:right">${j.phone_pct}%</td>
                                    <td style="text-align:right">${j.email_pct}%</td>
                                    <td style="text-align:right">${j.web_pct}%</td>
                                    <td style="text-align:right; font-weight:600">${j.quality_score}%</td>
                                    <td style="font-size:var(--font-xs); color:var(--text-secondary)">${escapeHtml(j.user_name || '—')}</td>
                                    <td style="text-align:right; white-space:nowrap; font-size:var(--font-xs); color:var(--text-muted)">${formatDateTime(j.created_at)}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            `}
        </div>
    `;

    // ── Panel 5: Activity Timeline ──────────────────────────────
    const maxBatches = Math.max(1, ...timeline.map(t => t.batches || 0));
    const maxCompanies = Math.max(1, ...timeline.map(t => t.companies || 0));
    const timelinePanel = `
        <div class="card analysis-panel" style="grid-column: 1 / -1">
            <h3 class="analysis-panel-title">📈 Activité hebdomadaire ${info('Nombre de batches et d\'entreprises traitées par semaine (12 dernières semaines)')}</h3>
            ${timeline.length === 0 ? '<p style="color:var(--text-muted)">Aucune activité récente</p>' : `
                <div style="display:flex; gap:var(--space-sm); align-items:flex-end; height:120px; padding-top:var(--space-md)">
                    ${timeline.map(t => {
                        const bHeight = Math.max(4, Math.round(100 * (t.batches || 0) / maxBatches));
                        const cHeight = Math.max(4, Math.round(100 * (t.companies || 0) / maxCompanies));
                        const weekLabel = (t.week || '').replace(/^\d{4}-W/, 'S');
                        return `
                            <div style="flex:1; display:flex; flex-direction:column; align-items:center; gap:2px" title="${t.week}: ${t.batches} batches, ${t.companies || 0} entreprises">
                                <div style="display:flex; gap:2px; align-items:flex-end; height:100px; width:100%">
                                    <div style="flex:1; height:${bHeight}%; background:var(--accent); border-radius:3px 3px 0 0; min-height:4px; transition:height 0.3s ease"></div>
                                    <div style="flex:1; height:${cHeight}%; background:var(--text-muted); border-radius:3px 3px 0 0; min-height:4px; opacity:0.5; transition:height 0.3s ease"></div>
                                </div>
                                <span style="font-size:10px; color:var(--text-muted); text-align:center; writing-mode:horizontal-tb">${weekLabel}</span>
                            </div>
                        `;
                    }).join('')}
                </div>
                <div style="display:flex; gap:var(--space-lg); margin-top:var(--space-md); font-size:var(--font-xs); color:var(--text-muted)">
                    <span><span style="display:inline-block; width:10px; height:10px; background:var(--accent); border-radius:2px; vertical-align:middle; margin-right:4px"></span> Batches</span>
                    <span><span style="display:inline-block; width:10px; height:10px; background:var(--text-muted); border-radius:2px; vertical-align:middle; margin-right:4px; opacity:0.5"></span> Entreprises</span>
                </div>
            `}
        </div>
    `;

    // ── Panel 6: System Usage ───────────────────────────────────
    const systemPanel = `
        <div class="card analysis-panel" style="grid-column: 1 / -1">
            <h3 class="analysis-panel-title">🖥️ Utilisation du système ${info('Historique complet des batches : qui a lancé quoi, quand, avec quel worker, et combien de temps ça a pris.')}</h3>
            ${systemUsage.length === 0 ? '<p style="color:var(--text-muted)">Aucun batch</p>' : `
                <div style="overflow-x:auto">
                    <table class="analysis-table">
                        <thead>
                            <tr>
                                <th style="text-align:left">Recherche</th>
                                <th>Statut</th>
                                <th style="text-align:right">Entreprises</th>
                                <th style="text-align:left">Utilisateur</th>
                                <th style="text-align:left">Worker</th>
                                <th style="text-align:left">Stratégie</th>
                                <th style="text-align:right">Durée</th>
                                <th style="text-align:right">Lancé le</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${systemUsage.map(u => {
                                const dur = u.duration_seconds ? _formatDuration(u.duration_seconds) : '—';
                                return `
                                    <tr class="analysis-table-row" onclick="window.location.hash='#/job/${encodeURIComponent(u.query_id)}'">
                                        <td style="font-weight:600; max-width:180px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap" title="${escapeHtml(u.query_name)}">${escapeHtml(u.query_name)}</td>
                                        <td>${statusBadge(u.status)}</td>
                                        <td style="text-align:right">${u.companies_scraped || 0}/${u.batch_size || 0}</td>
                                        <td style="font-size:var(--font-xs)">${escapeHtml(u.user_name || '—')}</td>
                                        <td style="font-size:var(--font-xs); font-family:var(--font-mono, monospace)">${escapeHtml(u.worker_id || '—')}</td>
                                        <td style="font-size:var(--font-xs)">${u.strategy === 'maps' ? '🗺️ Maps' : '📊 SIRENE'}</td>
                                        <td style="text-align:right; font-size:var(--font-xs)">${dur}</td>
                                        <td style="text-align:right; white-space:nowrap; font-size:var(--font-xs); color:var(--text-muted)">${formatDateTime(u.created_at)}</td>
                                    </tr>
                                `;
                            }).join('')}
                        </tbody>
                    </table>
                </div>
            `}
        </div>
    `;

    // ── Assemble ─────────────────────────────────────────────────
    view.innerHTML = `
        <div class="analysis-grid">
            ${qualityPanel}
            ${enricherPanel}
            ${sectorPanel}
            ${jobsPanel}
            ${timelinePanel}
            ${systemPanel}
        </div>
    `;
}

// ── Analysis helpers ─────────────────────────────────────────────
function _metricBar(label, value, total, pct, tooltip) {
    const infoHtml = tooltip ? ` <span class="info-tooltip" title="${escapeHtml(tooltip)}">ℹ️</span>` : '';
    return `
        <div>
            <div style="display:flex; justify-content:space-between; font-size:var(--font-sm); margin-bottom:3px">
                <span style="color:var(--text-secondary)">${label}${infoHtml}</span>
                <span style="font-weight:600; color:var(--text-primary)">${value.toLocaleString('fr-FR')} / ${total.toLocaleString('fr-FR')} (${pct}%)</span>
            </div>
            <div style="height:8px; background:var(--bg-tertiary); border-radius:4px; overflow:hidden">
                <div style="height:100%; width:${pct}%; background:var(--accent); border-radius:4px; transition:width 0.5s ease"></div>
            </div>
        </div>
    `;
}

function _renderMethodBreakdown(methods, countKey) {
    const entries = Object.entries(methods);
    if (entries.length === 0) return '<p style="color:var(--text-muted);font-size:var(--font-xs)">Pas de données</p>';
    const total = entries.reduce((s, [, v]) => s + (v.count || 0), 0);

    return `
        <div style="font-size:var(--font-xs); color:var(--text-secondary)">
            ${entries.map(([method, stats]) => {
                const pct = total > 0 ? Math.round(100 * (stats.count || 0) / total) : 0;
                const subVal = stats[countKey] || 0;
                return `
                    <div style="display:flex; align-items:center; gap:var(--space-sm); margin-bottom:3px">
                        <span style="min-width:100px; font-family:var(--font-mono, monospace)">${escapeHtml(method)}</span>
                        <div style="flex:1; height:6px; background:var(--bg-input); border-radius:3px; overflow:hidden">
                            <div style="height:100%; width:${pct}%; background:var(--accent); border-radius:3px"></div>
                        </div>
                        <span style="min-width:60px; text-align:right">${stats.count || 0} (${subVal})</span>
                    </div>
                `;
            }).join('')}
        </div>
    `;
}

function _formatDuration(seconds) {
    if (!seconds || seconds < 0) return '—';
    const s = Math.round(seconds);
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    const rem = s % 60;
    if (m < 60) return `${m}m ${rem}s`;
    const h = Math.floor(m / 60);
    const remM = m % 60;
    return `${h}h ${remM}m`;
}
