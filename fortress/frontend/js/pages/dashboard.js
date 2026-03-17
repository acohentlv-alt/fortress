/**
 * Dashboard Page — Dual view: By Location / By Job (grouped)
 *
 * "By Job" groups same-name queries together (case-insensitive),
 * sorted by most recent batch, with timeline of all batches.
 */

import { getDashboardStats, getDepartments, getJobs, getDashboardStatsByJob, getDataBank, getSectorStats, deleteSectorTags, deleteDeptTags, deleteJobGroup, checkHealth, extractApiError, getCachedUser } from '../api.js';
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
            <button class="view-toggle-btn" id="btn-by-job">📋 Par Recherche</button>
            <button class="view-toggle-btn" id="btn-by-sector">🏭 Par Secteur</button>
            ${(getCachedUser()?.role === 'admin') ? '<button class="view-toggle-btn" id="btn-data-bank">🏦 Banque de Données</button>' : ''}
        </div>

        <!-- View Container -->
        <div id="dashboard-view"></div>
    `;

    // Render initial view
    renderByLocation(departments, container);

    // Master Export handler
    document.getElementById('btn-master-export').addEventListener('click', () => {
        window.open(`${API_BASE}/export/master/csv`, '_blank');
    });

    // Toggle handlers
    document.getElementById('btn-by-location').addEventListener('click', () => {
        setActiveToggle('btn-by-location');
        renderByLocation(departments, container);
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

    document.getElementById('btn-by-sector').addEventListener('click', async () => {
        setActiveToggle('btn-by-sector');
        const view = document.getElementById('dashboard-view');
        view.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
        const sectorData = await getSectorStats();
        if (sectorData && Array.isArray(sectorData) && sectorData.length > 0) {
            renderBySectorFromAPI(sectorData, container);
        } else {
            renderBySector(jobs, container);
        }
    });

    // Admin-only data bank tab
    const dataBankBtn = document.getElementById('btn-data-bank');
    if (dataBankBtn) {
        dataBankBtn.addEventListener('click', async () => {
            setActiveToggle('btn-data-bank');
            const view = document.getElementById('dashboard-view');
            view.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
            const data = await getDataBank();
            if (!data || data._ok === false) {
                view.innerHTML = '<div class="empty-state"><div class="empty-state-icon">⚠️</div><div class="empty-state-text">Erreur de chargement</div></div>';
                return;
            }
            renderDataBank(data);
        });
    }
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

// ── Data Bank View (admin only) ──────────────────────────────────
function renderDataBank(data) {
    const view = document.getElementById('dashboard-view');
    const t = data.totals || {};
    const sectors = data.top_sectors || [];
    const depts = data.top_departments || [];
    const workers = data.workers || [];

    // Find max sector company count for bar widths
    const maxSectorCount = sectors.length > 0 ? Math.max(...sectors.map(s => s.companies || 0)) : 1;

    view.innerHTML = `
        <!-- Global totals -->
        <div class="stats-grid" style="margin-bottom:var(--space-xl)">
            <div class="stat-card">
                <span class="stat-card-icon">🏦</span>
                <span class="stat-card-value">${(t.total_enriched || 0).toLocaleString('fr-FR')}</span>
                <span class="stat-card-label">Entreprises enrichies</span>
            </div>
            <div class="stat-card">
                <span class="stat-card-icon">📞</span>
                <span class="stat-card-value">${(t.with_phone || 0).toLocaleString('fr-FR')}</span>
                <span class="stat-card-label">Avec téléphone</span>
            </div>
            <div class="stat-card">
                <span class="stat-card-icon">✉️</span>
                <span class="stat-card-value">${(t.with_email || 0).toLocaleString('fr-FR')}</span>
                <span class="stat-card-label">Avec email</span>
            </div>
            <div class="stat-card">
                <span class="stat-card-icon">👥</span>
                <span class="stat-card-value">${t.total_users || 0}</span>
                <span class="stat-card-label">Utilisateurs</span>
            </div>
        </div>

        <div style="display:grid; grid-template-columns: 1fr 1fr; gap:var(--space-xl)">
            <!-- Top Sectors -->
            <div class="card">
                <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                    🏭 Top secteurs
                </h3>
                ${sectors.length === 0 ? '<p style="color:var(--text-muted)">Aucun secteur</p>' :
                    sectors.map(s => `
                        <div style="margin-bottom:var(--space-md)">
                            <div style="display:flex; justify-content:space-between; font-size:var(--font-sm); margin-bottom:var(--space-xs)">
                                <span style="font-weight:600">${escapeHtml(s.sector || '—')}</span>
                                <span style="color:var(--text-secondary)">${(s.companies || 0).toLocaleString('fr-FR')}</span>
                            </div>
                            <div style="height:6px; background:var(--bg-tertiary); border-radius:3px; overflow:hidden">
                                <div style="height:100%; width:${Math.round(100 * (s.companies || 0) / maxSectorCount)}%; background:var(--accent); border-radius:3px; transition:width 0.3s ease"></div>
                            </div>
                        </div>
                    `).join('')
                }
            </div>

            <!-- Top Departments -->
            <div class="card">
                <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                    📍 Top départements
                </h3>
                ${depts.length === 0 ? '<p style="color:var(--text-muted)">Aucun département</p>' :
                    `<div class="dept-grid">${depts.map(d => `
                        <div class="dept-card" onclick="window.location.hash='#/department/${d.departement}'">
                            <div class="dept-card-header">
                                <span class="dept-card-number">${escapeHtml(d.departement || '—')}</span>
                                <span class="dept-card-count">${(d.companies || 0).toLocaleString('fr-FR')}</span>
                            </div>
                            <div class="dept-card-name">entreprises</div>
                        </div>
                    `).join('')}</div>`
                }
            </div>
        </div>

        <!-- Active Workers -->
        ${workers.length > 0 ? `
            <div class="card" style="margin-top:var(--space-xl)">
                <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                    🖥️ Workers actifs (7 derniers jours)
                </h3>
                <div style="display:grid; grid-template-columns:repeat(auto-fill, minmax(200px, 1fr)); gap:var(--space-md)">
                    ${workers.map(w => `
                        <div style="background:var(--bg-tertiary); border:1px solid var(--border-subtle); border-radius:var(--radius); padding:var(--space-lg)">
                            <div style="font-weight:700; font-size:var(--font-sm); margin-bottom:var(--space-xs)">🖥️ ${escapeHtml(w.worker)}</div>
                            <div style="font-size:var(--font-xs); color:var(--text-secondary)">${w.batches} batch${w.batches > 1 ? 'es' : ''}</div>
                            <div style="font-size:var(--font-xs); color:var(--text-muted)">${formatDateTime(w.last_active)}</div>
                        </div>
                    `).join('')}
                </div>
            </div>
        ` : ''}
    `;
}
