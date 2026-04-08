/**
 * Dashboard Page — Dual view: By Location / By Job (grouped)
 *
 * "By Job" groups same-name queries together (case-insensitive),
 * sorted by most recent batch, with timeline of all batches.
 */

import { getDashboardStats, getDepartments, getJobs, getDashboardStatsByJob, getAnalysis, getBatchAnalysis, getAllData, getClientStats, getMasterExportUrl, bulkExportCSV, deleteSectorTags, deleteDeptTags, deleteJobGroup, checkHealth, extractApiError, getCachedUser, getPendingLinks } from '../api.js';
import { isStale } from '../app.js';
import { showAddEntityModal } from '../components/add-entity-modal.js';
import { renderGauge, statusBadge, formatDateTime, escapeHtml, showToast, showConfirmModal } from '../components.js';
import { GlobalSelection } from '../state.js';
import { t, getLang } from '../i18n.js';

let _dashboardData = null;
let _currentTab = 'stats'; // stats, all, missing_web, missing_phone
let _selected = GlobalSelection;
let _searchTerm = '';
const API_BASE = '/api';

// Module-level cache for getDepartments — avoids duplicate API calls within same session
let _cachedDepartments = null;
async function _getDepartmentsCached() {
    if (_cachedDepartments !== null) return _cachedDepartments;
    _cachedDepartments = await getDepartments();
    return _cachedDepartments;
}

export async function renderDashboard(container, gen) {
    // Show loading state
    container.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

    let stats, departments, jobs, pendingLinksData;

    try {
        [stats, departments, jobs, pendingLinksData] = await Promise.all([
            getDashboardStats(),
            _getDepartmentsCached(),
            getJobs(),
            getPendingLinks(),
        ]);
    } catch {
        stats = null;
        departments = null;
        jobs = null;
        pendingLinksData = null;
    }

    // If the user navigated away while these API calls were in flight, bail out.
    // (gen is undefined for internal retries — those are always valid)
    if (gen !== undefined && isStale(gen)) return;
    // Helper: check if a response is an API error (not a valid data payload)
    const isErr = (r) => !r || (r._ok === false);

    // If the primary stats endpoint fails (e.g. 503 Neon sleeping), fail the whole dashboard
    const dataFailed = isErr(stats);
    if (dataFailed) {
        const errorMsg = extractApiError(stats || departments);
        container.innerHTML = `
            <h1 class="page-title">Dashboard</h1>
            <p class="page-subtitle">${t('dashboard.subtitle')}</p>
            <div class="error-state text-center" style="margin-top:var(--space-2xl); padding:var(--space-2xl)">
                <div style="font-size:3rem; margin-bottom:var(--space-lg)">🔌</div>
                <div style="font-size:var(--font-md); color:var(--text-secondary); margin-bottom:var(--space-lg)">
                    ${escapeHtml(errorMsg)}
                </div>
                <button id="retry-dashboard" class="btn btn-primary" style="margin-top:var(--space-md)">
                    ${t('dashboard.retryBtn')}
                </button>
            </div>
        `;
        document.getElementById('retry-dashboard').addEventListener('click', async (e) => {
            e.target.disabled = true;
            e.target.textContent = t('dashboard.retrying');
            const health = await checkHealth();
            if (health.ok) {
                renderDashboard(container);
            } else {
                showToast(t('dashboard.serverUnavailable'), 'error');
                e.target.disabled = false;
                e.target.textContent = t('dashboard.retryBtn');
            }
        });
        return;
    }

    // Normalize: ensure departments/jobs are arrays (may be error objects if partially failed)
    if (!Array.isArray(departments)) departments = [];
    if (!Array.isArray(jobs)) jobs = [];

    const s = stats || {};
    const user = getCachedUser();
    const pendingCount = (pendingLinksData && pendingLinksData.count) ? pendingLinksData.count : 0;
    const pendingBadge = pendingCount > 0
        ? ` <span style="background:var(--warning); color:#000; border-radius:999px; font-size:var(--font-xs); font-weight:700; padding:1px 6px; margin-left:4px">${pendingCount}</span>`
        : '';

    container.innerHTML = `


        <!-- Welcome Banner -->
        <div class="welcome-banner">
            <div class="welcome-banner-main">
                <div class="welcome-banner-text">
                    <h1 class="welcome-banner-title">${t('welcomeBanner.title')}</h1>
                    <p class="welcome-banner-subtitle">${t('welcomeBanner.subtitle', { date: (() => { const lang = getLang(); return new Date().toLocaleDateString(lang === 'fr' ? 'fr-FR' : 'en-US', { day: 'numeric', month: 'long', year: 'numeric' }); })() })}</p>
                </div>
                ${s.total_companies === 0 ? `
                    <div class="welcome-banner-empty">${t('welcomeBanner.emptyState')}</div>
                ` : `
                    <div class="welcome-banner-pills">
                        <div class="stat-pill">
                            <span class="stat-pill-value">${(s.total_companies || 0).toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')}</span>
                            <span class="stat-pill-label">${t('welcomeBanner.totalCompanies')}</span>
                        </div>
                        <div class="stat-pill">
                            <span class="stat-pill-value">${(s.enriched_this_week || 0).toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')}</span>
                            <span class="stat-pill-label">${t('welcomeBanner.enrichedWeek')}</span>
                        </div>
                        <div class="stat-pill">
                            <span class="stat-pill-value">${s.running_jobs || 0}</span>
                            <span class="stat-pill-label">${t('welcomeBanner.activeBatches')}</span>
                        </div>
                    </div>
                `}
                <a href="#/new-batch" class="btn-hero">
                    ${t('dashboard.newSearch')} <span class="btn-hero-arrow">→</span>
                </a>
            </div>
        </div>

        <!-- Secondary actions row -->
        <div class="secondary-actions">
            <a href="#/monitor" class="btn btn-secondary btn-sm">${t('dashboard.pipelineLive')}</a>
            <button class="btn btn-secondary btn-sm" id="btn-master-export">${t('dashboard.export')}</button>
            <button class="btn btn-secondary btn-sm" id="btn-add-entity">${t('dashboard.addEntity')}</button>
        </div>



        <!-- View Toggle -->
        <div class="view-toggle">
            <button class="view-toggle-btn active" id="btn-analysis">${t('dashboard.tabAnalysis')}</button>
            <button class="view-toggle-btn" id="btn-by-job">${t('dashboard.tabByJob')}</button>
            <button class="view-toggle-btn" id="btn-by-dept">${t('dashboard.tabByDept')}</button>
            <button class="view-toggle-btn" id="btn-by-upload">${t('dashboard.tabByUpload')}</button>
            <button class="view-toggle-btn" id="btn-pending-links">${t('dashboard.tabPendingLinks')}${pendingBadge}</button>
        </div>

        <!-- View Container -->
        <div id="dashboard-view"><div class="loading"><div class="spinner"></div></div></div>
    `;

    // Render initial view — only on first visit (before any tab has been chosen)
    // On subsequent visits, _currentTab will have been set and the restore block below handles it.
    if (_currentTab === 'stats') {
        _loadAnalysisView(container);
    }

    // Master Export handler — dropdown with CSV + XLSX
    const exportBtn = document.getElementById('btn-master-export');
    exportBtn.addEventListener('click', () => {
        // Toggle dropdown
        let dd = document.getElementById('export-dropdown');
        if (dd) { dd.remove(); return; }
        dd = document.createElement('div');
        dd.id = 'export-dropdown';
        dd.style.cssText = 'position:absolute; top:100%; right:0; margin-top:var(--space-xs); background:var(--bg-elevated); border:1px solid var(--border-default); border-radius:var(--radius); box-shadow:0 8px 24px rgba(0,0,0,0.3); z-index:50; min-width:160px; overflow:hidden;';
        dd.innerHTML = `
            <a href="${getMasterExportUrl('csv')}" style="display:block; padding:var(--space-sm) var(--space-lg); color:var(--text-primary); text-decoration:none; transition:background 0.15s" onmouseover="this.style.background='var(--bg-hover)'" onmouseout="this.style.background=''">📥 CSV</a>
            <a href="${getMasterExportUrl('xlsx')}" style="display:block; padding:var(--space-sm) var(--space-lg); color:var(--text-primary); text-decoration:none; transition:background 0.15s" onmouseover="this.style.background='var(--bg-hover)'" onmouseout="this.style.background=''">📗 XLSX (Excel)</a>
        `;
        exportBtn.parentElement.style.position = 'relative';
        exportBtn.parentElement.appendChild(dd);
        // Close on outside click
        const close = (e) => { if (!dd.contains(e.target) && e.target !== exportBtn) { dd.remove(); document.removeEventListener('click', close); } };
        setTimeout(() => document.addEventListener('click', close), 0);
    });

    // Toggle handlers
    const analysisBtn = document.getElementById('btn-analysis');
    if (analysisBtn) {
        analysisBtn.addEventListener('click', () => {
            _currentTab = 'analyse';
            setActiveToggle('btn-analysis');
            _loadAnalysisView(container);
        });
    }

    document.getElementById('btn-by-job').addEventListener('click', async () => {
        _currentTab = 'by-job';
        setActiveToggle('btn-by-job');
        const byJobData = await getDashboardStatsByJob();
        if (byJobData && Array.isArray(byJobData) && byJobData.length > 0) {
            renderByJobFromAPI(byJobData, container);
        } else {
            renderByJob(jobs, container);
        }
    });

    document.getElementById('btn-by-dept').addEventListener('click', async () => {
        _currentTab = 'by-dept';
        setActiveToggle('btn-by-dept');
        const view = document.getElementById('dashboard-view');
        view.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
        const depts = await _getDepartmentsCached();
        renderByLocation(depts, container);
    });

    document.getElementById('btn-by-upload').addEventListener('click', async () => {
        _currentTab = 'upload';
        setActiveToggle('btn-by-upload');
        const view = document.getElementById('dashboard-view');
        view.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
        const data = await getClientStats();
        _renderByUpload(data, container);
    });

    document.getElementById('btn-pending-links').addEventListener('click', async () => {
        _currentTab = 'pending-links';
        setActiveToggle('btn-pending-links');
        const view = document.getElementById('dashboard-view');
        view.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
        const data = await getPendingLinks();
        _renderPendingLinks(data, container);
    });

    document.getElementById('btn-add-entity').addEventListener('click', () => {
        showAddEntityModal({ onSuccess: () => renderDashboard(container) });
    });

    // Restore previously active tab
    if (_currentTab === 'by-job') {
        document.getElementById('btn-by-job')?.click();
    } else if (_currentTab === 'by-dept') {
        document.getElementById('btn-by-dept')?.click();
    } else if (_currentTab === 'upload') {
        document.getElementById('btn-by-upload')?.click();
    } else if (_currentTab === 'pending-links') {
        document.getElementById('btn-pending-links')?.click();
    } else if (_currentTab === 'analyse') {
        document.getElementById('btn-analysis')?.click();
    }
    // Note: the initial render above (lines 119-128) already loaded the default view on first visit.
    // Tab restore only fires on subsequent visits when _currentTab is already set.

}

async function _loadAnalysisView(rootContainer) {
    const view = document.getElementById('dashboard-view');
    if (!view) return;
    view.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
    const [data, batchData, depts] = await Promise.all([
        getAnalysis(),
        getBatchAnalysis(),
        _getDepartmentsCached(),
    ]);
    if (!data || data._ok === false) {
        view.innerHTML = `<div class="empty-state"><div class="empty-state-icon">⚠️</div><div class="empty-state-text">${t('dashboard.loadError')}</div></div>`;
        return;
    }
    const isAdmin = getCachedUser()?.role === 'admin';
    renderAnalysis(data, isAdmin, rootContainer, batchData, Array.isArray(depts) ? depts : []);
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
                <div class="empty-state-text">${t('dashboard.noDept')}</div>
                <p style="color: var(--text-muted)">${t('dashboard.noDeptHint')}</p>
            </div>
        `;
        return;
    }

    view.innerHTML = `
        <div class="dept-grid">
            ${departments.map(d => `
                <div class="dept-card" style="position:relative" data-dept="${escapeHtml(d.departement)}">
                    <button class="card-delete-btn" data-delete-type="dept" data-delete-id="${escapeHtml(d.departement)}" data-delete-label="${escapeHtml(d.department_name)} (${d.company_count} entreprises)"
                        onclick="event.stopPropagation()" title="${t('dashboard.deleteDept')}">✕</button>
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
                title: t('dashboard.deleteDept'),
                body: t('dashboard.deleteDeptBody', { label }),
                confirmLabel: t('dashboard.deleteConfirm'),
                danger: true,
                onConfirm: async () => {
                    const result = await deleteDeptTags(dept);
                    if (result._ok) {
                        showToast(t('dashboard.deleteDeptSuccess', { dept, count: result.tags_removed }), 'success');
                        renderDashboard(rootContainer);
                    } else {
                        showToast(extractApiError(result), 'error');
                    }
                },
            });
        });
    });
}

// ── By Upload View — shows uploaded CRM files + stats ────────────
function _renderByUpload(data, rootContainer) {
    const view = document.getElementById('dashboard-view');

    if (!data || data._ok === false) {
        view.innerHTML = `<div class="empty-state"><div class="empty-state-icon">⚠️</div><div class="empty-state-text">${t('dashboard.loadError')}</div></div>`;
        return;
    }

    const uploads = data.uploads || [];
    const totalSirens = data.total_sirens || 0;

    if (uploads.length === 0) {
        view.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">📤</div>
                <div class="empty-state-text">${t('dashboard.noUpload')}</div>
                <p style="color: var(--text-muted)">${t('dashboard.noUploadHint')} <a href="#/upload" style="color:var(--accent)">Import/Export</a></p>
            </div>
        `;
        return;
    }

    view.innerHTML = `
        <div style="margin-bottom:var(--space-xl)">
            <div class="stats-grid" style="grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); margin-bottom:var(--space-2xl)">
                <div class="stat-card">
                    <div class="stat-card-icon">📤</div>
                    <div class="stat-card-value">${totalSirens.toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')}</div>
                    <div class="stat-card-label">${t('dashboard.sirensImported')}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-card-icon">📁</div>
                    <div class="stat-card-value">${uploads.length}</div>
                    <div class="stat-card-label">${t('dashboard.filesImported', { plural: uploads.length > 1 ? 's' : '' })}</div>
                </div>
            </div>

            <div style="display:flex; flex-direction:column; gap:var(--space-md)">
                ${uploads.map(u => {
                    const rawName = u.source_file || '';
                    const displayName = rawName.replace(/\.[^/.]+$/, '').replace(/[_-]/g, ' ').trim() || '—';
                    const sirenCount = (u.siren_count || 0).toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US');
                    const bid = escapeHtml(u.batch_id || '');
                    return `
                    <div class="card card-clickable upload-row" data-batch-id="${bid}"
                         style="padding:var(--space-lg) var(--space-xl); display:flex; align-items:center; justify-content:space-between; gap:var(--space-lg)">
                        <div style="display:flex; align-items:center; gap:var(--space-lg); min-width:0; flex:1">
                            <div style="font-size:2rem; flex-shrink:0">📄</div>
                            <div style="min-width:0">
                                <div style="font-weight:700; color:var(--text-primary); font-size:var(--font-md); white-space:nowrap; overflow:hidden; text-overflow:ellipsis">${escapeHtml(displayName)}</div>
                                <div style="font-size:var(--font-xs); color:var(--text-muted); margin-top:2px">${formatDateTime(u.uploaded_at)}</div>
                            </div>
                        </div>
                        <div style="display:flex; align-items:center; gap:var(--space-xl); flex-shrink:0">
                            <div style="text-align:right">
                                <div style="font-size:var(--font-lg); font-weight:700; color:var(--accent)">${sirenCount}</div>
                                <div style="font-size:var(--font-xs); color:var(--text-muted)">SIRENs</div>
                            </div>
                            <div style="color:var(--text-muted); font-size:var(--font-sm)">→</div>
                        </div>
                    </div>
                    `;
                }).join('')}
            </div>
        </div>
    `;

    // Wire clicks
    view.querySelectorAll('.upload-row').forEach(row => {
        row.addEventListener('click', () => {
            const bid = row.dataset.batchId;
            if (bid) {
                window.location.hash = `#/job/${encodeURIComponent(bid)}`;
            }
        });
    });
}




// ── Pending Links View — companies waiting for SIRENE link confirmation ──
function _renderPendingLinks(data, rootContainer) {
    const view = document.getElementById('dashboard-view');

    if (!data || data._ok === false) {
        view.innerHTML = `<div class="empty-state"><div class="empty-state-icon">⚠️</div><div class="empty-state-text">${t('dashboard.loadError')}</div></div>`;
        return;
    }

    const results = data.results || [];

    if (results.length === 0) {
        view.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">✅</div>
                <div class="empty-state-text">${t('dashboard.noPendingLinksMsg')}</div>
                <p style="color:var(--text-muted)">${t('dashboard.allMatchesDone')}</p>
            </div>
        `;
        return;
    }

    view.innerHTML = `
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:var(--space-md)">
            <span style="font-size:var(--font-sm); color:var(--text-secondary)">
                ${t('dashboard.pendingCount', { count: results.length, plural: results.length > 1 ? 's' : '' })}
            </span>
        </div>
        <div class="card" style="overflow-x:auto">
            <table style="width:100%; border-collapse:collapse; font-size:var(--font-sm)">
                <thead>
                    <tr>
                        <th class="contacts-th">${t('dashboard.colCompany')}</th>
                        <th class="contacts-th" style="white-space:nowrap">${t('dashboard.colMapsId')}</th>
                        <th class="contacts-th">${t('dashboard.colSuggested')}</th>
                        <th class="contacts-th">${t('dashboard.colReason')}</th>
                        <th class="contacts-th">${t('dashboard.colPhone')}</th>
                        <th class="contacts-th">${t('dashboard.colCity')}</th>
                        <th class="contacts-th" style="white-space:nowrap">${t('dashboard.colDept')}</th>
                        <th class="contacts-th">${t('dashboard.colSearch')}</th>
                        <th class="contacts-th">${t('dashboard.colActions')}</th>
                    </tr>
                </thead>
                <tbody>
                    ${results.map((r, idx) => {
                        const reason = r.link_method === 'fuzzy_name' ? t('dashboard.reasonFuzzy')
                            : r.link_method === 'address' ? t('dashboard.reasonAddress')
                            : r.link_method === 'phone' ? t('dashboard.reasonPhone')
                            : r.link_method === 'phone_weak' ? t('dashboard.reasonPhoneWeak')
                            : r.link_method === 'enseigne' ? t('dashboard.reasonEnseigne')
                            : r.link_method === 'enseigne_weak' ? t('dashboard.reasonEnseigneWeak')
                            : r.link_method || '—';
                        const hints = [];
                        if (r.suggested_ville && r.maps_address &&
                            r.maps_address.toLowerCase().includes(r.suggested_ville.toLowerCase())) hints.push(t('dashboard.reasonAddress').toLowerCase());
                        const reasonDisplay = hints.length ? `${reason} · ${hints.join(' · ')}` : reason;
                        return `
                        <tr class="contacts-row" onclick="window.location.hash='#/company/${escapeHtml(r.siren)}'">
                            <td class="contacts-td" style="font-weight:500; max-width:220px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap">
                                ${escapeHtml(r.denomination || '—')}
                            </td>
                            <td class="contacts-td" style="font-family:var(--font-mono); color:var(--accent); font-weight:600; white-space:nowrap">
                                ${escapeHtml(r.siren)}
                            </td>
                            <td class="contacts-td" style="max-width:200px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap">
                                ${r.suggested_name
                                    ? `<span style="color:var(--warning); font-weight:500">${escapeHtml(r.suggested_name)}</span><br><span style="color:var(--text-muted); font-size:var(--font-xs)">${escapeHtml(r.suggested_siren || '')}</span>`
                                    : `<span style="color:var(--text-muted); font-size:var(--font-xs)">${escapeHtml(r.suggested_siren || '—')}</span>`}
                            </td>
                            <td class="contacts-td reason-toggle" data-idx="${idx}" onclick="event.stopPropagation()">
                                <span class="reason-chevron">▸</span>
                                <span style="color:var(--text-secondary); font-size:var(--font-xs)">${escapeHtml(reasonDisplay)}</span>
                            </td>
                            <td class="contacts-td">
                                ${r.phone
                                    ? `<span style="color:var(--success); font-weight:600; white-space:nowrap">${escapeHtml(r.phone)}</span>`
                                    : '<span style="color:var(--text-disabled)">—</span>'}
                            </td>
                            <td class="contacts-td" style="max-width:140px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap">
                                ${escapeHtml(r.ville || '—')}
                            </td>
                            <td class="contacts-td" style="text-align:center; white-space:nowrap">
                                ${escapeHtml(r.departement || '—')}
                            </td>
                            <td class="contacts-td" style="max-width:140px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:var(--text-muted)">
                                ${escapeHtml(r.batch_name || '—')}
                            </td>
                            <td style="white-space:nowrap; text-align:right" onclick="event.stopPropagation()">
                                <button class="action-btn action-btn-confirm pending-confirm" data-siren="${escapeHtml(r.siren)}" data-target="${escapeHtml(r.suggested_siren || '')}" title="Confirmer le lien">✓</button>
                                <button class="action-btn action-btn-reject pending-reject" data-siren="${escapeHtml(r.siren)}" title="Rejeter le lien">✕</button>
                            </td>
                        </tr>
                        <tr class="evidence-row" id="evidence-${idx}" style="display:none">
                            <td colspan="9" style="padding:0">
                                <div class="evidence-content">
                                    <div class="evidence-grid">
                                        <div class="evidence-side">
                                            <div class="evidence-side-title">${t('dashboard.mapsData')}</div>
                                            <div class="evidence-field"><span class="evidence-label">Nom</span> ${escapeHtml(r.denomination || '—')}</div>
                                            <div class="evidence-field"><span class="evidence-label">Adresse</span> ${escapeHtml(r.maps_address || r.ville || '—')}</div>
                                            <div class="evidence-field"><span class="evidence-label">Tél</span> ${escapeHtml(r.phone || '—')}</div>
                                        </div>
                                        <div class="evidence-side">
                                            <div class="evidence-side-title">${t('dashboard.sireneCandidate')}</div>
                                            <div class="evidence-field"><span class="evidence-label">Nom</span> ${escapeHtml(r.suggested_name || '—')}</div>
                                            <div class="evidence-field"><span class="evidence-label">Adresse</span> ${escapeHtml(r.suggested_address || r.suggested_ville || '—')}</div>
                                            <div class="evidence-field"><span class="evidence-label">NAF</span> ${escapeHtml(r.suggested_naf || '—')} ${escapeHtml(r.suggested_naf_libelle || '')}</div>
                                        </div>
                                    </div>
                                </div>
                            </td>
                        </tr>
                    `}).join('')}
                </tbody>
            </table>
        </div>
    `;

    // Evidence row toggle handler
    view.addEventListener('click', (e) => {
        const toggle = e.target.closest('.reason-toggle');
        if (!toggle) return;
        e.stopPropagation();
        const idx = toggle.dataset.idx;
        const evidenceRow = document.getElementById(`evidence-${idx}`);
        if (!evidenceRow) return;
        const isOpen = evidenceRow.style.display !== 'none';
        // Close all other open evidence rows
        view.querySelectorAll('.evidence-row').forEach(row => {
            row.style.display = 'none';
        });
        view.querySelectorAll('.reason-chevron').forEach(ch => {
            ch.style.transform = '';
        });
        if (!isOpen) {
            evidenceRow.style.display = '';
            const chevron = toggle.querySelector('.reason-chevron');
            if (chevron) chevron.style.transform = 'rotate(90deg)';
        }
    });

    // Confirm handler
    view.querySelectorAll('.pending-confirm').forEach(btn => {
        btn.addEventListener('click', async (e) => {
            e.stopPropagation();
            const origText = btn.textContent;
            btn.disabled = true;
            btn.textContent = '⏳...';
            const siren = btn.dataset.siren;
            const target = btn.dataset.target;
            try {
                const resp = await fetch(`/api/companies/${encodeURIComponent(siren)}/link`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({target_siren: target}),
                    credentials: 'same-origin',
                });
                if (resp.ok) {
                    showToast(t('dashboard.linkConfirmed'), 'success');
                    const fresh = await getPendingLinks();
                    _renderPendingLinks(fresh, rootContainer);
                    const badge = document.querySelector('#btn-pending-links .badge');
                    if (badge) badge.textContent = fresh.count > 0 ? fresh.count : '';
                } else {
                    showToast(t('dashboard.linkConfirmError'), 'error');
                    btn.disabled = false;
                    btn.textContent = origText;
                }
            } catch {
                showToast(t('dashboard.networkError'), 'error');
                btn.disabled = false;
                btn.textContent = origText;
            }
        });
    });

    // Reject handler
    view.querySelectorAll('.pending-reject').forEach(btn => {
        btn.addEventListener('click', async (e) => {
            e.stopPropagation();
            const origText = btn.textContent;
            btn.disabled = true;
            btn.textContent = '⏳...';
            const siren = btn.dataset.siren;
            try {
                const resp = await fetch(`/api/companies/${encodeURIComponent(siren)}/reject-link`, {
                    method: 'POST',
                    credentials: 'same-origin',
                });
                if (resp.ok) {
                    showToast(t('dashboard.linkRejected'), 'success');
                    const fresh = await getPendingLinks();
                    _renderPendingLinks(fresh, rootContainer);
                    const badge = document.querySelector('#btn-pending-links .badge');
                    if (badge) badge.textContent = fresh.count > 0 ? fresh.count : '';
                } else {
                    showToast(t('dashboard.linkRejectError'), 'error');
                    btn.disabled = false;
                    btn.textContent = origText;
                }
            } catch {
                showToast(t('dashboard.networkError'), 'error');
                btn.disabled = false;
                btn.textContent = origText;
            }
        });
    });
}


// ── All Data View — searchable + paginated table of all enriched entities ──
function _renderAllData(rootContainer, q = '', department = '', naf_code = '', offset = 0) {
    const view = document.getElementById('dashboard-view');
    const PAGE_SIZE = 50;
    const _selected = new Set();
    const isAdminUser = getCachedUser()?.role === 'admin';

    view.innerHTML = `
        <div class="search-filters-row" style="align-items:center">
            <div style="position:relative; flex:1; min-width:200px">
                <span style="position:absolute; left:12px; top:50%; transform:translateY(-50%); color:var(--text-muted)">🔍</span>
                <input type="text" id="alldata-search" placeholder="${t('dashboard.alldataSearch')}"
                    value="${escapeHtml(q)}"
                    style="width:100%; padding:var(--space-sm) var(--space-md) var(--space-sm) 36px;
                           background:var(--bg-input); border:1px solid var(--border-default);
                           border-radius:var(--radius); color:var(--text-primary);
                           font-family:var(--font-family); font-size:var(--font-sm); outline:none;">
            </div>
            <input type="text" id="alldata-dept" placeholder="${t('dashboard.alldataDept')}"
                value="${escapeHtml(department)}"
                style="width:80px; padding:var(--space-sm) var(--space-md);
                       background:var(--bg-input); border:1px solid var(--border-default);
                       border-radius:var(--radius); color:var(--text-primary);
                       font-family:var(--font-family); font-size:var(--font-sm); outline:none;">
            <input type="text" id="alldata-naf" placeholder="${t('dashboard.alldataNaf')}"
                value="${escapeHtml(naf_code)}"
                style="width:120px; padding:var(--space-sm) var(--space-md);
                       background:var(--bg-input); border:1px solid var(--border-default);
                       border-radius:var(--radius); color:var(--text-primary);
                       font-family:var(--font-family); font-size:var(--font-sm); outline:none;">
        </div>
        <div id="alldata-results"><div class="loading"><div class="spinner"></div></div></div>
    `;

    const searchInput = document.getElementById('alldata-search');
    const deptInput = document.getElementById('alldata-dept');
    const nafInput = document.getElementById('alldata-naf');
    const resultsEl = document.getElementById('alldata-results');
    let timer;

    async function loadData() {
        const curQ = searchInput.value.trim();
        const curDept = deptInput.value.trim();
        const curNaf = nafInput.value.trim();
        resultsEl.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

        const data = await getAllData({ q: curQ, department: curDept, naf_code: curNaf, limit: PAGE_SIZE, offset });
        if (!data || data._ok === false) {
            resultsEl.innerHTML = `<div class="empty-state"><div class="empty-state-icon">⚠️</div><div class="empty-state-text">${t('dashboard.loadError')}</div></div>`;
            return;
        }

        const rows = data.results || [];
        const total = data.total || 0;
        const currentPage = Math.floor(offset / PAGE_SIZE) + 1;
        const totalPages = Math.ceil(total / PAGE_SIZE);

        if (rows.length === 0) {
            resultsEl.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">🗃️</div>
                    <div class="empty-state-text">${curQ ? t('dashboard.alldataEmpty') : t('dashboard.alldataNoData')}</div>
                    <p style="color:var(--text-muted)">${curQ ? t('dashboard.alldataTryOther') : t('dashboard.alldataStartBatch')}</p>
                </div>
            `;
            return;
        }

        resultsEl.innerHTML = `
            <p style="font-size:var(--font-sm); color:var(--text-secondary); margin-bottom:var(--space-md)">
                ${t('dashboard.alldataCount', { total: total.toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US'), plural: total > 1 ? 's' : '' })}
                ${totalPages > 1 ? t('dashboard.alldataPage', { current: currentPage, total: totalPages }) : ''}
            </p>
            <div class="card" style="overflow-x:auto">
                <table style="width:100%; border-collapse:collapse; font-size:var(--font-sm)">
                    <thead>
                        <tr>
                            <th style="text-align:center; padding:var(--space-sm); border-bottom:2px solid var(--border-default); width:36px">
                                <input type="checkbox" id="alldata-selectall" title="${t('dashboard.alldataSelectAll')}" style="cursor:pointer">
                            </th>
                            <th style="text-align:left; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase; white-space:nowrap">SIREN</th>
                            <th style="text-align:left; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase">Dénomination</th>
                            <th style="text-align:left; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase; white-space:nowrap">📞 Tél</th>
                            <th style="text-align:left; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase; white-space:nowrap">✉️ Email</th>
                            <th style="text-align:left; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase">🌐 Site</th>
                            <th style="text-align:left; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase; white-space:nowrap">${t('dashboard.colDept')}</th>
                            <th style="text-align:left; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase; white-space:nowrap">${t('dashboard.colSearch')}</th>
                            ${isAdminUser ? `<th style="text-align:left; padding:var(--space-sm) var(--space-md); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase; white-space:nowrap">Espaces</th>` : ''}
                        </tr>
                    </thead>
                    <tbody>
                        ${rows.map(c => {
                            let hostname = '';
                            try { hostname = c.website ? new URL(c.website).hostname : ''; } catch { hostname = c.website || ''; }
                            const navSiren = c.representative_siren || c.siren;
                            return `
                            <tr style="cursor:pointer; transition:background 0.15s${_selected.has(navSiren) ? '; background:var(--bg-hover)' : ''}"
                                onmouseover="this.style.background='var(--bg-hover)'"
                                onmouseout="this.style.background='${_selected.has(navSiren) ? 'var(--bg-hover)' : ''}'"
                                data-siren="${navSiren}">
                                <td style="text-align:center; padding:var(--space-sm); border-bottom:1px solid var(--border-subtle)" onclick="event.stopPropagation()">
                                    <input type="checkbox" class="alldata-cb" data-siren="${navSiren}" ${_selected.has(navSiren) ? 'checked' : ''} style="cursor:pointer">
                                </td>
                                <td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); font-family:var(--font-mono); color:var(--accent); font-weight:600; white-space:nowrap" onclick="window.location.hash='#/company/${navSiren}'">${escapeHtml(c.siren)}</td>
                                <td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); color:var(--text-primary); font-weight:500; max-width:220px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap" onclick="window.location.hash='#/company/${navSiren}'">${escapeHtml(c.denomination || '—')}</td>
                                <td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); color:${c.phone ? 'var(--success)' : 'var(--text-muted)'}; white-space:nowrap">${c.phone ? escapeHtml(c.phone) : '—'}</td>
                                <td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); color:${c.email ? 'var(--success)' : 'var(--text-muted)'}; max-width:200px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap">${c.email ? escapeHtml(c.email) : '—'}</td>
                                <td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); max-width:180px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap">${c.website ? `<a href="${escapeHtml(c.website)}" target="_blank" rel="noopener" style="color:var(--accent)" onclick="event.stopPropagation()">${escapeHtml(hostname)}</a>` : '<span style="color:var(--text-muted)">—</span>'}</td>
                                <td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); color:var(--text-secondary); text-align:center">${escapeHtml(c.departement || '—')}</td>
                                <td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); font-size:var(--font-xs); color:var(--text-muted); max-width:120px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap">${escapeHtml(c.batch_name || '—')}</td>
                                ${isAdminUser ? `<td style="padding:var(--space-sm) var(--space-md); border-bottom:1px solid var(--border-subtle); font-size:var(--font-xs); color:var(--text-secondary); white-space:nowrap">${escapeHtml((c.workspaces || []).join(', ') || '—')}</td>` : ''}
                            </tr>
                        `}).join('')}
                    </tbody>
                </table>
            </div>
            ${totalPages > 1 ? `
                <div style="display:flex; justify-content:center; align-items:center; gap:var(--space-lg); margin-top:var(--space-xl)">
                    <button class="btn btn-secondary btn-sm" id="alldata-prev" ${offset > 0 ? '' : 'disabled'} style="${offset > 0 ? '' : 'opacity:0.4; cursor:not-allowed'}">${t('dashboard.alldataPrev')}</button>
                    <span style="font-size:var(--font-sm); color:var(--text-secondary); font-weight:600">${currentPage} / ${totalPages}</span>
                    <button class="btn btn-secondary btn-sm" id="alldata-next" ${offset + PAGE_SIZE < total ? '' : 'disabled'} style="${offset + PAGE_SIZE < total ? '' : 'opacity:0.4; cursor:not-allowed'}">${t('dashboard.alldataNext')}</button>
                </div>
            ` : ''}
        `;

        // Wire pagination
        const prevBtn = document.getElementById('alldata-prev');
        const nextBtn = document.getElementById('alldata-next');
        if (prevBtn && offset > 0) {
            prevBtn.addEventListener('click', () => {
                offset = Math.max(0, offset - PAGE_SIZE);
                loadData();
            });
        }
        if (nextBtn && offset + PAGE_SIZE < total) {
            nextBtn.addEventListener('click', () => {
                offset += PAGE_SIZE;
                loadData();
            });
        }

        // Wire checkboxes
        resultsEl.querySelectorAll('.alldata-cb').forEach(cb => {
            cb.addEventListener('change', () => {
                if (cb.checked) GlobalSelection.add(cb.dataset.siren);
                else GlobalSelection.delete(cb.dataset.siren);
                _updateBulkExportBar(GlobalSelection);
            });
        });

        // Select-all toggle
        const selectAll = document.getElementById('alldata-selectall');
        if (selectAll) {
            selectAll.addEventListener('change', () => {
                resultsEl.querySelectorAll('.alldata-cb').forEach(cb => {
                    cb.checked = selectAll.checked;
                    if (selectAll.checked) GlobalSelection.add(cb.dataset.siren);
                    else GlobalSelection.delete(cb.dataset.siren);
                });
                _updateBulkExportBar(_selected);
            });
        }

        _updateBulkExportBar(_selected);
    }

    // Wire search inputs — live results as user types
    for (const el of [searchInput, deptInput, nafInput]) {
        el.addEventListener('input', () => {
            clearTimeout(timer);
            timer = setTimeout(() => { offset = 0; loadData(); }, 400);
        });
    }

    // Initial load
    loadData();
}

// ── Floating bulk export bar for All Data selection ─────────────
function _updateBulkExportBar(selected) {
    let bar = document.getElementById('alldata-bulk-bar');
    if (selected.size === 0) {
        if (bar) bar.remove();
        return;
    }
    if (!bar) {
        bar = document.createElement('div');
        bar.id = 'alldata-bulk-bar';
        bar.className = 'bulk-action-bar';
        document.body.appendChild(bar);
    }
    const n = selected.size;
    bar.innerHTML = `
        <span style="font-weight:600; color:var(--text-primary)">${t('dashboard.alldataBulkSelected', { count: n, plural: n > 1 ? 's' : '' })}</span>
        <div style="display:flex; gap:10px">
            <button class="btn btn-primary" id="alldata-bulk-export">${t('dashboard.alldataBulkExport')}</button>
            <button class="btn" style="background:var(--accent); color:white; border:none" id="alldata-bulk-enrich">${t('dashboard.alldataBulkEnrich')}</button>
        </div>
    `;
    
    document.getElementById('alldata-bulk-enrich').addEventListener('click', () => {
        const sirens = [...selected];
        import('../live-enrich.js').then(m => m.openLiveEnrichModal(sirens));
    });
    
    document.getElementById('alldata-bulk-export').addEventListener('click', async () => {
        const sirens = [...selected];
        showToast(t('dashboard.alldataExporting', { count: sirens.length }), 'info');
        try {
            const resp = await bulkExportCSV(sirens);
            if (resp.ok) {
                const blob = await resp.blob();
                const url = URL.createObjectURL(blob);
                const a = document.createElement('a');
                a.href = url;
                a.download = `fortress_selection_${sirens.length}.csv`;
                document.body.appendChild(a);
                a.click();
                a.remove();
                URL.revokeObjectURL(url);
                showToast(t('dashboard.alldataExported', { count: sirens.length }), 'success');
            } else {
                showToast(t('dashboard.alldataExportError'), 'error');
            }
        } catch {
            showToast(t('dashboard.alldataNetworkError'), 'error');
        }
    });
}

// ── By Job View — From normalized API (no client-side grouping needed) ────
function renderByJobFromAPI(groups, rootContainer) {
    const view = document.getElementById('dashboard-view');
    if (!groups || groups.length === 0) {
        _renderJobEmptyState(view);
        return;
    }

    // Filter out manual addition groups from "Par Recherche" tab
    const filteredGroups = groups.filter(g => {
        const name = (g.display_name || g.batch_name || '').toLowerCase();
        return name !== 'ajout manuel';
    });

    if (!filteredGroups || filteredGroups.length === 0) {
        _renderJobEmptyState(view);
        return;
    }

    // The API returns pre-grouped, uppercase-normalized data
    const groupCards = filteredGroups.map((g, idx) => _renderGroupCard(g, idx));

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

    // Filter out manual addition jobs before grouping
    const filteredJobs = jobs.filter(j => {
        const name = (j.batch_name || '').toLowerCase().trim();
        return name !== 'ajout manuel';
    });

    // Group by batch_name (case-insensitive)
    const groups = {};
    for (const j of filteredJobs) {
        const key = (j.batch_name || '').toUpperCase().trim();
        if (!groups[key]) groups[key] = { display_name: j.batch_name, batches: [] };
        groups[key].batches.push(j);
    }

    // Sort each group's batches by date (newest first)
    for (const g of Object.values(groups)) {
        g.batches.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
        g.display_name = g.batches[0].batch_name;
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
            <div class="empty-state-text">${t('dashboard.noJobFound')}</div>
            <p style="color: var(--text-muted)">${t('dashboard.noJobHint')}</p>
        </div>
    `;
}

function _wireJobGroupDeleteButtons(view, rootContainer) {
    view.querySelectorAll('.card-delete-btn[data-delete-type="job-group"]').forEach(btn => {
        btn.addEventListener('click', () => {
            const batchName = btn.dataset.deleteId;
            const label = btn.dataset.deleteLabel;
            showConfirmModal({
                title: t('dashboard.deleteJobGroup'),
                body: t('dashboard.deleteJobGroupBody', { label }),
                confirmLabel: t('dashboard.deleteConfirm'),
                danger: true,
                onConfirm: async () => {
                    const result = await deleteJobGroup(batchName);
                    if (result._ok) {
                        showToast(t('dashboard.deleteJobGroupSuccess', { name: batchName, jobs: result.jobs_deleted, tags: result.tags_removed }), 'success');
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
    const uniqueCompanies = g.unique_companies || 0;
    const totalScraped = batches.reduce((s, b) => s + (b.companies_scraped || 0), 0);
    const totalTarget = (batches[0] || {}).total_companies || 0;
    const overallPct = Math.min(100, Math.round((totalScraped / Math.max(totalTarget, 1)) * 100));
    const latest = batches[0] || {};
    const hasRunning = batches.some(b => b.status === 'in_progress');
    const batchCount = batches.length;
    const groupId = `jobgroup-${idx}`;

    // Use display_name if available (preserves original casing), fall back to batch_name
    const displayName = escapeHtml(g.display_name || g.batch_name || '');
    const batchName = g.display_name || g.batch_name || '';
    const latestBatchId = latest.batch_id || '';

    return `
        <div class="job-group-card" style="position:relative" data-group-name="${escapeHtml(batchName)}">
            <button class="card-delete-btn" data-delete-type="job-group" data-delete-id="${escapeHtml(batchName)}" data-delete-label="${displayName} (${batchCount} batch${batchCount > 1 ? 'es' : ''})"
                onclick="event.stopPropagation()" title="${t('dashboard.deleteJobGroup')}">✕</button>
            <div class="job-group-header" onclick="window.location.hash='#/job/${encodeURIComponent(latestBatchId)}'">
                <div class="job-group-info">
                    <div class="job-group-name">${displayName}</div>
                    <div class="job-group-meta">
                        <span>${batchCount} batch${batchCount > 1 ? 'es' : ''}</span>
                        <span>·</span>
                        <span>${uniqueCompanies} entreprise${uniqueCompanies !== 1 ? 's' : ''}</span>
                        <span>·</span>
                        <span>${t('dashboard.lastBatch', { date: formatDateTime(latest.created_at) })}</span>
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
                        <div class="job-timeline-item" onclick="event.stopPropagation(); window.location.hash='#/job/${encodeURIComponent(b.batch_id)}'">
                            <div class="timeline-dot ${bIdx === 0 ? 'latest' : ''}"></div>
                            <div class="timeline-content">
                                <div class="timeline-batch-id">${escapeHtml(b.batch_id)}</div>
                                <div class="timeline-meta">
                                    <span>${formatDateTime(b.created_at)}</span>
                                    <span>${bScraped}/${bTotal} ${t('dashboard.scrapedOf')}</span>
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
                            onclick="event.stopPropagation()" title="${t('dashboard.deleteSector')}">✕</button>
                        <div onclick="window.location.hash='#/search?q=${encodeURIComponent(s.sector)}'" style="cursor:pointer">
                            <div class="dept-card-header">
                                <span class="dept-card-number">🏭</span>
                                <span class="dept-card-count">${total} entreprise${total > 1 ? 's' : ''}</span>
                            </div>
                            <div class="dept-card-name">${escapeHtml(s.sector)}</div>
                            <div style="font-size:var(--font-xs); color:var(--text-muted); margin-top:var(--space-xs)">
                                ${s.batch_count || 0} batch${(s.batch_count || 0) > 1 ? 'es' : ''}
                                ${depts.length > 0 ? ` · ${depts.slice(0, 3).join(', ')}${depts.length > 3 ? '…' : ''}` : ''}
                                ${hasRunning ? ` · ${t('dashboard.inProgress')}` : ''}
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
                title: t('dashboard.deleteSector'),
                body: t('dashboard.deleteSectorBody', { label }),
                confirmLabel: t('dashboard.deleteConfirm'),
                danger: true,
                onConfirm: async () => {
                    const result = await deleteSectorTags(sector);
                    if (result._ok) {
                        showToast(t('dashboard.deleteSectorSuccess', { sector, count: result.tags_removed }), 'success');
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
            <div class="empty-state-text">${t('dashboard.noJobFound')}</div>
            <p style="color: var(--text-muted)">${t('dashboard.noJobHint')}</p>
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

    // Group by sector (first word of batch_name, uppercased)
    const sectors = {};
    for (const j of jobs) {
        const sector = (j.sector || (j.batch_name || '').split(' ')[0]).toUpperCase().trim();
        if (!sector) continue;
        if (!sectors[sector]) sectors[sector] = { name: sector, batches: [], totalCompanies: 0 };
        sectors[sector].batches.push(j);
        sectors[sector].totalCompanies += (j.unique_companies || j.companies_qualified || j.companies_scraped || 0);
    }

    // Sort by total scraped (most data first)
    const sorted = Object.values(sectors).sort((a, b) => b.totalCompanies - a.totalCompanies);

    view.innerHTML = `
        <div class="dept-grid">
            ${sorted.map(s => {
                const batchCount = s.batches.length;
                const hasRunning = s.batches.some(b => b.status === 'in_progress');
                const depts = [...new Set(s.batches.map(b => {
                    const parts = (b.batch_name || '').split(' ');
                    return parts.length > 1 ? parts.slice(1).join(' ') : '';
                }).filter(Boolean))];

                return `
                    <div class="dept-card" style="position:relative" data-sector="${escapeHtml(s.name)}">
                        <button class="card-delete-btn" data-delete-type="sector" data-delete-id="${escapeHtml(s.name)}" data-delete-label="${escapeHtml(s.name)} (${s.totalCompanies} entreprises)"
                            onclick="event.stopPropagation()" title="${t('dashboard.deleteSector')}">✕</button>
                        <div onclick="window.location.hash='#/search?q=${encodeURIComponent(s.name)}'" style="cursor:pointer">
                            <div class="dept-card-header">
                                <span class="dept-card-number">🏭</span>
                                <span class="dept-card-count">${s.totalCompanies} entreprise${s.totalCompanies > 1 ? 's' : ''}</span>
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
                title: t('dashboard.deleteSector'),
                body: t('dashboard.deleteSectorBody', { label }),
                confirmLabel: t('dashboard.deleteConfirm'),
                danger: true,
                onConfirm: async () => {
                    const result = await deleteSectorTags(sector);
                    if (result._ok) {
                        showToast(t('dashboard.deleteSectorSuccess', { sector, count: result.tags_removed }), 'success');
                        renderDashboard(rootContainer);
                    } else {
                        showToast(extractApiError(result), 'error');
                    }
                },
            });
        });
    });
}

// ── Data Analysis View (redesigned — 4 focused panels + batch success) ──────────
function renderAnalysis(data, isAdmin, rootContainer, batchData, departments = []) {
    const view = document.getElementById('dashboard-view');
    if (!view) return;
    const q = data.quality || {};
    const gaps = data.gaps || {};
    const enrichers = data.enrichers || {};
    const pipeline = data.pipeline || {};
    const trend = pipeline.weekly_trend || [];
    const recentJobs = pipeline.recent_jobs || [];
    const weekComp = data.week_comparison || { this_week: { companies: 0, batches: 0 }, last_week: { companies: 0, batches: 0 } };
    const topSearches = data.top_searches || [];
    const recentSearches = data.recent_searches || [];

    // ── Section 1: Hero stats bar ────────────────────────────────
    const thisWeekCo = weekComp.this_week.companies || 0;
    const lastWeekCo = weekComp.last_week.companies || 0;
    const weekDiff = thisWeekCo - lastWeekCo;
    const weekTrendHtml = weekDiff > 0
        ? `<span class="analytics-trend-up">${t('dashboard.analysisTrendUp', { count: weekDiff })}</span>`
        : weekDiff < 0
        ? `<span class="analytics-trend-down">${t('dashboard.analysisTrendDown', { count: weekDiff })}</span>`
        : `<span style="font-size:var(--font-xs); color:var(--text-muted)">${t('dashboard.analysisTrendStable')}</span>`;

    const heroSection = `
        <div class="analytics-hero-bar">
            <div class="analytics-hero-card">
                <div class="analytics-hero-number">${(q.total || 0).toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')}</div>
                <div class="analytics-hero-label">${t('dashboard.analysisHeroEnriched')}</div>
                ${weekTrendHtml}
            </div>
            <div class="analytics-hero-card">
                <div class="analytics-hero-number">${q.phone_pct || 0}%</div>
                <div class="analytics-hero-label">${t('dashboard.analysisPhoneRate')}</div>
                <span style="font-size:var(--font-xs); color:var(--text-muted)">${(q.with_phone || 0).toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')} entreprises</span>
            </div>
            <div class="analytics-hero-card">
                <div class="analytics-hero-number">${q.email_pct || 0}%</div>
                <div class="analytics-hero-label">${t('dashboard.analysisEmailRate')}</div>
                <span style="font-size:var(--font-xs); color:var(--text-muted)">${(q.with_email || 0).toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')} entreprises</span>
            </div>
            <div class="analytics-hero-card">
                <div class="analytics-hero-number">${q.website_pct || 0}%</div>
                <div class="analytics-hero-label">${t('dashboard.analysisWebRate')}</div>
                <span style="font-size:var(--font-xs); color:var(--text-muted)">${(q.with_website || 0).toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')} entreprises</span>
            </div>
        </div>
    `;

    // ── Section 2: Quality score donut + breakdown bars ──────────
    const qualitySection = `
        <div class="card analysis-panel" style="grid-column: 1 / -1; margin-bottom:var(--space-xl)">
            <h3 class="analysis-panel-title">${t('dashboard.analysisQualityTitle')}</h3>
            <div class="analysis-quality-layout">
                <div class="analysis-gauge-big">
                    ${renderGauge(q.overall_score || 0, t('dashboard.analysisGlobalScore'))}
                </div>
                <div class="analysis-metrics">
                    ${_metricBarThick(t('dashboard.analysisPhone'), q.with_phone || 0, q.total || 0, q.phone_pct || 0)}
                    ${_metricBarThick(t('dashboard.analysisEmail'), q.with_email || 0, q.total || 0, q.email_pct || 0)}
                    ${_metricBarThick(t('dashboard.analysisWeb'), q.with_website || 0, q.total || 0, q.website_pct || 0)}
                    ${_metricBarThick(t('dashboard.analysisSocial'), q.with_social || 0, q.total || 0, q.social_pct || 0)}
                </div>
                <div class="analysis-total-count">
                    <div class="analysis-total-number">${(q.total || 0).toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')}</div>
                    <div class="analysis-total-label">${t('dashboard.analysisTotal')}</div>
                </div>
            </div>
        </div>
    `;

    // ── Section 3: Recent activity ───────────────────────────────
    const recentSearchNames = recentSearches.map(r => escapeHtml(r.batch_name || '')).filter(Boolean);
    const topDept = departments.length > 0
        ? departments.slice().sort((a, b) => (b.company_count || 0) - (a.company_count || 0))[0]
        : null;

    const recentSection = `
        <div class="card analysis-panel">
            <h3 class="analysis-panel-title">${t('dashboard.analysisRecentTitle')}</h3>
            <div style="display:flex; flex-direction:column; gap:var(--space-md)">
                <div>
                    <div style="font-size:var(--font-lg); font-weight:700; color:var(--text-primary)">${thisWeekCo.toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')}</div>
                    <div style="font-size:var(--font-sm); color:var(--text-muted)">${t('dashboard.analysisWeekCompanies')}</div>
                </div>
                <div>
                    <div style="font-size:var(--font-lg); font-weight:700; color:var(--text-primary)">${weekComp.this_week.batches || 0}</div>
                    <div style="font-size:var(--font-sm); color:var(--text-muted)">${t('dashboard.analysisWeekBatches')}</div>
                </div>
                ${recentSearchNames.length > 0 ? `
                    <div>
                        <div style="font-size:var(--font-xs); font-weight:600; color:var(--text-secondary); text-transform:uppercase; letter-spacing:0.05em; margin-bottom:var(--space-xs)">${t('dashboard.analysisRecentSearches')}</div>
                        <div style="display:flex; flex-wrap:wrap; gap:var(--space-xs)">
                            ${recentSearchNames.map(n => `<span style="background:var(--bg-elevated); border:1px solid var(--border-default); border-radius:var(--radius-sm); padding:2px 8px; font-size:var(--font-xs); color:var(--text-secondary)">${n}</span>`).join('')}
                        </div>
                    </div>
                ` : ''}
                ${topDept ? `
                    <div>
                        <div style="font-size:var(--font-xs); font-weight:600; color:var(--text-secondary); text-transform:uppercase; letter-spacing:0.05em; margin-bottom:var(--space-xs)">${t('dashboard.analysisTopDept')}</div>
                        <div style="font-size:var(--font-sm); color:var(--text-primary); font-weight:600">
                            ${escapeHtml(topDept.departement || '')} — ${escapeHtml(topDept.department_name || '')}
                            <span style="color:var(--text-muted); font-weight:400">(${(topDept.company_count || 0).toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')} entreprises)</span>
                        </div>
                    </div>
                ` : ''}
            </div>
        </div>
    `;

    // ── Section 4: Top recherches ────────────────────────────────
    const topSearchesSection = topSearches.length > 0 ? `
        <div class="card analysis-panel">
            <h3 class="analysis-panel-title">${t('dashboard.analysisTopSearches')}</h3>
            <div>
                ${topSearches.map((s, i) => `
                    <div class="analytics-search-rank">
                        <div class="analytics-rank-number">${i + 1}</div>
                        <div style="flex:1; min-width:0">
                            <div style="font-weight:600; font-size:var(--font-sm); white-space:nowrap; overflow:hidden; text-overflow:ellipsis">${escapeHtml(s.batch_name || '—')}</div>
                            <div style="font-size:var(--font-xs); color:var(--text-muted)">
                                ${(s.company_count || 0).toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')} entreprises
                                · ${s.phone_rate || 0}% tél.
                                · ${s.email_rate || 0}% email
                            </div>
                        </div>
                    </div>
                `).join('')}
            </div>
        </div>
    ` : '';

    // ── Panels: Gaps + Enrichers (existing, kept) ────────────────
    const gTotal = gaps.total || 0;
    const complete = gaps.complete || 0;
    const missingAll = gaps.missing_all || 0;
    const missingPhone = gaps.missing_phone || 0;
    const missingEmail = gaps.missing_email || 0;
    const missingWeb = gaps.missing_website || 0;
    const completePct = gTotal > 0 ? Math.round(100 * complete / gTotal) : 0;

    const gapsPanel = `
        <div class="card analysis-panel">
            <h3 class="analysis-panel-title">${t('dashboard.analysisMissingData')}</h3>
            <div class="analysis-gaps">
                <div class="analysis-gap-highlight ${missingAll > 0 ? 'critical' : 'ok'}">
                    <div class="analysis-gap-number">${missingAll.toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')}</div>
                    <div class="analysis-gap-label">${missingAll > 0 ? t('dashboard.analysisMissingAll') : t('dashboard.analysisAllEnriched')}</div>
                </div>
                <div class="analysis-gap-bars">
                    ${_gapBar(t('dashboard.analysisMissingPhone'), missingPhone, gTotal)}
                    ${_gapBar(t('dashboard.analysisMissingEmail'), missingEmail, gTotal)}
                    ${_gapBar(t('dashboard.analysisMissingWeb'), missingWeb, gTotal)}
                </div>
                <div class="analysis-gap-complete">
                    <div class="analysis-gap-complete-bar">
                        <div class="analysis-gap-complete-fill" style="width:${completePct}%"></div>
                    </div>
                    <span class="analysis-gap-complete-text">${t('dashboard.analysisComplete', { count: complete.toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US'), pct: completePct })}</span>
                </div>
            </div>
        </div>
    `;

    const maps = enrichers.maps || {};
    const crawl = enrichers.crawl || {};
    const outcomes = enrichers.outcomes || {};

    const enricherPanel = `
        <div class="card analysis-panel">
            <h3 class="analysis-panel-title">${t('dashboard.analysisEnrichersTitle')}</h3>
            <div class="analysis-enrichers">
                ${_enricherCard(t('dashboard.analysisMaps'), maps)}
                ${_enricherCard(t('dashboard.analysisCrawl'), crawl)}
            </div>
            ${Object.keys(outcomes).length > 0 ? `
                <div class="analysis-outcomes">
                    ${Object.entries(outcomes).map(([k, v]) => {
                        const icons = { qualified: '✅', sirene_only: '📄', replaced: '🔄', failed: '❌', skipped: '⏭️' };
                        const labels = { qualified: t('dashboard.outcomeQualified'), sirene_only: t('dashboard.outcomeSireneOnly'), replaced: t('dashboard.outcomeReplaced'), failed: t('dashboard.outcomeFailed'), skipped: t('dashboard.outcomeSkipped') };
                        return `<span class="badge badge-muted">${icons[k] || '•'} ${labels[k] || k}: ${v.toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')}</span>`;
                    }).join('')}
                </div>
            ` : ''}
        </div>
    `;

    // ── Pipeline panel (existing, kept) ─────────────────────────
    const completed7d = pipeline.completed_7d || 0;
    const failed7d = pipeline.failed_7d || 0;
    const runningNow = pipeline.running_now || 0;
    const totalQualified = pipeline.total_qualified || 0;

    const maxQuality = Math.max(100, ...trend.map(t => t.avg_quality || 0));
    const trendBars = trend.length > 0 ? `
        <div style="display:flex; gap:var(--space-sm); align-items:flex-end; position:relative">
            <!-- Y-axis labels -->
            <div style="display:flex; flex-direction:column; justify-content:space-between; height:120px; padding-bottom:20px; min-width:28px; font-size:10px; color:var(--text-muted); text-align:right; flex-shrink:0">
                <span>100%</span>
                <span>50%</span>
                <span>0%</span>
            </div>
            <!-- Chart area with grid lines -->
            <div style="flex:1; position:relative; height:120px">
                <!-- Grid lines -->
                <div style="position:absolute; inset:0; pointer-events:none; padding-bottom:20px">
                    <div style="position:absolute; left:0; right:0; top:0; border-top:1px dashed rgba(255,255,255,0.15)"></div>
                    <div style="position:absolute; left:0; right:0; top:25%; border-top:1px dashed rgba(255,255,255,0.15)"></div>
                    <div style="position:absolute; left:0; right:0; top:50%; border-top:1px dashed rgba(255,255,255,0.15)"></div>
                    <div style="position:absolute; left:0; right:0; top:75%; border-top:1px dashed rgba(255,255,255,0.15)"></div>
                </div>
                <div class="analysis-trend-chart" style="position:relative; z-index:1">
                    ${trend.map(tw => {
                        const qVal = tw.avg_quality || 0;
                        const height = Math.max(4, Math.round(100 * qVal / maxQuality));
                        const weekLabel = (tw.week || '').replace(/^\d{4}-W/, 'S');
                        const color = qVal >= 60 ? '#22c55e' : qVal >= 30 ? '#f59e0b' : '#ef4444';
                        const lighterColor = qVal >= 60 ? '#4ade80' : qVal >= 30 ? '#fbbf24' : '#f87171';
                        return `
                            <div class="analysis-trend-bar-group" title="${tw.week}: ${qVal}%, ${tw.companies || 0} entreprises">
                                <div style="position:relative; display:flex; flex-direction:column; align-items:center">
                                    <span style="font-size:10px; color:var(--text-muted); margin-bottom:2px; line-height:1">${qVal}%</span>
                                    <div class="analysis-trend-bar" style="height:${height}%; background:linear-gradient(to top, ${color}, ${lighterColor}); max-width:44px; border-radius:6px 6px 0 0"></div>
                                </div>
                                <span class="analysis-trend-label">${weekLabel}</span>
                            </div>
                        `;
                    }).join('')}
                </div>
            </div>
        </div>
        <div class="analysis-trend-legend">
            <span><span class="analysis-legend-dot" style="background:var(--success)"></span> ≥60%</span>
            <span><span class="analysis-legend-dot" style="background:var(--warning, #f59e0b)"></span> 30-59%</span>
            <span><span class="analysis-legend-dot" style="background:var(--error, #ef4444)"></span> <30%</span>
        </div>
    ` : `<p style="color:var(--text-muted)">${t('dashboard.analysisNoActivity')}</p>`;

    const pipelinePanel = `
        <div class="card analysis-panel" style="grid-column: 1 / -1">
            <h3 class="analysis-panel-title">${t('dashboard.analysisPipelineTitle')}</h3>
            <div class="analysis-pipeline-stats">
                <div class="analysis-stat-card stat-success">
                    <div class="analysis-stat-number">${completed7d}</div>
                    <div class="analysis-stat-label">${t('dashboard.analysisCompleted7d')}</div>
                </div>
                <div class="analysis-stat-card stat-error">
                    <div class="analysis-stat-number">${failed7d}</div>
                    <div class="analysis-stat-label">${t('dashboard.analysisFailed7d')}</div>
                </div>
                <div class="analysis-stat-card stat-running">
                    <div class="analysis-stat-number">${runningNow}</div>
                    <div class="analysis-stat-label">${t('dashboard.analysisRunning')}</div>
                </div>
                <div class="analysis-stat-card stat-neutral">
                    <div class="analysis-stat-number">${totalQualified.toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')}</div>
                    <div class="analysis-stat-label">${t('dashboard.analysisTotalQualified')}</div>
                </div>
            </div>

            <h4 class="analysis-sub-title">${t('dashboard.analysisQualityWeekly')}</h4>
            ${trendBars}

            ${recentJobs.length > 0 ? `
                <h4 class="analysis-sub-title">${t('dashboard.analysisLastBatches')}</h4>
                <div class="analysis-recent-jobs">
                    ${recentJobs.map(j => `
                        <div class="analysis-recent-job" onclick="window.location.hash='#/job/${encodeURIComponent(j.batch_id)}'">
                            <span class="analysis-recent-name">${escapeHtml(j.batch_name)}</span>
                            ${statusBadge(j.status)}
                            <span class="analysis-recent-count">${j.companies_scraped || 0}/${j.batch_size || 0}</span>
                            <span class="analysis-recent-date">${formatDateTime(j.created_at)}</span>
                        </div>
                    `).join('')}
                </div>
            ` : ''}
        </div>
    `;

    // ── Section 5: Department coverage (existing, at bottom) ─────
    const deptSection = departments.length > 0 ? `
        <div class="card analysis-panel" style="grid-column: 1 / -1">
            <h3 class="analysis-panel-title">${t('dashboard.analysisDeptCoverage')}</h3>
            <div style="display:grid; grid-template-columns:repeat(auto-fill, minmax(160px, 1fr)); gap:var(--space-sm)">
                ${departments.slice(0, 20).map(d => `
                    <div style="background:var(--bg-elevated); border:1px solid var(--border-default); border-radius:var(--radius-sm); padding:var(--space-sm) var(--space-md); cursor:pointer"
                        onclick="window.location.hash='#/department/${encodeURIComponent(d.departement)}'">
                        <div style="font-weight:700; font-size:var(--font-sm)">${escapeHtml(d.departement)} <span style="color:var(--text-muted); font-weight:400; font-size:var(--font-xs)">${escapeHtml(d.department_name || '')}</span></div>
                        <div style="font-size:var(--font-xs); color:var(--text-muted)">${(d.company_count || 0).toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')} entreprises</div>
                        <div style="font-size:var(--font-xs); color:var(--text-muted)">📞 ${d.phone_pct || 0}% · ✉️ ${d.email_pct || 0}%</div>
                    </div>
                `).join('')}
            </div>
        </div>
    ` : '';

    // ── Batch Pipeline Success panel (existing, at bottom) ───────
    const batches = (batchData && batchData.batches) || [];
    const batchSuccessPanel = batches.length > 0 ? `
        <div class="card analysis-panel" style="grid-column: 1 / -1">
            <h3 class="analysis-panel-title">${t('dashboard.analysisBatchSuccess')}</h3>
            <div style="display:flex; flex-direction:column; gap:var(--space-md)">
                ${batches.map(b => {
                    const batchLabel = b.batch_name || b.batch_id || '—';
                    const dateStr = b.created_at ? new Date(b.created_at).toLocaleDateString('fr-FR') : '';
                    const isUpload = b.is_upload;
                    const steps = b.steps || [];

                    return `
                        <div style="
                            background: var(--bg-secondary, rgba(255,255,255,0.04));
                            border-radius: var(--radius-md);
                            padding: var(--space-md) var(--space-lg);
                            cursor: pointer;
                        " onclick="window.location.hash='#/job/${encodeURIComponent(b.batch_id)}'" title="${t('job.clickForDetail')}">
                            <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:var(--space-md)">
                                <span style="font-weight:600; font-size:var(--font-sm)">${isUpload ? '📥' : '⚡'} ${escapeHtml(batchLabel)}</span>
                                <span style="color:var(--text-muted); font-size:var(--font-xs)">${dateStr}</span>
                            </div>
                            <div class="batch-steps-row" style="margin-bottom:var(--space-sm)">
                                ${steps.map(s => {
                                    const pct = s.pct || 0;
                                    const color = pct >= 60 ? 'var(--success)' : pct >= 30 ? 'var(--warning, #f59e0b)' : 'var(--error, #ef4444)';
                                    return `
                                    <div>
                                        <div style="display:flex; justify-content:space-between; font-size:var(--font-xs); margin-bottom:4px">
                                            <span style="font-weight:500">${escapeHtml(s.label)}</span>
                                            <span style="color:${color}; font-weight:600">${pct}%</span>
                                        </div>
                                        <div style="background:rgba(255,255,255,0.06); border-radius:4px; height:6px; overflow:hidden; margin-bottom:4px">
                                            <div style="width:${pct}%; height:100%; background:${color}; border-radius:4px; transition:width 0.4s"></div>
                                        </div>
                                        <div style="font-size:10px; color:var(--text-secondary)">${escapeHtml(s.detail)}</div>
                                    </div>
                                    `;
                                }).join('')}
                            </div>
                            <div style="font-size:var(--font-xs); color:var(--text-secondary); border-top:1px solid rgba(255,255,255,0.05); padding-top:var(--space-sm); margin-top:var(--space-sm)">
                                📝 ${escapeHtml(b.summary)}
                            </div>
                        </div>
                    `;
                }).join('')}
            </div>
        </div>
    ` : '';

    // ── Assemble ────────────────────────────────────────────────
    view.innerHTML = `
        <div class="analysis-grid">
            <div style="grid-column: 1 / -1">
                ${heroSection}
            </div>
            ${qualitySection}
            <div style="grid-column: 1 / -1; display:grid; grid-template-columns:1fr 1fr; gap:var(--space-lg)">
                ${recentSection}
                ${topSearchesSection}
            </div>
            ${gapsPanel}
            ${enricherPanel}
            ${pipelinePanel}
            ${deptSection}
            ${batchSuccessPanel}
        </div>
    `;

    // Animate metric bars in after render
    requestAnimationFrame(() => {
        view.querySelectorAll('.analysis-metric-fill-anim').forEach(el => {
            const target = el.dataset.width || '0';
            el.style.width = target + '%';
        });
    });
}

// ── Analysis helpers ─────────────────────────────────────────────
function _metricBar(label, value, total, pct) {
    return `
        <div class="analysis-metric">
            <div class="analysis-metric-header">
                <span class="analysis-metric-label">${label}</span>
                <span class="analysis-metric-value">${value.toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')} / ${total.toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')} (${pct}%)</span>
            </div>
            <div class="analysis-metric-track">
                <div class="analysis-metric-fill" style="width:${pct}%"></div>
            </div>
        </div>
    `;
}

function _metricBarThick(label, value, total, pct) {
    return `
        <div class="analysis-metric">
            <div class="analysis-metric-header">
                <span class="analysis-metric-label">${label}</span>
                <span class="analysis-metric-value">${value.toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')} / ${total.toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')} (${pct}%)</span>
            </div>
            <div class="analysis-metric-track" style="height:8px">
                <div class="analysis-metric-fill analysis-metric-fill-anim" data-width="${pct}" style="width:0%; height:100%; transition:width 0.6s ease"></div>
            </div>
        </div>
    `;
}

function _gapBar(label, missing, total) {
    const pct = total > 0 ? Math.round(100 * missing / total) : 0;
    return `
        <div class="analysis-gap-row">
            <span class="analysis-gap-row-label">${label}</span>
            <div class="analysis-gap-row-track">
                <div class="analysis-gap-row-fill" style="width:${pct}%"></div>
            </div>
            <span class="analysis-gap-row-value">${missing.toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')} (${pct}%)</span>
        </div>
    `;
}

function _enricherCard(title, data) {
    const rate = data.rate || 0;
    const statusIcon = rate >= 70 ? '🟢' : rate >= 40 ? '🟡' : '🔴';
    const last24Total = data.last_24h_total || 0;
    const last24Success = data.last_24h_success || 0;
    const last24Rate = last24Total > 0 ? Math.round(100 * last24Success / last24Total) : 0;

    // Format last run as relative time
    let lastRunText = '—';
    if (data.last_run) {
        const d = new Date(data.last_run);
        if (!isNaN(d.getTime())) {
            const diffMin = Math.round((Date.now() - d.getTime()) / 60000);
            if (diffMin < 60) lastRunText = t('dashboard.analysisAgoMin', { count: diffMin });
            else if (diffMin < 1440) lastRunText = t('dashboard.analysisAgoH', { count: Math.round(diffMin / 60) });
            else lastRunText = t('dashboard.analysisAgoD', { count: Math.round(diffMin / 1440) });
        }
    }

    return `
        <div class="analysis-enricher-card">
            <div class="analysis-enricher-header">
                <span class="analysis-enricher-title">${title}</span>
                <span class="analysis-enricher-status">${statusIcon} ${rate}%</span>
            </div>
            <div class="analysis-enricher-rate-bar">
                <div class="analysis-enricher-rate-fill" style="width:${rate}%"></div>
            </div>
            <div class="analysis-enricher-stats">
                <span>📊 ${(data.total || 0).toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')} total</span>
                <span>✅ ${(data.success || 0).toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US')} succès</span>
                <span>⏱️ ${data.avg_time_s || 0}s moy.</span>
            </div>
            <div class="analysis-enricher-live">
                <span class="analysis-enricher-24h">${last24Total > 0 ? t('dashboard.analysisTotal24h', { success: last24Success, total: last24Total, rate: last24Rate }) : t('dashboard.analysisNone24h')}</span>
                <span class="analysis-enricher-lastrun">${t('dashboard.analysisLastRun')} ${lastRunText}</span>
            </div>
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

