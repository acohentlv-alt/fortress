/**
 * Job Page — Drill-down into a specific job
 */

import { getJob, getJobCompanies, getJobQuality, getJobSummary, getJobQueries, getExpansionSuggestions, getExportUrl, deleteJob, untagCompany, enrichCompany, startDeepEnrich } from '../api.js';
import { renderGauge, companyCard, renderPagination, breadcrumb, statusBadge, formatDateTime, escapeHtml, showConfirmModal, showToast } from '../components.js';
import { GlobalSelection } from '../state.js';
import { t } from '../i18n.js';

// ── Selection state ──────────────────────────────────────────────
let selectionMode = false;
let selectedSirens = GlobalSelection;
let _currentBatchId = null;
let _currentBatchName = null;
let _currentPage = 1;
let _currentSort = 'completude';

export async function renderJob(container, batchId) {
    batchId = decodeURIComponent(batchId);

    const [job, quality, summary] = await Promise.all([
        getJob(batchId),
        getJobQuality(batchId),
        getJobSummary(batchId),
    ]);

    if (!job || job._ok === false || job.error) {
        const isServerError = job && job._ok === false && job._status >= 500;
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">${isServerError ? '⚠️' : '❌'}</div>
                <div class="empty-state-text">${isServerError ? t('job.serverUnavailable') : t('job.jobNotFound')}</div>
                <p style="color:var(--text-muted)">${isServerError ? t('job.retryInMoment') : ''}</p>
                <a href="#/" class="btn btn-primary">${t('job.backToDashboard')}</a>
            </div>
        `;
        return;
    }

    const batchSize = job.batch_size || job.total_companies || 1;
    const scraped = job.companies_scraped || 0;
    const qualified = job.companies_qualified || 0;
    const progressPct = Math.min(100, Math.round((qualified / batchSize) * 100));
    const q = quality || {};

    // ── Build summary card HTML ──────────────────────────────────
    function buildSummaryCard(s) {
        if (!s) return '';
        const target = s.target || 0;
        const qual = s.qualified || 0;
        const black = (s.triage && s.triage.black) || 0;
        const green = (s.triage && s.triage.green) || 0;
        const yellow = (s.triage && s.triage.yellow) || 0;
        const red = (s.triage && s.triage.red) || 0;
        const total = black + green + yellow + red;

        const nonQualified = total - qual;
        const breakdownLines = [];
        if (red > 0) breakdownLines.push(`<div style="display:flex;align-items:center;gap:8px;padding:5px 0"><span style="color:#10b981">🟢</span> <span>${red} nouvelle${red > 1 ? 's' : ''} entreprise${red > 1 ? 's' : ''} — traitement complet</span></div>`);
        if (yellow > 0) breakdownLines.push(`<div style="display:flex;align-items:center;gap:8px;padding:5px 0"><span style="color:#fdcb6e">🟡</span> <span>${yellow} enrichissement${yellow > 1 ? 's' : ''} partiel${yellow > 1 ? 's' : ''}</span></div>`);
        if (green > 0) breakdownLines.push(`<div style="display:flex;align-items:center;gap:8px;padding:5px 0"><span style="color:#00cec9">♻️</span> <span>${green} déjà enrichie${green > 1 ? 's' : ''} — ignorée${green > 1 ? 's' : ''}</span></div>`);
        if (nonQualified > 0) breakdownLines.push(`<div style="display:flex;align-items:center;gap:8px;padding:5px 0"><span style="color:var(--text-muted)">⚪</span> <span>${nonQualified} sans téléphone ni site web</span></div>`);
        if (black > 0) breakdownLines.push(`<div style="display:flex;align-items:center;gap:8px;padding:5px 0"><span>⚫</span> <span>${black} en liste noire ou sans nom</span></div>`);
        if ((s.failed || 0) > 0) breakdownLines.push(`<div style="display:flex;align-items:center;gap:8px;padding:5px 0"><span style="color:#e74c3c">❌</span> <span>${s.failed} échec${s.failed > 1 ? 's' : ''}</span></div>`);

        const shortfallHtml = s.shortfall_reason
            ? `<div style="margin-top:var(--space-md);padding:12px;background:rgba(253,203,110,0.1);border-left:3px solid #fdcb6e;border-radius:4px;font-size:var(--font-sm);color:var(--text-secondary)">💡 ${escapeHtml(s.shortfall_reason)}</div>`
            : '';

        return `
            <div class="card" style="margin-bottom:var(--space-xl); border-left:3px solid var(--accent)">
                <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-md)">
                    Résumé du batch
                </h3>
                <p style="font-size:var(--font-lg); font-weight:700; margin-bottom:var(--space-md)">
                    ${qual} entreprise${qual > 1 ? 's' : ''} qualifiée${qual > 1 ? 's' : ''} sur ${target} demandée${target > 1 ? 's' : ''}
                </p>
                ${(() => {
                    const yieldPct = target > 0 ? Math.round((qual / target) * 100) : 0;
                    const yieldColor = yieldPct >= 70 ? '#10b981' : yieldPct >= 30 ? '#f59e0b' : '#ef4444';
                    return `
                    <div style="margin-bottom:var(--space-md)">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px">
                            <span style="font-size:var(--font-xs); color:var(--text-muted); font-weight:600; text-transform:uppercase; letter-spacing:0.06em">Rendement</span>
                            <span style="font-size:var(--font-sm); font-weight:700; color:${yieldColor}">${yieldPct}%</span>
                        </div>
                        <div style="height:6px; background:var(--bg-secondary); border-radius:3px; overflow:hidden">
                            <div style="width:${yieldPct}%; height:100%; background:${yieldColor}; border-radius:3px; transition:width 0.4s ease"></div>
                        </div>
                    </div>`;
                })()}
                <div style="font-size:var(--font-sm); color:var(--text-secondary)">
                    ${breakdownLines.join('')}
                </div>
                ${shortfallHtml}
            </div>
        `;
    }

    container.innerHTML = `
        ${breadcrumb([
        { label: 'Dashboard', href: '#/' },
        { label: job.batch_name },
    ])}

        ${buildSummaryCard(summary)}

        <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:var(--space-xl); flex-wrap:wrap; margin-bottom:var(--space-2xl)">
            <div>
                <h1 class="page-title">
                    ${escapeHtml(job.batch_name)}
                    ${job.batch_number ? `<span style="font-size:var(--font-sm); font-weight:400; color:var(--text-muted); margin-left:var(--space-sm)">${t('job.batchNumber', { number: job.batch_number })}</span>` : ''}
                </h1>
                <div style="display:flex; align-items:center; gap:var(--space-md); margin-top:var(--space-sm)">
                    ${statusBadge(job.status)}
                    <span style="color:var(--text-secondary); font-size:var(--font-sm)">
                        ${t('job.createdOn')} ${formatDateTime(job.created_at)}
                    </span>
                    ${(job.triage_green || 0) > 0 ? `<span class="badge" style="background:rgba(34,197,94,0.15); color:rgb(34,197,94); border:1px solid rgba(34,197,94,0.3)">🟢 ${job.triage_green} ${t('job.existingData')}</span>` : ''}
                </div>
            </div>
            <div style="display:flex; gap:var(--space-sm)">
                <a href="${getExportUrl(batchId, 'csv')}" class="btn btn-secondary" download>📥 CSV</a>
                <a href="${getExportUrl(batchId, 'xlsx')}" class="btn btn-secondary" download>📥 XLSX</a>
                <a href="${getExportUrl(batchId, 'jsonl')}" class="btn btn-secondary" download>📥 JSONL</a>
                ${job.status !== 'in_progress' ? `<button id="btn-rerun" class="btn btn-secondary" title="${t('job.rerun')}">${t('job.rerun')}</button>` : ''}
                <button id="btn-delete-job" class="btn btn-secondary" title="${t('job.delete')}" style="color:var(--danger)">🗑️</button>
                ${job.status === 'in_progress' ?
            `<a href="#/monitor/${encodeURIComponent(batchId)}" class="btn btn-primary">${t('job.liveMonitor')}</a>` : ''}
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
                <span style="font-weight:600">${(job.status === 'completed' || job.status === 'interrupted') ? t('job.results') : t('job.progress')} — ${t('job.companiesCount', { count: batchSize, plural: batchSize > 1 ? 's' : '' })}</span>
                <div style="display:flex; align-items:center; gap:var(--space-md)">
                    <span style="color:var(--text-secondary); font-weight:500">${qualified} ${t('job.foundLabel', { plural: qualified !== 1 ? 's' : '' })}</span>
                    ${(job.pending_links || 0) > 0 ? `<span class="badge" style="background:rgba(245,158,11,0.15); color:rgb(245,158,11); border:1px solid rgba(245,158,11,0.3)">${t('job.pendingLinks', { count: job.pending_links, plural: job.pending_links > 1 ? 's' : '' })}</span>` : ''}
                    <button id="toggle-provenance" title="${t('job.provenanceDetails')}" style="background:none;border:none;cursor:pointer;font-size:14px;opacity:0.4;transition:opacity 0.2s" onmouseover="this.style.opacity=1" onmouseout="this.style.opacity=0.4">ℹ️</button>
                </div>
            </div>
            ${job.status === 'in_progress' ? `
            <div class="progress-bar" style="height:10px">
                <div class="progress-bar-fill progress-bar-accent animated" style="width:${progressPct}%"></div>
            </div>` : ''}
            <div id="provenance-panel" style="display:none; margin-top:var(--space-lg); padding:var(--space-lg); background:var(--bg-secondary); border-radius:var(--radius-sm); border:1px solid var(--border-subtle)">
                <div style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-md)">
                    ${t('job.provenanceDetails')}
                </div>
                ${scraped > 0 ? `<div style="font-size:var(--font-sm); color:var(--text-secondary); margin-bottom:var(--space-md)">
                    🔍 <strong>${scraped}</strong> entreprises évaluées — <strong>${qualified}</strong> retenues, <strong>${scraped - qualified}</strong> ignorées car déjà enrichies par Maps ou sans correspondance.
                </div>` : ''}
                ${(job.triage_green || 0) > 0 ? `<div style="font-size:var(--font-sm); color:rgb(34,197,94); margin-bottom:var(--space-md)">
                    ♻️ <strong>${job.triage_green}</strong> entreprise${job.triage_green > 1 ? 's étaient' : ' était'} déjà dans la base de données avec des données Maps complètes.
                </div>` : ''}
                <div id="provenance-sources" style="display:flex; gap:var(--space-xl); flex-wrap:wrap; font-size:var(--font-sm)">
                    <span style="color:var(--text-secondary)">${t('job.provenanceSources')}</span>
                </div>
            </div>
        </div>

        <!-- Quality Gauges -->
        <div class="card" style="margin-bottom:var(--space-xl)">
            <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                ${t('job.qualityTitle')}
            </h3>
            <div style="display:flex; gap:var(--space-2xl); justify-content:center; flex-wrap:wrap">
                ${renderGauge(q.phone_pct || 0, '📞 Tél.')}
                ${renderGauge(q.email_pct || 0, '✉️ Email')}
                ${renderGauge(q.website_pct || 0, '🌐 Web')}
                ${renderGauge(q.officers_pct || 0, '👤')}
                ${renderGauge(q.financials_pct || 0, '💰')}
                ${renderGauge(q.siret_pct || q.social_pct || 0, '🔗')}
            </div>
            <div style="text-align:center; font-size:var(--font-sm); color:var(--text-muted); margin-top:var(--space-lg)">
                ${t('job.companiesCount', { count: batchSize, plural: batchSize > 1 ? 's' : '' })}
            </div>
        </div>

        <!-- Departments touched -->
        ${job.departments && job.departments.length > 0 ? `
            <div class="card" style="margin-bottom:var(--space-xl)">
                <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                    ${t('job.deptsCovered')}
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

        <!-- Smart Expansion Suggestions -->
        <div id="expansion-card-container"></div>

        <!-- Query History -->
        <div class="card" id="queries-card" style="margin-bottom:var(--space-xl)">
            <div style="display:flex; align-items:center; justify-content:space-between; cursor:pointer" id="queries-toggle">
                <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin:0">
                    Détail des recherches
                </h3>
                <span id="queries-chevron" style="color:var(--text-muted); font-size:14px; transition:transform 0.2s">▼</span>
            </div>
            <div id="queries-panel" style="display:none; margin-top:var(--space-lg)">
                <div class="loading"><div class="spinner"></div></div>
            </div>
        </div>

        <!-- Companies -->
        <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:var(--space-lg)">
            <h2 style="font-size:var(--font-lg); font-weight:600">${t('job.companiesLabel')}</h2>
            <div style="display:flex; gap:var(--space-sm)">
                <button id="btn-select-mode" class="btn-select-mode" title="${t('job.selectMode')}">
                    ${t('job.selectMode')}
                </button>
                <select id="job-sort" style="background:var(--bg-input); border:1px solid var(--border-default); border-radius:var(--radius-sm); padding:var(--space-sm) var(--space-md); color:var(--text-primary); font-family:var(--font-family); font-size:var(--font-sm)">
                    <option value="completude">${t('job.sortCompleteness')}</option>
                    <option value="name">${t('job.sortName')}</option>
                    <option value="date">${t('job.sortDate')}</option>
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
    _currentBatchName = job.batch_name;
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
                    container.innerHTML = `<span style="color:var(--text-muted)">${t('job.noSourceData')}</span>`;
                    return;
                }
                container.innerHTML = entries.map(([action, data]) => {
                    const label = icons[action] || action;
                    const color = data.rate >= 70 ? 'var(--success)' : data.rate >= 40 ? 'var(--warning)' : 'var(--text-muted)';
                    return `<div style="display:flex;flex-direction:column;gap:2px;padding:var(--space-sm) var(--space-md);background:var(--bg-elevated);border-radius:var(--radius-sm);min-width:180px">
                        <span style="font-weight:600">${label}</span>
                        <span style="color:${color}">${data.success}/${data.total} (${data.rate}%)</span>
                    </div>`;
                }).join('');
            }
        });
    }

    // Query history toggle
    const queriesToggle = document.getElementById('queries-toggle');
    let queriesLoaded = false;
    if (queriesToggle) {
        queriesToggle.addEventListener('click', async () => {
            const panel = document.getElementById('queries-panel');
            const chevron = document.getElementById('queries-chevron');
            if (!panel) return;
            const visible = panel.style.display !== 'none';
            panel.style.display = visible ? 'none' : 'block';
            if (chevron) chevron.style.transform = visible ? '' : 'rotate(180deg)';
            if (!visible && !queriesLoaded) {
                queriesLoaded = true;
                try {
                    const data = await getJobQueries(batchId);
                    const queries = (data && data.queries) || [];
                    if (queries.length === 0) {
                        panel.innerHTML = `<span style="color:var(--text-muted); font-size:var(--font-sm)">Aucune donnée de recherche disponible pour ce batch.</span>`;
                        return;
                    }
                    const totalCards = queries.reduce((s, q) => s + (q.cards_found || 0), 0);
                    const totalFiltered = queries.reduce((s, q) => s + (q.filtered_count || 0), 0);
                    const totalDedup = queries.reduce((s, q) => s + (q.dedup_count || 0), 0);
                    const totalNew = queries.reduce((s, q) => s + (q.new_companies || 0), 0);
                    const funnelParts = [];
                    funnelParts.push(`<span style="color:var(--text-primary); font-weight:600">${totalCards} résultat${totalCards > 1 ? 's' : ''}</span>`);
                    if (totalFiltered > 0) funnelParts.push(`<span style="color:var(--warning)">−${totalFiltered} filtrés</span>`);
                    if (totalDedup > 0) funnelParts.push(`<span style="color:var(--accent)">−${totalDedup} doublons</span>`);
                    funnelParts.push(`<span style="color:var(--success); font-weight:600">= ${totalNew} nouvelle${totalNew > 1 ? 's' : ''} entreprise${totalNew > 1 ? 's' : ''}</span>`);
                    const headerHtml = `<div style="font-size:var(--font-sm); color:var(--text-secondary); margin-bottom:var(--space-md)">
                        <div style="font-weight:600; margin-bottom:var(--space-xs)">${queries.length} recherche${queries.length > 1 ? 's' : ''} effectuée${queries.length > 1 ? 's' : ''}</div>
                        <div style="display:flex; align-items:center; gap:var(--space-sm); flex-wrap:wrap">${funnelParts.join('<span style="color:var(--text-muted); margin:0 2px">→</span>')}</div>
                    </div>`;
                    const rowsHtml = queries.map(q => {
                        const newColor = q.is_expansion
                            ? 'color:var(--accent)'
                            : q.new_companies > 0
                                ? 'color:var(--success)'
                                : 'color:var(--text-muted)';
                        const expansionBadge = q.is_expansion
                            ? `<span style="font-size:11px; padding:1px 6px; border-radius:3px; background:rgba(74,144,217,0.15); color:var(--accent); margin-left:6px; vertical-align:middle">expansion</span>`
                            : '';
                        const filteredBadge = (q.filtered_count > 0)
                            ? `<span style="color:var(--warning); min-width:60px; text-align:right">−${q.filtered_count} filtrés</span>`
                            : '';
                        const dedupBadge = (q.dedup_count > 0)
                            ? `<span style="color:var(--accent); min-width:60px; text-align:right">−${q.dedup_count} doublons</span>`
                            : '';
                        return `<div style="display:flex; align-items:center; gap:var(--space-md); padding:6px 0; border-bottom:1px solid var(--border-subtle); font-size:var(--font-sm)">
                            <span style="flex:1; color:var(--text-primary)">${escapeHtml(q.query)}${expansionBadge}</span>
                            <span style="color:var(--text-muted); min-width:80px; text-align:right">${q.cards_found} carte${q.cards_found > 1 ? 's' : ''}</span>
                            ${filteredBadge}
                            ${dedupBadge}
                            <span style="${newColor}; font-weight:600; min-width:80px; text-align:right">+${q.new_companies} entreprise${q.new_companies > 1 ? 's' : ''}</span>
                            <span style="color:var(--text-muted); min-width:60px; text-align:right">${q.duration_sec}s</span>
                        </div>`;
                    }).join('');
                    panel.innerHTML = headerHtml + `<div>${rowsHtml}</div>`;
                } catch (err) {
                    panel.innerHTML = `<span style="color:var(--danger); font-size:var(--font-sm)">Erreur lors du chargement de l'historique des recherches.</span>`;
                }
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
                title: t('job.confirmDelete'),
                body: `
                    <p><strong>Batch :</strong> ${escapeHtml(job.batch_name)}</p>
                    <p><strong>${t('job.createdOn')} :</strong> ${formatDateTime(job.created_at)}</p>
                    <p><strong>${scraped}</strong> entreprises collectées</p>
                    <p style="color:var(--danger)">⚠️ <strong>${t('job.confirmDeleteWithInfo')}</strong></p>
                    <p style="color:var(--text-muted)">${t('job.confirmDeleteKeep')}</p>
                `,
                confirmLabel: t('job.deleteConfirmPermanent'),
                danger: true,
                onConfirm: async () => {
                    const result = await deleteJob(batchId);
                    if (result._ok !== false) {
                        showToast(t('job.deleteSuccess', { contacts: result.deleted_contacts || 0, sirens: result.sirens_affected || 0 }), 'success');
                        window.location.hash = '#/';
                    } else {
                        showToast(result.error || t('job.deleteError'), 'error');
                    }
                },
            });
        });
    }

    // Rerun button
    const rerunBtn = document.getElementById('btn-rerun');
    if (rerunBtn) {
        rerunBtn.addEventListener('click', () => {
            // Pre-fill new batch form using search_queries (current format)
            const queries = job.search_queries || [];
            const parsedQueries = typeof queries === 'string' ? JSON.parse(queries) : queries;
            if (parsedQueries.length > 0) {
                sessionStorage.setItem('fortress_expansion_prefill', JSON.stringify({
                    queries: parsedQueries,
                    size: job.batch_size || 20
                }));
                window.location.hash = '#/new-batch';
            } else {
                // Fallback to old filters_json format
                const params = new URLSearchParams();
                if (job.filters_json) {
                    const f = typeof job.filters_json === 'string' ? JSON.parse(job.filters_json) : job.filters_json;
                    if (f.sector) params.set('sector', f.sector);
                    if (f.department) params.set('department', f.department);
                    if (f.size) params.set('size', f.size);
                }
                window.location.hash = `#/new-batch?${params.toString()}`;
            }
        });
    }

    // ── Smart Expansion Suggestions ─────────────────────
    if (job.status === 'completed' || job.status === 'interrupted' || job.status === 'failed') {
        getExpansionSuggestions(batchId).then(data => {
            const expansionContainer = document.getElementById('expansion-card-container');
            if (!expansionContainer) return;  // DOM guard
            if (!data || !data.suggestions || data.suggestions.length === 0) return;

            const sectorWord = data.sector_word || '';
            const deptCode = data.dept_code || '';

            const suggestionsHtml = data.suggestions.map(s => {
                const freshness = s.exhaustion_score <= 0.0
                    ? '<span style="color:var(--success); font-weight:600">Inexplore</span>'
                    : s.exhaustion_score <= 0.4
                        ? '<span style="color:var(--success)">Bon potentiel</span>'
                        : '<span style="color:var(--warning)">Partiellement explore</span>';
                const memoryNote = s.queries_in_memory > 0
                    ? `<span style="color:var(--text-muted); font-size:var(--font-xs)">${s.queries_in_memory} recherche${s.queries_in_memory > 1 ? 's' : ''} precedente${s.queries_in_memory > 1 ? 's' : ''}</span>`
                    : '<span style="color:var(--text-muted); font-size:var(--font-xs)">Aucune recherche</span>';

                return `
                    <div style="display:flex; align-items:center; justify-content:space-between; padding:var(--space-md); background:var(--bg-elevated); border-radius:var(--radius-sm); border:1px solid var(--border-subtle)">
                        <div style="flex:1">
                            <div style="font-weight:600; color:var(--text-primary)">${escapeHtml(s.dept_name)} (${escapeHtml(s.dept_code)})</div>
                            <div style="display:flex; gap:var(--space-md); margin-top:2px">${freshness} ${memoryNote}</div>
                        </div>
                        <button class="btn btn-primary expansion-launch-btn" data-dept="${escapeHtml(s.dept_code)}" data-sector="${escapeHtml(sectorWord)}" style="padding:var(--space-sm) var(--space-lg); font-size:var(--font-sm)">
                            Lancer
                        </button>
                    </div>
                `;
            }).join('');

            expansionContainer.innerHTML = `
                <div class="card" style="margin-bottom:var(--space-xl); border-left:3px solid var(--accent)">
                    <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-sm)">
                        Expansion geographique
                    </h3>
                    <p style="font-size:var(--font-sm); color:var(--text-secondary); margin-bottom:var(--space-lg)">
                        Continuer la recherche <strong>${escapeHtml(sectorWord)}</strong> dans les departements voisins du ${escapeHtml(deptCode)}
                    </p>
                    <div style="display:flex; flex-direction:column; gap:var(--space-sm)">
                        ${suggestionsHtml}
                    </div>
                </div>
            `;

            // Wire up launch buttons
            expansionContainer.querySelectorAll('.expansion-launch-btn').forEach(btn => {
                btn.addEventListener('click', () => {
                    const dept = btn.dataset.dept;
                    const sector = btn.dataset.sector;
                    const prefill = {
                        queries: [`${sector} ${dept}`],
                        size: 20,
                    };
                    sessionStorage.setItem('fortress_expansion_prefill', JSON.stringify(prefill));
                    window.location.hash = '#/new-batch';
                });
            });
        }).catch(() => {
            // Best-effort; swallow errors
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
                        ${t('job.allEnriched')}
                    </div>
                    <p style="color:var(--text-secondary); margin-bottom:var(--space-lg)">
                        ${t('job.allEnrichedBody', { count: greenCount, name: escapeHtml(batchName) })}
                    </p>
                    <p style="color:var(--text-muted); font-size:var(--font-sm); margin-bottom:var(--space-xl)">
                        ${t('job.allEnrichedSub')}
                    </p>
                    <a href="#/new-batch" class="btn btn-primary">${t('job.newSearch')}</a>
                </div>
            `;
        } else {
            companiesContainer.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">📭</div>
                    <div class="empty-state-text">${t('job.noCompaniesFound')}</div>
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
            const key = c.search_query || t('job.noQueryGroup');
            if (!groups.has(key)) groups.set(key, []);
            groups.get(key).push(c);
        }
        gridContent = [...groups.entries()].map(([query, companies]) => `
            <div style="margin-bottom:var(--space-lg)">
                <div style="display:flex; align-items:center; gap:var(--space-sm); margin-bottom:var(--space-md); padding:var(--space-sm) var(--space-md); background:var(--bg-secondary); border-radius:var(--radius-sm); border-left:3px solid var(--accent)">
                    <span style="font-size:var(--font-sm); color:var(--accent); font-weight:600">🔍 ${escapeHtml(query)}</span>
                    <span style="font-size:var(--font-xs); color:var(--text-muted); margin-left:auto">
                        ${t('job.queriesResultCount', { count: companies.length, plural: companies.length > 1 ? 's' : '' })}
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
                    title: t('job.removeConfirmTitle'),
                    body: `<p>${t('job.removeConfirmBody', { name: escapeHtml(name) })}</p>`,
                    confirmLabel: t('common.delete'),
                    danger: true,
                    checkboxLabel: t('job.alsoBlacklist'),
                    onConfirm: async (checkboxChecked) => {
                        const result = await untagCompany(siren, _currentBatchName);
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
                            showToast(t('job.removeSuccess', { name }), 'success');
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
                            showToast(t('job.removeError'), 'error');
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
        btn.innerHTML = selectionMode ? t('job.cancelSelect') : t('job.selectMode');
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
        <span class="bulk-count">${t('job.selected', { count: n, plural: n > 1 ? 's' : '' })}</span>
        <button class="btn btn-secondary" id="bulk-select-all">${t('job.selectAll')}</button>
        <button class="btn btn-primary" id="bulk-enrich-web">${t('job.enrichWeb')}</button>
        <button class="btn btn-danger" id="bulk-delete">${t('job.bulkDelete')}</button>
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
            showToast(t('job.maxEnrich'), 'error');
            return;
        }
        showToast(t('job.enrichLaunching', { count: sirens.length }), 'info');
        const result = await startDeepEnrich(sirens);
        if (result && result._ok !== false) {
            showToast(t('job.enrichLaunched', { count: sirens.length }), 'success');
            selectionMode = false;
            selectedSirens.clear();
            _removeBulkBar();
            await loadCompanies(_currentBatchId, _currentPage, _currentSort);
        } else {
            showToast(t('job.enrichError'), 'error');
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
        title: t('job.bulkDeleteTitle', { count: sirens.length, plural: sirens.length > 1 ? 's' : '' }),
        body: `<p>${t('job.bulkDeleteBody', { count: sirens.length, plural: sirens.length > 1 ? 's' : '' })}</p>`,
        confirmLabel: t('job.suppressPermanent'),
        danger: true,
        checkboxLabel: t('job.alsoBlacklistBulk'),
        onConfirm: async (blacklist) => {
            let ok = 0;
            for (const siren of sirens) {
                const res = await untagCompany(siren, _currentBatchName);
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
            showToast(t('job.bulkDeleteSuccess', { ok, total: sirens.length }), 'success');
            selectionMode = false;
            selectedSirens.clear();
            _removeBulkBar();
            await loadCompanies(_currentBatchId, _currentPage, _currentSort);
        }
    });
}
