/**
 * Pipeline Monitor Page — Live progress for running jobs
 *
 * Architecture:
 *   - Page skeleton rendered ONCE (no full DOM rebuild on poll)
 *   - update() patches only changing elements by ID
 *   - DOM refs cached after first render (perf: no re-query per poll)
 *   - Polling interval registered via registerCleanup()
 *   - Poll rate: 1.5s for snappy feeling
 *   - Polling failure: resilient (show warning, keep last data)
 *   - Company cards: append-only (track rendered SIRENs in Set)
 *   - Counters: animate only when value changes
 */

import { getJobs, getJob, getJobQuality, getJobCompanies, cancelJob, deleteJob } from '../api.js';
import {
    breadcrumb, statusBadge, formatDateTime, escapeHtml,
    renderGauge, companyCard, renderPipelineStages,
    animateCounter, renderProgressRing, showConfirmModal, showToast,
} from '../components.js';
import { registerCleanup } from '../app.js';
import { getCachedUser } from '../api.js';
import { t, getLang } from '../i18n.js';

let pollInterval = null;
let _monitorListAbort = null; // AbortController for renderMonitorList event listener

/**
 * Build the summary HTML from polled job data (no extra API call needed).
 * Uses triage counts already present in the job detail response.
 */
function renderSummary(job) {
    const green = job.triage_green || 0;
    const black = job.triage_black || 0;
    const yellow = job.triage_yellow || 0;
    const red = job.triage_red || 0;
    const failed = job.companies_failed || 0;
    const shortfall = job.shortfall_reason;

    const lines = [];
    if (green > 0) lines.push(`<div style="display:flex;align-items:center;gap:8px;padding:6px 0"><span style="color:#00cec9;font-size:1.1em">🟢</span> <span>${green} déjà enrichie${green > 1 ? 's' : ''} (ignorée${green > 1 ? 's' : ''})</span></div>`);
    if (black > 0) lines.push(`<div style="display:flex;align-items:center;gap:8px;padding:6px 0"><span style="font-size:1.1em">⚫</span> <span>${black} en liste noire ou sans nom</span></div>`);
    if (red > 0) lines.push(`<div style="display:flex;align-items:center;gap:8px;padding:6px 0"><span style="color:#e74c3c;font-size:1.1em">🔴</span> <span>${red} nouvelle${red > 1 ? 's' : ''} entreprise${red > 1 ? 's' : ''} (traitement complet)</span></div>`);
    if (yellow > 0) lines.push(`<div style="display:flex;align-items:center;gap:8px;padding:6px 0"><span style="color:#fdcb6e;font-size:1.1em">🟡</span> <span>${yellow} enrichissement${yellow > 1 ? 's' : ''} partiel${yellow > 1 ? 's' : ''}</span></div>`);
    if (failed > 0) lines.push(`<div style="display:flex;align-items:center;gap:8px;padding:6px 0"><span style="color:#e74c3c;font-size:1.1em">❌</span> <span>${failed} échec${failed > 1 ? 's' : ''}</span></div>`);

    if (shortfall) {
        lines.push(`<div style="margin-top:12px;padding:12px;background:rgba(253,203,110,0.1);border-left:3px solid #fdcb6e;border-radius:4px;font-size:var(--font-sm);color:var(--text-secondary)">💡 ${escapeHtml(shortfall)}</div>`);
    }

    if (lines.length === 0) {
        return '<span style="color:var(--text-muted)">En attente des premiers résultats...</span>';
    }

    return lines.join('');
}

/**
 * Translate a widening stop_reason code to a French label for the monitor panel.
 */
function stopReasonText(reason, cumulative) {
    switch (reason) {
        case 'threshold_met_dry_streak':
            return t('monitor.queriesStopThreshold').replace('{{n}}', cumulative != null ? cumulative : '?');
        case 'candidates_exhausted':
            return t('monitor.queriesStopExhausted');
        case 'max_per_primary':
            return t('monitor.queriesStopMaxPerPrimary').replace('{{n}}', '12');
        default:
            return t('monitor.queriesStopGeneric');
    }
}

/**
 * Render the queries panel HTML — groups primary + expansion queries.
 * Returns an HTML string.
 */
function renderQueriesPanel(queries) {
    if (!queries || queries.length === 0) {
        return `<span style="color:var(--text-muted)">${t('monitor.queriesEmpty')}</span>`;
    }
    const primaries = queries.filter(q => !q.is_expansion);
    const lines = [];
    for (const p of primaries) {
        lines.push(`<div style="display:flex; justify-content:space-between; gap:24px; padding:6px 12px 6px 0">
            <span><strong>${escapeHtml(p.query)}</strong></span>
            <span style="color:var(--text-muted)">&#8594; ${p.new_companies || 0} ${t('monitor.queriesNewEntities')}</span>
        </div>`);
        const expansions = queries.filter(q => q.is_expansion && q.primary_query === p.query);
        for (const e of expansions) {
            const typeLabel = e.widening_type === 'city'
                ? t('monitor.queriesCityLabel')
                : t('monitor.queriesPostalLabel');
            lines.push(`<div style="display:flex; justify-content:space-between; gap:24px; padding:4px 12px 4px 24px; font-size:var(--font-sm); color:var(--text-secondary)">
                <span>&#8627; ${escapeHtml(typeLabel)} — ${escapeHtml(e.value || '')}</span>
                <span>&#8594; ${e.new_companies || 0} ${t('monitor.queriesNewEntities')}</span>
            </div>`);
        }
        // Stop reason banner if last expansion has stop_reason
        const lastExp = expansions[expansions.length - 1];
        if (lastExp && lastExp.stop_reason) {
            const reasonText = stopReasonText(lastExp.stop_reason, lastExp.primary_cumulative_yield_after);
            lines.push(`<div style="padding:4px 0 8px 24px; font-size:var(--font-xs); color:var(--text-muted); font-style:italic">
                [${escapeHtml(reasonText)}]
            </div>`);
        }
    }
    return lines.join('');
}

export async function renderMonitor(container, batchId) {
    // Clear any previous polling (safety net — cleanup system handles this too)
    if (pollInterval) {
        clearInterval(pollInterval);
        pollInterval = null;
    }

    if (batchId && typeof batchId === 'string') {
        batchId = decodeURIComponent(batchId);
        await renderJobMonitor(container, batchId);
    } else {
        await renderMonitorList(container);
    }
}

async function renderMonitorList(container) {
    const user = getCachedUser();
    const canDelete = user?.role === 'admin' || user?.role === 'head';

    const jobs = await getJobs();
    const jobsList = Array.isArray(jobs) ? jobs : [];
    const runningJobs = jobsList.filter(j =>
        j.status === 'in_progress' || j.status === 'queued' || j.status === 'triage'
    );

    container.innerHTML = `
        <h1 class="page-title">${t('monitor.title')}</h1>
        <p class="page-subtitle">${t('monitor.subtitle')}</p>

        ${runningJobs.length === 0 ? `
            <div class="empty-state">
                <div class="empty-state-icon">📡</div>
                <div class="empty-state-text">${t('monitor.noBatch')}</div>
                <p style="color: var(--text-muted)">${t('monitor.selectBatch')}</p>
                <a href="#/new-batch" class="btn btn-primary" style="margin-top:var(--space-lg)">🚀 ${t('newBatch.launch')}</a>
            </div>
        ` : `
            <div class="job-list">
                ${runningJobs.map(j => {
        const scraped = j.companies_scraped || 0;
        return `
                        <div class="job-card" onclick="window.location.hash='#/monitor/${encodeURIComponent(j.batch_id)}'">
                            <div class="job-card-info">
                                <div class="job-card-name">${escapeHtml(j.batch_name)}</div>
                                <div class="job-card-meta">
                                    <span>${formatDateTime(j.created_at)}</span>
                                    <span>${scraped} ${t('monitor.entitiesFound')}</span>
                                    ${j.wave_total ? `<span>${t('monitor.wavechip', { current: j.wave_current || 0, total: j.wave_total })}</span>` : ''}
                                </div>
                            </div>
                            <div class="job-card-stats" style="display:flex;align-items:center;gap:var(--space-sm)">
                                ${statusBadge(j.status)}
                                <div style="display:flex;align-items:center">
                                    ${j.status === 'in_progress' || j.status === 'queued' || j.status === 'triage'
                                        ? `<span class="activity-pulse" title="${t('monitor.running')}"></span>`
                                        : `<span style="color:var(--success);font-size:1.1em">✓</span>`}
                                </div>
                                ${canDelete ? `<button class="btn-delete-job" data-batch-id="${escapeHtml(j.batch_id)}" data-batch-name="${escapeHtml(j.batch_name)}" data-running="true" title="Supprimer ce batch" style="background:none;border:none;cursor:pointer;color:var(--text-muted);font-size:1.1em;padding:4px 6px;border-radius:4px;line-height:1;flex-shrink:0" onmouseover="this.style.color='var(--danger)'" onmouseout="this.style.color='var(--text-muted)'">×</button>` : ''}
                            </div>
                        </div>
                    `;
    }).join('')}
            </div>
        `}

        ${jobsList.filter(j => (j.status === 'completed' || j.status === 'interrupted') && !j.batch_id.startsWith('MANUAL_')).length > 0 ? `
            <h2 style="font-size:var(--font-lg); font-weight:600; margin-top:var(--space-2xl); margin-bottom:var(--space-lg)">
                ${t('monitor.recentCompleted')}
            </h2>
            <div class="job-list">
                ${jobsList.filter(j => (j.status === 'completed' || j.status === 'interrupted') && !j.batch_id.startsWith('MANUAL_')).slice(0, 5).map(j => `
                    <div class="job-card" onclick="window.location.hash='#/job/${encodeURIComponent(j.batch_id)}'">
                        <div class="job-card-info">
                            <div class="job-card-name">${escapeHtml(j.batch_name)}</div>
                            <div class="job-card-meta">
                                <span>${formatDateTime(j.updated_at)}</span>
                                <span>${j.companies_scraped || 0} ${t('monitor.companies')}</span>
                                ${(j.exhaustive && !j.exhaustive_default) ? `<span class="chip-exhaustive">${t('monitor.exhaustive')}</span>` : ''}
                            </div>
                        </div>
                        <div style="display:flex;align-items:center;gap:var(--space-sm)">
                            ${statusBadge(j.status)}
                            ${canDelete ? `<button class="btn-delete-job" data-batch-id="${escapeHtml(j.batch_id)}" data-batch-name="${escapeHtml(j.batch_name)}" data-running="false" title="Supprimer ce batch" style="background:none;border:none;cursor:pointer;color:var(--text-muted);font-size:1.1em;padding:4px 6px;border-radius:4px;line-height:1;flex-shrink:0" onmouseover="this.style.color='var(--danger)'" onmouseout="this.style.color='var(--text-muted)'">×</button>` : ''}
                        </div>
                    </div>
                `).join('')}
            </div>
        ` : ''}
    `;

    // ── Event delegation for delete buttons ──────────────────────────
    // Abort any previous listener on this container (prevent stacking on re-render).
    if (_monitorListAbort) {
        _monitorListAbort.abort();
    }
    _monitorListAbort = new AbortController();
    // Use capture:true so this fires before the job-card inline onclick can
    // navigate away — stop propagation + prevent default + stop further capture
    // listeners ALL fire so the parent .job-card inline onclick navigation
    // cannot win the race in any browser.
    container.addEventListener('click', async (event) => {
        const btn = event.target.closest('.btn-delete-job');
        if (!btn) return;
        event.stopPropagation();
        event.stopImmediatePropagation();
        event.preventDefault();

        const batchId = btn.dataset.batchId;
        const batchName = btn.dataset.batchName;
        const isRunning = btn.dataset.running === 'true';

        // Surface the real API failure reason in the toast instead of a
        // generic "Erreur lors de la suppression". The previous text hid
        // 401/404/409 paths so users couldn't tell why delete failed.
        const explainError = (result) => {
            if (!result) return 'Erreur lors de la suppression.';
            if (result._status === 401) return 'Session expirée — reconnectez-vous.';
            if (result._status === 403) return 'Accès refusé.';
            if (result._status === 404) return 'Batch introuvable (déjà supprimé ?).';
            if (result._status === 409) return result.error || 'Arrêtez le batch en cours d\'abord.';
            return result.error || 'Erreur lors de la suppression.';
        };

        if (isRunning) {
            showConfirmModal({
                title: 'Supprimer ce batch en cours ?',
                body: `
                    <p><strong>Batch :</strong> ${escapeHtml(batchName)}</p>
                    <p style="color:var(--warning)">Ce batch est encore en cours. Il sera d'abord annulé, puis supprimé.</p>
                    <p style="color:var(--danger)">Toutes les données collectées seront supprimées.</p>
                `,
                confirmLabel: 'Annuler et supprimer',
                danger: true,
                onConfirm: async () => {
                    try {
                        await cancelJob(batchId);
                    } catch { /* ignore cancel errors — proceed to delete */ }
                    const result = await deleteJob(batchId);
                    if (result && result._ok !== false) {
                        showToast(`Batch « ${batchName} » supprimé.`, 'success');
                        await renderMonitorList(container);
                    } else {
                        showToast(explainError(result), 'error');
                    }
                },
            });
        } else {
            showConfirmModal({
                title: 'Supprimer ce batch ?',
                body: `
                    <p><strong>Batch :</strong> ${escapeHtml(batchName)}</p>
                    <p style="color:var(--danger)">Les entités MAPS orphelines et leurs données associées seront supprimées.</p>
                `,
                confirmLabel: 'Supprimer',
                danger: true,
                onConfirm: async () => {
                    const result = await deleteJob(batchId);
                    if (result && result._ok !== false) {
                        showToast(`Batch « ${batchName} » supprimé.`, 'success');
                        await renderMonitorList(container);
                    } else {
                        showToast(explainError(result), 'error');
                    }
                },
            });
        }
    }, { capture: true, signal: _monitorListAbort.signal });
}

async function renderJobMonitor(container, batchId) {
    // Role detection for conditional rendering
    const user = getCachedUser();
    const isAdmin = user?.role === 'admin';

    // ── Render skeleton ONCE ─────────────────────────────────────
    container.innerHTML = `
        <div id="mon-breadcrumb"></div>

        <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:var(--space-xl); margin-bottom:var(--space-2xl)">
            <div>
                <h1 class="page-title" id="mon-title">${t('monitor.loading')}</h1>
                <div style="display:flex; align-items:center; gap:var(--space-md); margin-top:var(--space-sm)" id="mon-status-row"></div>
                <div id="mon-current-query" style="margin-top:6px; font-size:13px; color:var(--accent, #4A90D9); display:none"></div>
                <div id="mon-queue-info" style="display:none"></div>
            </div>
            <div style="display:flex; align-items:center; gap:var(--space-sm)">
                <button id="mon-cancel-btn" class="btn btn-secondary" style="color:var(--danger); display:none" title="${t('monitor.stopBtnTitle')}">${t('monitor.stopBtn')}</button>
                <a href="#/job/${encodeURIComponent(batchId)}" class="btn btn-secondary">${t('monitor.fullDetail')}</a>
            </div>
        </div>

        <!-- Poll warning (hidden by default) -->
        <div id="mon-poll-warning" style="display:none; margin-bottom:var(--space-lg)"></div>

        <!-- Progress Ring + Metrics -->
        <div class="card" style="margin-bottom:var(--space-xl)">
            <div style="display:flex; align-items:center; gap:var(--space-2xl); flex-wrap:wrap">
                <!-- Progress Ring -->
                <div style="flex-shrink:0" id="mon-ring">
                    ${renderProgressRing(null, 140, 6, t('monitor.ringLabelQualRate'))}
                </div>

                <!-- Metric Cards -->
                <div class="monitor-metrics" style="flex:1; min-width:280px">
                        <div class="monitor-metric">
                            <div class="monitor-metric-value" id="mon-scraped">0</div>
                            <div class="monitor-metric-label">${t('monitor.metricCompleted')}</div>
                        </div>
                        <div class="monitor-metric">
                            <div class="monitor-metric-value" id="mon-failed">0</div>
                            <div class="monitor-metric-label">${t('monitor.metricFailed')}</div>
                        </div>
                        <div class="monitor-metric">
                            <div class="monitor-metric-value" id="mon-duration">0m 0s</div>
                            <div class="monitor-metric-label">${t('monitor.metricDuration')}</div>
                        </div>
                        ${isAdmin ? `
                        <div class="monitor-metric">
                            <div class="monitor-metric-value" id="mon-replaced">0</div>
                            <div class="monitor-metric-label">${t('monitor.metricSubstitutions')}</div>
                        </div>
                        ` : '<div id="mon-replaced" style="display:none">0</div>'}
                </div>
            </div>
        </div>

        <!-- Pipeline Stage + Wave (admin only) -->
        ${isAdmin ? `
        <div style="display:flex; align-items:center; justify-content:space-between; gap:var(--space-lg); margin-bottom:var(--space-xl); flex-wrap:wrap">
            <div id="mon-pipeline">${renderPipelineStages(null)}</div>
            <div id="mon-wave"></div>
        </div>

        ` : `
        <div id="mon-pipeline" style="display:none"></div>
        <div id="mon-wave" style="display:none"></div>
        `}

        <!-- Quality Gauges -->
        <div class="card" style="margin-bottom:var(--space-xl)">
            <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                ${t('monitor.dataQuality')}
            </h3>
            <div style="display:flex; gap:var(--space-2xl); justify-content:center" id="mon-gauges">—</div>
        </div>

        <!-- Batch Summary + Queries Panel removed (Apr 28) — no useful data
             during a running batch; available on the job detail page (#/job/:id). -->

        <!-- Completion CTA (hidden by default) -->
        <div id="mon-completion" style="display:none; margin-bottom:var(--space-xl)"></div>

        <div style="font-size:var(--font-xs); color:var(--text-muted); text-align:center" id="mon-footer">
            ${t('monitor.autoRefresh')}
        </div>

        <!-- Live Company Cards -->
        <div style="margin-top:var(--space-2xl)">
            <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)" id="mon-cards-title">
                ${t('monitor.collectingTitle', { count: 0 })}
            </h3>
            <div class="company-grid" id="mon-cards">
                <div style="text-align:center; color:var(--text-muted); font-style:italic; padding:var(--space-lg); grid-column:1/-1">
                    ${t('monitor.waitingData')}
                </div>
            </div>
        </div>
    `;

    // ── Cache DOM references (perf: no re-query per poll) ────────
    const $ = {
        title: document.getElementById('mon-title'),
        statusRow: document.getElementById('mon-status-row'),
        ring: document.getElementById('mon-ring'),
        scraped: document.getElementById('mon-scraped'),
        failed: document.getElementById('mon-failed'),
        duration: document.getElementById('mon-duration'),
        replaced: document.getElementById('mon-replaced'),
        pipeline: document.getElementById('mon-pipeline'),
        wave: document.getElementById('mon-wave'),
        gauges: document.getElementById('mon-gauges'),
        footer: document.getElementById('mon-footer'),
        cardsTitle: document.getElementById('mon-cards-title'),
        cards: document.getElementById('mon-cards'),
        breadcrumb: document.getElementById('mon-breadcrumb'),
        pollWarning: document.getElementById('mon-poll-warning'),
        completion: document.getElementById('mon-completion'),
        cancelBtn: document.getElementById('mon-cancel-btn'),
        summary: document.getElementById('mon-summary'),
        currentQuery: document.getElementById('mon-current-query'),
        queueInfo: document.getElementById('mon-queue-info'),
        queriesCard: document.getElementById('mon-queries-card'),
        queriesList: document.getElementById('mon-queries-list'),
    };

    // ── State tracking ──────────────────────────────────────────
    const renderedSirens = new Set();
    let lastScrapedCount = 0;
    let failedPolls = 0;
    let previousValues = { scraped: -1, failed: -1, replaced: -1, pct: -1 };

    // Durée ticker state — freezes when batch ends (completed/failed/interrupted/cancelled)
    let jobStartMs = null;           // set from job.created_at on first successful poll
    let jobEndMs = null;             // set from job.updated_at when batch reaches terminal state
    let durationTickerInterval = null;

    function formatDurationMs(ms) {
        if (!ms || ms < 0) ms = 0;
        const totalSec = Math.floor(ms / 1000);
        const h = Math.floor(totalSec / 3600);
        const m = Math.floor((totalSec % 3600) / 60);
        const s = totalSec % 60;
        if (h > 0) return `${h}h ${m}m ${s}s`;
        return `${m}m ${s}s`;
    }

    function tickDuration() {
        if (!$.duration) return;
        if (!jobStartMs) {
            $.duration.textContent = '0m 0s';
            return;
        }
        const endMs = jobEndMs || Date.now();
        $.duration.textContent = formatDurationMs(endMs - jobStartMs);
    }

    // ── Cancel button handler ───────────────────────────────────────────
    if ($.cancelBtn) {
        $.cancelBtn.addEventListener('click', () => {
            const scraped = previousValues.scraped > 0 ? previousValues.scraped : 0;
            showConfirmModal({
                title: t('monitor.stopConfirmTitle'),
                body: `
                    <p><strong>${t('monitor.stopConfirmBatch')}</strong> ${escapeHtml($.title.textContent)}</p>
                    <p><strong>${t('monitor.stopConfirmCollected')}</strong> ${scraped} ${t('monitor.companies')}</p>
                    <p style="color:var(--success)">${t('monitor.stopConfirmKept', { count: scraped })}</p>
                    <p style="color:var(--warning)">${t('monitor.stopConfirmStopsSearching')}</p>
                `,
                confirmLabel: t('monitor.stopConfirmBtn'),
                danger: true,
                onConfirm: async () => {
                    const result = await cancelJob(batchId);
                    if (result._ok !== false) {
                        showToast(t('monitor.stopSuccess'), 'success');
                    } else {
                        showToast(t('monitor.stopError'), 'error');
                    }
                },
            });
        });
    }

    // ── Smart update function — patches by ID, no full rebuild ───
    async function update() {
        // Guard: if user navigated away, stop polling
        const currentHash = window.location.hash || '#/';
        if (!currentHash.includes('/monitor/')) {
            if (pollInterval) { clearInterval(pollInterval); pollInterval = null; }
            return;
        }

        // Guard: container was replaced by another page
        if (!$.title) return;

        let job;
        try {
            job = await getJob(batchId);
            // Successful poll — clear failure state
            if (failedPolls > 0) {
                failedPolls = 0;
                $.pollWarning.style.display = 'none';
            }
        } catch {
            // Polling failure resilience
            failedPolls++;
            if (failedPolls === 1) {
                $.pollWarning.style.display = 'block';
                $.pollWarning.innerHTML = `<div class="poll-warning">${t('monitor.connectionInterrupted')}</div>`;
            } else if (failedPolls >= 2) {
                $.pollWarning.innerHTML = `<div class="poll-warning">${t('monitor.connectionLost')}</div>`;
            }
            return; // Keep last valid state
        }
        if (!job || job.error) return;

        // ── Duration tracking ──────────────────────────────────
        // Initial render shows "0m 0s" from the template. First poll (~1.5s in)
        // sets jobStartMs here, then calls tickDuration() SYNCHRONOUSLY so the
        // Durée value is correct immediately — before the 1s setInterval ticker
        // below would otherwise kick in. Prevents the stale "0m 0s" flash during
        // the first poll cycle.
        if (job.created_at && jobStartMs === null) {
            jobStartMs = new Date(job.created_at).getTime();
        }
        const terminalStatuses = new Set(['completed', 'failed', 'interrupted', 'cancelled']);
        if (terminalStatuses.has(job.status)) {
            jobEndMs = job.updated_at ? new Date(job.updated_at).getTime() : Date.now();
        } else {
            jobEndMs = null;
        }
        tickDuration();

        const scraped = job.companies_scraped || 0;
        const qualified = job.companies_qualified || 0;
        const failed = job.companies_failed || 0;
        const replaced = job.replaced_count || 0;
        // Ring now shows qualification rate: qualified / scraped (of Maps results
        // evaluated, how many passed the quality filter). Divide-by-zero guarded
        // by null pct when scraped == 0 (Maps still searching).
        const qualRate = scraped > 0 ? Math.round((qualified / scraped) * 100) : null;
        const isRunning = job.status === 'in_progress' || job.status === 'queued' || job.status === 'triage';
        const isUpload = job.mode === 'upload';

        // ── Breadcrumb + Title ──────────────────────────────────
        $.breadcrumb.innerHTML = breadcrumb([
            { label: 'Pipeline Live', href: '#/monitor' },
            { label: job.batch_name },
        ]);
        $.title.textContent = job.batch_name || t('monitor.batchInProgress');
        $.statusRow.innerHTML = `
            ${isRunning ? '' : statusBadge(job.status || 'queued')}
            ${isRunning ? `<span class="live-badge"><span class="live-badge-dot"></span> ${t('monitor.liveLabel')}</span>` : ''}
        `;

        // Show/hide cancel button
        if ($.cancelBtn) {
            $.cancelBtn.style.display = isRunning ? '' : 'none';
        }

        // ── Current Query Indicator ─────────────────────────────
        if ($.currentQuery) {
            if (isRunning && job.current_query) {
                const waveText = (job.wave_current && job.wave_total)
                    ? ` (${job.wave_current}/${job.wave_total})`
                    : '';
                $.currentQuery.style.display = 'block';
                $.currentQuery.textContent = `Recherche : ${job.current_query}${waveText}...`;
            } else {
                $.currentQuery.style.display = 'none';
            }
        }

        // ── Queue info banner ──
        const queueBanner = $.queueInfo;
        if (queueBanner) {
            if (job.status === 'queued' && job.queue_info) {
                const qi = job.queue_info;
                let html = '';

                if (qi.blocking_batch) {
                    const b = qi.blocking_batch;
                    html += `<div style="padding:12px 16px; background:var(--bg-secondary); border-left:3px solid var(--warning, #f0ad4e); border-radius:var(--radius-sm); margin-top:var(--space-md)">`;
                    html += `<div style="font-weight:600; color:var(--warning, #f0ad4e); font-size:var(--font-sm)">`;
                    html += `⏳ ${t('monitor.queueWaiting')}`;
                    html += `</div>`;
                    html += `<div style="font-size:var(--font-sm); margin-top:4px; color:var(--text-secondary)">`;
                    if (b.exhaustive_default) {
                        // For Apr-17+ batches, target is a ceiling not a goal.
                        // Show raw progress. "entité" singular for count=1, "entités" otherwise.
                        const unit = b.progress === 1 ? 'entité' : 'entités';
                        html += `${escapeHtml(b.batch_name)} — ${b.progress} ${unit}`;
                    } else {
                        html += `${escapeHtml(b.batch_name)} — ${b.progress}/${b.target}`;
                    }
                    if (b.current_query) html += ` (${escapeHtml(b.current_query)})`;
                    html += `</div>`;
                    if (qi.estimated_wait_minutes != null) {
                        html += `<div style="font-size:var(--font-xs); margin-top:4px; color:var(--text-muted)">`;
                        html += `${t('monitor.queueEstimate', {minutes: qi.estimated_wait_minutes})}`;
                        html += `</div>`;
                    }
                    html += `</div>`;
                } else {
                    html += `<div style="padding:12px 16px; background:var(--bg-secondary); border-left:3px solid var(--warning, #f0ad4e); border-radius:var(--radius-sm); margin-top:var(--space-md)">`;
                    html += `<div style="font-weight:600; color:var(--warning, #f0ad4e); font-size:var(--font-sm)">`;
                    html += `⏳ ${t('monitor.queueWaitingGeneric')}`;
                    html += `</div></div>`;
                }

                queueBanner.innerHTML = html;
                queueBanner.style.display = 'block';
            } else {
                queueBanner.style.display = 'none';
                queueBanner.innerHTML = '';
            }
        }

        // ── Progress Ring — only update if qualRate changed ─────
        if (qualRate !== previousValues.pct) {
            previousValues.pct = qualRate;
            const ringCircle = document.getElementById('progress-ring-circle');
            const ringPct = document.getElementById('progress-ring-pct');
            const ringLabel = document.querySelector('.progress-ring-label');
            if (ringCircle && ringPct) {
                const r = (140 - 6) / 2; // match renderProgressRing defaults
                const circumference = 2 * Math.PI * r;
                if (qualRate === null) {
                    // scraped == 0 — empty arc, em-dash value
                    ringCircle.setAttribute('stroke-dashoffset', circumference);
                    ringPct.textContent = '—';
                } else {
                    const offset = circumference - (qualRate / 100) * circumference;
                    ringCircle.setAttribute('stroke-dashoffset', offset);
                    ringPct.textContent = qualRate + '%';
                }
            }
            // Label swap: when batch is terminal + scraped=0, show "Batch sans
            // résultat" instead of the generic "Taux de qualification" (reads
            // awkwardly on a finished batch with nothing found). On running batches
            // the label stays "Taux de qualification".
            if (ringLabel) {
                const isTerminalNoResult = terminalStatuses.has(job.status) && scraped === 0;
                ringLabel.textContent = isTerminalNoResult
                    ? t('monitor.ringLabelNoResult')
                    : t('monitor.ringLabelQualRate');
            }
        }

        // ── Metric Counters — animate only on change ────────────
        const completedValue = qualified > 0 ? qualified : (job.triage_green || 0);
        const completedLabel = qualified > 0 ? t('monitor.metricCompleted') : (job.triage_green || 0) > 0 ? t('monitor.metricKnown') : t('monitor.metricCompleted');
        if (completedValue !== previousValues.scraped) {
            previousValues.scraped = completedValue;
            animateCounter($.scraped, completedValue);
            // Update the label below the counter
            const scrapedLabel = $.scraped?.nextElementSibling;
            if (scrapedLabel && scrapedLabel.classList.contains('monitor-metric-label')) {
                scrapedLabel.textContent = completedLabel;
            }
        }
        if (failed !== previousValues.failed) {
            previousValues.failed = failed;
            animateCounter($.failed, failed);
        }
        if (replaced !== previousValues.replaced) {
            previousValues.replaced = replaced;
            animateCounter($.replaced, replaced);
        }

        // ── Pipeline Stage (hide for uploads) ──────────────────
        // 6 stages: maps → triage → web → match → inpi → save
        // Detection uses coarse heuristics (scraped/qualified counts);
        // per-step real-time activity is a follow-up brief (Option B/C).
        if (isUpload) {
            $.pipeline.style.display = 'none';
            if ($.pipeline.parentElement) $.pipeline.parentElement.style.display = 'none';
        } else {
            let stage = null;
            if (isRunning) {
                if (job.status === 'triage') stage = null;
                else if (qualified > 0) stage = 'inpi';            // mid-pipeline (INPI is the slow representative)
                else if (scraped > 0) stage = 'triage';            // past Maps, classifying/processing
                else stage = 'maps';                                // still searching Maps
            } else if (job.status === 'completed' || job.status === 'interrupted') {
                stage = 'save';
            }
            $.pipeline.innerHTML = renderPipelineStages(stage);
        }

        // ── Wave Chip ───────────────────────────────────────────
        const waveCurrent = job.wave_current || 0;
        const waveTotal = job.wave_total || 0;
        const wavePct = waveTotal > 0 ? Math.round((waveCurrent / waveTotal) * 100) : 0;
        $.wave.innerHTML = waveTotal > 0
            ? `<div class="wave-chip">
                    <div class="wave-chip-fill" style="width:${wavePct}%"></div>
                    <span class="wave-chip-text">${t('monitor.wavechip', { current: waveCurrent, total: waveTotal })}</span>
               </div>`
            : '';


        // ── Quality Gauges ──────────────────────────────────────
        try {
            const q = await getJobQuality(batchId) || {};
            $.gauges.innerHTML = `
                ${renderGauge(q.phone_pct || 0, t('monitor.gaugePhone'))}
                ${renderGauge(q.email_pct || 0, t('monitor.gaugeEmail'))}
                ${renderGauge(q.website_pct || 0, t('monitor.gaugeWeb'))}
                ${renderGauge(q.officers_pct || 0, '👤 Dirigeants')}
                ${renderGauge(q.financials_pct || 0, '💰 Financier')}
                ${renderGauge(q.siret_pct || q.social_pct || 0, '🔗 Social')}
            `;
        } catch { /* gauges are optional */ }

        // ── Info Banner for running all-GREEN batches ───────────
        // When a batch is still running but all entities so far were GREEN (already
        // enriched), explain to the user why "Complétées" stays at 0 — or why it
        // shows the green count instead. This only shows during RUNNING state.
        const triageGreen = job.triage_green || 0;
        const infoBannerId = 'mon-green-info-banner';
        let infoBanner = document.getElementById(infoBannerId);
        if (isRunning && triageGreen > 0 && qualified === 0) {
            if (!infoBanner) {
                infoBanner = document.createElement('div');
                infoBanner.id = infoBannerId;
                infoBanner.style.cssText = 'background:color-mix(in srgb, var(--bg-surface) 90%, var(--success)); border-left:4px solid var(--success); border-radius:var(--radius-md); padding:var(--space-md) var(--space-lg); margin-bottom:var(--space-xl); font-size:var(--font-sm); color:var(--text-secondary);';
                infoBanner.innerHTML = t('monitor.allEnrichedRunning', { count: triageGreen, plural: triageGreen > 1 ? 'ies' : 'y' });
                $.completion.parentElement.insertBefore(infoBanner, $.completion);
            } else {
                infoBanner.innerHTML = t('monitor.allEnrichedRunning', { count: triageGreen, plural: triageGreen > 1 ? 'ies' : 'y' });
            }
            infoBanner.style.display = 'block';
        } else if (infoBanner) {
            infoBanner.style.display = 'none';
        }

        // ── Completion State ────────────────────────────────────
        if (!isRunning && job.status === 'completed' && qualified === 0 && (job.triage_green || 0) > 0) {
            $.completion.style.display = 'block';
            $.completion.innerHTML = `
                <div class="completion-card" style="border-left: 4px solid var(--success); background: color-mix(in srgb, var(--bg-surface) 90%, var(--success));">
                    <div class="completion-icon" style="background:rgba(16,185,129,0.1); color:var(--success)">✅</div>
                    <div class="completion-title" style="color:var(--success)">${t('monitor.completionAllKnown')}</div>
                    <div class="completion-subtitle">${t('monitor.completionAllKnownSub', { count: scraped })}</div>
                    <a href="#/job/${encodeURIComponent(batchId)}" class="btn" style="border:1px solid var(--success); color:var(--text)">${t('monitor.completionSeeResults')}</a>
                </div>
            `;
        } else if (!isRunning && job.status === 'completed') {
            $.completion.style.display = 'block';
            $.completion.innerHTML = `
                <div class="completion-card">
                    <div class="completion-icon">${qualified > 0 ? '🎉' : '⚠️'}</div>
                    <div class="completion-title">${qualified > 0 ? t('monitor.completionDone') : t('monitor.completionNoResults')}</div>
                    <div class="completion-subtitle">${t('monitor.completionQualified', { qualified, scraped })}</div>
                    <a href="#/job/${encodeURIComponent(batchId)}" class="btn btn-primary">${t('monitor.completionSeeResults')}</a>
                </div>
            `;
        } else if (!isRunning && job.status === 'failed') {
             $.completion.style.display = 'block';
             $.completion.innerHTML = `
                 <div class="completion-card" style="border-left: 4px solid var(--danger); background: color-mix(in srgb, var(--bg-surface) 90%, var(--danger));">
                     <div class="completion-icon" style="background:var(--danger-bg); color:var(--danger)">⚠️</div>
                     <div class="completion-title" style="color:var(--danger)">${t('monitor.completionInterrupted')}</div>
                     <div class="completion-subtitle">${t('monitor.completionInterruptedSub', { qualified })}</div>
                     <a href="#/job/${encodeURIComponent(batchId)}" class="btn" style="border:1px solid var(--danger); color:var(--text)">${t('monitor.completionPartialResults')}</a>
                 </div>
             `;
        } else if (!isRunning && job.status === 'interrupted') {
            $.completion.style.display = 'block';
            $.completion.innerHTML = `
                <div class="completion-card" style="border-left: 4px solid #F97316; background: color-mix(in srgb, var(--bg-surface) 90%, #F97316);">
                    <div class="completion-icon" style="background:rgba(249,115,22,0.1); color:#F97316">⚠️</div>
                    <div class="completion-title" style="color:#F97316">Batch interrompu</div>
                    <div class="completion-subtitle">${job.shortfall_reason || 'Le processus s\'est arrêté de manière inattendue.'}</div>
                    <a href="#/job/${encodeURIComponent(batchId)}" class="btn" style="border:1px solid #F97316; color:var(--text)">Voir les résultats partiels</a>
                </div>
            `;
        }

        // ── Batch Summary ───────────────────────────────────────
        if ($.summary) {
            $.summary.innerHTML = renderSummary(job);
        }

        // ── Queries Panel (TOP 3 widening) ──────────────────────
        if ($.queriesList) {
            try {
                const queriesResp = await fetch(`/api/jobs/${encodeURIComponent(batchId)}/queries`, { credentials: 'include' });
                if (queriesResp.ok) {
                    const queriesData = await queriesResp.json();
                    $.queriesList.innerHTML = renderQueriesPanel(queriesData.queries || []);
                }
            } catch (_qe) {
                // best-effort — don't break the rest of polling on error
            }
        }

        // ── Footer ──────────────────────────────────────────────
        let footerText = t('monitor.footerAuto', { datetime: formatDateTime(job.updated_at) });
        if (isRunning && job.updated_at) {
            const idleSec = (Date.now() - new Date(job.updated_at).getTime()) / 1000;
            if (idleSec > 30) {
                const minutes = Math.floor(idleSec / 60);
                footerText = minutes > 0
                    ? t('monitor.footerAutoStale', { datetime: formatDateTime(job.updated_at), minutes })
                    : t('monitor.footerAutoStaleRecent', { datetime: formatDateTime(job.updated_at) });
            }
        } else if (!isRunning) {
            footerText = (job.status === 'failed' || job.status === 'interrupted')
                ? t('monitor.footerFailed', { datetime: formatDateTime(job.updated_at) })
                : t('monitor.footerDone', { datetime: formatDateTime(job.updated_at) });
        }
        $.footer.textContent = footerText;

        // ── Company Cards — append-only (track rendered SIRENs) ──
        $.cardsTitle.textContent = t('monitor.collectingTitle', { count: qualified });

        if (qualified > 0 && qualified !== lastScrapedCount) {
            lastScrapedCount = qualified;
            try {
                const cardData = await getJobCompanies(batchId, { page: 1, pageSize: 50, sort: 'completude' });
                if (cardData && cardData.companies && cardData.companies.length > 0) {
                    // Find genuinely new companies
                    const newCompanies = cardData.companies.filter(c => !renderedSirens.has(c.siren));

                    if (newCompanies.length > 0) {
                        // Remove placeholder if present
                        const placeholder = $.cards.querySelector('[style*="grid-column"]');
                        if (placeholder) placeholder.remove();

                        // Append new cards via DocumentFragment
                        const fragment = document.createDocumentFragment();
                        newCompanies.forEach(c => {
                            renderedSirens.add(c.siren);
                            const wrapper = document.createElement('div');
                            wrapper.className = 'monitor-card-enter';
                            wrapper.innerHTML = companyCard(c);
                            // Move the inner card out of the wrapper
                            const card = wrapper.firstElementChild;
                            if (card) {
                                card.classList.add('monitor-card-enter');
                                fragment.prepend(card);
                            }
                        });
                        $.cards.prepend(fragment);
                    }
                }
            } catch { /* cards are optional display */ }
        } else if (scraped === 0 && qualified === 0) {
            $.cards.innerHTML = `
                <div style="text-align:center; color:var(--text-muted); font-style:italic; padding:var(--space-lg); grid-column:1/-1">
                    ${isRunning ? t('monitor.waitingData') : t('monitor.noCompaniesCollected')}
                </div>
            `;
        } else if (!isRunning && qualified === 0 && scraped > 0) {
            $.cards.innerHTML = `
                <div style="text-align:center; color:var(--success); padding:var(--space-lg); grid-column:1/-1">
                    ${t('monitor.allEnrichedDone', { count: scraped })}
                </div>
            `;
        } else if (!isRunning && qualified > 0 && scraped === qualified) {
            $.cards.innerHTML = `
                <div style="text-align:center; color:var(--success); padding:var(--space-lg); grid-column:1/-1">
                    ${t('monitor.allEnrichedDone', { count: qualified })}
                </div>
            `;
        }

        // ── Stop polling if job is done ──────────────────────────
        if (!isRunning && pollInterval) {
            clearInterval(pollInterval);
            pollInterval = null;
        }
    }

    // ── Initial render ──────────────────────────────────────────
    await update();

    // ── Auto-poll every 1.5s ────────────────────────────────────
    pollInterval = setInterval(update, 1500);

    // Durée ticker — updates every 1s while running (separate from poll so it
    // feels alive between 1.5s polls). The tick function is a no-op when frozen.
    durationTickerInterval = setInterval(tickDuration, 1000);

    // ── Register cleanup ────────────────────────────────────────
    registerCleanup(() => {
        if (pollInterval) {
            clearInterval(pollInterval);
            pollInterval = null;
        }
        if (durationTickerInterval) {
            clearInterval(durationTickerInterval);
            durationTickerInterval = null;
        }
    });
}
