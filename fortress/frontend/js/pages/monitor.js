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
import { renderQueriesPanel, bindQueriesPanelClicks } from '../components/queries_panel.js';

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
    const canDelete = !!user;

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
                                ${canDelete ? `<button class="btn-delete-job" data-batch-id="${escapeHtml(j.batch_id)}" data-batch-name="${escapeHtml(j.batch_name)}" data-workspace-id="${j.workspace_id == null ? '' : j.workspace_id}" data-running="true" title="Supprimer ce batch" style="background:none;border:none;cursor:pointer;color:var(--text-muted);font-size:1.1em;padding:4px 6px;border-radius:4px;line-height:1;flex-shrink:0" onmouseover="this.style.color='var(--danger)'" onmouseout="this.style.color='var(--text-muted)'">×</button>` : ''}
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
                ${jobsList.filter(j => (j.status === 'completed' || j.status === 'interrupted') && !j.batch_id.startsWith('MANUAL_')).slice(0, 20).map(j => `
                    <div class="job-card" onclick="window.location.hash='#/job/${encodeURIComponent(j.batch_id)}'">
                        <div class="job-card-info">
                            <div class="job-card-name">${escapeHtml(j.batch_name)}</div>
                            <div class="job-card-meta">
                                <span>${formatDateTime(j.updated_at)}</span>
                                <span>${j.batch_unique_companies || 0} ${t('monitor.companies')}</span>
                                ${(j.exhaustive && !j.exhaustive_default) ? `<span class="chip-exhaustive">${t('monitor.exhaustive')}</span>` : ''}
                            </div>
                        </div>
                        <div style="display:flex;align-items:center;gap:var(--space-sm)">
                            ${statusBadge(j.status)}
                            ${canDelete ? `<button class="btn-delete-job" data-batch-id="${escapeHtml(j.batch_id)}" data-batch-name="${escapeHtml(j.batch_name)}" data-workspace-id="${j.workspace_id == null ? '' : j.workspace_id}" data-running="false" title="Supprimer ce batch" style="background:none;border:none;cursor:pointer;color:var(--text-muted);font-size:1.1em;padding:4px 6px;border-radius:4px;line-height:1;flex-shrink:0" onmouseover="this.style.color='var(--danger)'" onmouseout="this.style.color='var(--text-muted)'">×</button>` : ''}
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
        const wsAttr = btn.dataset.workspaceId;
        const targetWsId = (wsAttr === '' || wsAttr === undefined) ? null : Number(wsAttr);
        const currentUser = getCachedUser();
        const isCrossWorkspace = (
            currentUser?.role === 'admin'
            && targetWsId != null
            && targetWsId !== currentUser.workspace_id  // admin.workspace_id is null
        );

        // Surface the real API failure reason in the toast instead of a
        // generic "Erreur lors de la suppression". The previous text hid
        // 401/404/409 paths so users couldn't tell why delete failed.
        const explainError = (result) => {
            if (!result) return 'Erreur lors de la suppression.';
            if (result._status === 401) return 'Session expirée — reconnectez-vous.';
            if (result._status === 403) return 'Accès refusé.';
            if (result._status === 404) return 'Batch introuvable (déjà supprimé ?).';
            if (result._status === 409) return result.error || 'Arrêtez le batch en cours d\'abord.';
            if (result._status === 422) return result.error || 'Confirmation requise.';
            return result.error || 'Erreur lors de la suppression.';
        };

        if (isRunning) {
            showConfirmModal({
                title: isCrossWorkspace ? t('monitor.crossWorkspaceDelete.title') : 'Supprimer ce batch en cours ?',
                body: isCrossWorkspace
                    ? `<p>${t('monitor.crossWorkspaceDelete.prompt', { name: escapeHtml(batchName) })}</p>
                       <p style="color:var(--warning)">Ce batch est encore en cours. Il sera d'abord annulé, puis supprimé.</p>`
                    : `
                    <p><strong>Batch :</strong> ${escapeHtml(batchName)}</p>
                    <p style="color:var(--warning)">Ce batch est encore en cours. Il sera d'abord annulé, puis supprimé.</p>
                    <p style="color:var(--danger)">Toutes les données collectées seront supprimées.</p>
                `,
                confirmLabel: isCrossWorkspace ? t('monitor.crossWorkspaceDelete.submitLabel') : 'Annuler et supprimer',
                danger: true,
                requiredText: isCrossWorkspace ? batchName : null,
                onConfirm: async () => {
                    try {
                        if (isCrossWorkspace) {
                            await cancelJob(batchId, { confirmName: batchName });
                        } else {
                            await cancelJob(batchId);
                        }
                    } catch { /* ignore cancel errors — proceed to delete */ }
                    const result = isCrossWorkspace
                        ? await deleteJob(batchId, { confirmName: batchName })
                        : await deleteJob(batchId);
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
                title: isCrossWorkspace ? t('monitor.crossWorkspaceDelete.title') : 'Supprimer ce batch ?',
                body: isCrossWorkspace
                    ? `<p>${t('monitor.crossWorkspaceDelete.prompt', { name: escapeHtml(batchName) })}</p>`
                    : `
                    <p><strong>Batch :</strong> ${escapeHtml(batchName)}</p>
                    <p style="color:var(--danger)">Les entités MAPS orphelines et leurs données associées seront supprimées.</p>
                `,
                confirmLabel: isCrossWorkspace ? t('monitor.crossWorkspaceDelete.submitLabel') : 'Supprimer',
                danger: true,
                requiredText: isCrossWorkspace ? batchName : null,
                onConfirm: async () => {
                    const result = isCrossWorkspace
                        ? await deleteJob(batchId, { confirmName: batchName })
                        : await deleteJob(batchId);
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
                <button id="mon-delete-btn" class="btn-delete-job" data-batch-id="${escapeHtml(batchId)}" title="Supprimer ce batch" style="background:none;border:none;cursor:pointer;color:var(--text-muted);font-size:1.4em;padding:4px 8px;border-radius:4px;line-height:1" onmouseover="this.style.color='var(--danger)'" onmouseout="this.style.color='var(--text-muted)'">×</button>
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
                            <div class="monitor-metric-label" id="mon-scraped-label">${t('monitor.metricConfirmed')}</div>
                            <div class="monitor-metric-subtext" id="mon-scraped-scope" style="font-size:11px; color:var(--text-muted); margin-top:2px"></div>
                        </div>
                        <div class="monitor-metric">
                            <div class="monitor-metric-value" id="mon-duration">0m 0s</div>
                            <div class="monitor-metric-label">${t('monitor.metricTotalTime')}</div>
                            <div class="monitor-metric-subtext" style="font-size:11px; color:var(--text-muted); margin-top:2px">${t('monitor.metricTotalTimeSubtext')}</div>
                        </div>
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

        <!-- Queries Panel — foldable card; collapsed by default per Alan 2026-05-10 (you don't
             need to see the full query list every time you watch a live batch) -->
        <details class="card" id="mon-queries-card" style="margin-bottom:var(--space-xl)">
            <summary style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; cursor:pointer; list-style:revert">
                ${t('monitor.queriesPanel')}
            </summary>
            <div id="mon-queries-list" style="margin-top:var(--space-lg)">
                <span style="color:var(--text-muted); font-style:italic">${t('monitor.loading')}</span>
            </div>
        </details>

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
        duration: document.getElementById('mon-duration'),
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
    let lastScrapedCount = '';
    let failedPolls = 0;
    let previousValues = { scraped: -1, pct: -1 };
    let isRunning = false;
    let jobWorkspaceId = null;

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
            const titleNow = ($.title?.textContent || batchId).trim();
            const currentUser = getCachedUser();
            const isCrossWorkspace = (
                currentUser?.role === 'admin'
                && jobWorkspaceId != null
                && jobWorkspaceId !== currentUser.workspace_id
            );
            const explainError = (result) => {
                if (!result) return t('monitor.stopError');
                if (result._status === 401) return 'Session expirée — reconnectez-vous.';
                if (result._status === 403) return 'Accès refusé.';
                if (result._status === 404) return 'Batch introuvable.';
                if (result._status === 409) return result.error || t('monitor.stopError');
                if (result._status === 422) return result.error || 'Confirmation requise.';
                return result.error || t('monitor.stopError');
            };
            showConfirmModal({
                title: isCrossWorkspace ? t('monitor.crossWorkspaceCancel.title') : t('monitor.stopConfirmTitle'),
                body: isCrossWorkspace
                    ? `<p>${t('monitor.crossWorkspaceCancel.prompt', { name: escapeHtml(titleNow) })}</p>`
                    : `
                        <p><strong>${t('monitor.stopConfirmBatch')}</strong> ${escapeHtml(titleNow)}</p>
                        <p><strong>${t('monitor.stopConfirmCollected')}</strong> ${scraped} ${t('monitor.companies')}</p>
                        <p style="color:var(--success)">${t('monitor.stopConfirmKept', { count: scraped })}</p>
                        <p style="color:var(--warning)">${t('monitor.stopConfirmStopsSearching')}</p>
                    `,
                confirmLabel: isCrossWorkspace ? t('monitor.crossWorkspaceCancel.submitLabel') : t('monitor.stopConfirmBtn'),
                danger: true,
                requiredText: isCrossWorkspace ? titleNow : null,
                onConfirm: async () => {
                    const result = isCrossWorkspace
                        ? await cancelJob(batchId, { confirmName: titleNow })
                        : await cancelJob(batchId);
                    if (result && result._ok !== false) {
                        showToast(t('monitor.stopSuccess'), 'success');
                    } else {
                        showToast(explainError(result), 'error');
                    }
                },
            });
        });
    }

    // ── Delete button handler ────────────────────────────────────────────
    $.deleteBtn = document.getElementById('mon-delete-btn');
    if ($.deleteBtn) {
        $.deleteBtn.addEventListener('click', async () => {
            const titleNow = ($.title?.textContent || batchId).trim();
            // Read latest poll value for cross-workspace determination
            const currentUser = getCachedUser();
            const isCrossWorkspace = (
                currentUser?.role === 'admin'
                && jobWorkspaceId != null
                && jobWorkspaceId !== currentUser.workspace_id
            );
            const explainError = (result) => {
                if (!result) return 'Erreur lors de la suppression.';
                if (result._status === 401) return 'Session expirée — reconnectez-vous.';
                if (result._status === 403) return 'Accès refusé.';
                if (result._status === 404) return 'Batch introuvable (déjà supprimé ?).';
                if (result._status === 409) return result.error || 'Arrêtez le batch en cours d\'abord.';
                if (result._status === 422) return result.error || 'Confirmation requise.';
                return result.error || 'Erreur lors de la suppression.';
            };
            // Promise-mode: requiredText short-circuit prevents premature resolve
            const confirmed = await showConfirmModal({
                title: isCrossWorkspace ? t('monitor.crossWorkspaceDelete.title') : (isRunning ? 'Annuler et supprimer' : 'Supprimer ce batch'),
                body: isCrossWorkspace
                    ? `<p>${t('monitor.crossWorkspaceDelete.prompt', { name: escapeHtml(titleNow) })}</p>`
                    : (isRunning
                        ? `Le batch « ${escapeHtml(titleNow)} » est en cours. Il sera annulé puis supprimé. Cette action est irréversible.`
                        : `Supprimer définitivement le batch « ${escapeHtml(titleNow)} » ? Cette action est irréversible.`),
                confirmLabel: isCrossWorkspace ? t('monitor.crossWorkspaceDelete.submitLabel') : undefined,
                danger: true,
                requiredText: isCrossWorkspace ? titleNow : null,
            });
            if (!confirmed) return;
            try {
                if (isRunning) {
                    try {
                        if (isCrossWorkspace) {
                            await cancelJob(batchId, { confirmName: titleNow });
                        } else {
                            await cancelJob(batchId);
                        }
                    } catch { /* swallow — proceed to delete */ }
                }
                const result = isCrossWorkspace
                    ? await deleteJob(batchId, { confirmName: titleNow })
                    : await deleteJob(batchId);
                if (result && result._ok !== false) {
                    showToast(`Batch « ${titleNow} » supprimé.`, 'success');
                    window.location.hash = '#/monitor';
                } else {
                    showToast(explainError(result), 'error');
                }
            } catch (err) {
                showToast('Erreur lors de la suppression du batch.', 'error');
            }
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
        // Change 3+4: Primary tile now shows confirmed entities (exportable).
        // Same source as Job page tile #22. Replaces the old qualified/strict flip-flop.
        const confirmedValue = job.link_stats?.confirmed || 0;
        // Keep qualified for cards/completion logic (link_stats.total — broad set)
        const qualified = job.link_stats?.total || job.companies_qualified || 0;

        // Change 3: Scope label — explicit per mode/dept combination
        const isStrictMode = Boolean(job.strict_naf);
        const isDeptFiltered = Boolean(job.filters_json?.department && job.filters_json.department !== 'FR' && job.filters_json.department !== 'ALL');
        let scopeLabel = '';
        if (isStrictMode && isDeptFiltered) {
            scopeLabel = t('monitor.scopeStrictDept', { dept: job.filters_json.department });
        } else if (isStrictMode) {
            scopeLabel = t('monitor.scopeStrict');
        } else if (isDeptFiltered) {
            scopeLabel = t('monitor.scopeDept', { dept: job.filters_json.department });
        } else {
            scopeLabel = t('monitor.scopeAllBatch');
        }

        // Change 4: Ring = confirmed / total_discovered; label changes per mode
        // Divide-by-zero guarded by null pct when scraped == 0.
        const qualRate = scraped > 0 ? Math.round((confirmedValue / scraped) * 100) : null;
        isRunning = job.status === 'in_progress' || job.status === 'queued' || job.status === 'triage';
        jobWorkspaceId = job.workspace_id == null ? null : job.workspace_id;
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
            ${Boolean(job.strict_naf) ? `<span class="mode-chip mode-chip--strict" title="${t('monitor.modeStrictTooltip')}">${t('monitor.modeStrictChip')}</span>` : ''}
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
                $.currentQuery.textContent = t('monitor.currentQueryLabel', { query: `${job.current_query}${waveText}` });
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
                        // Show raw progress. Use i18n key for unit.
                        html += `${escapeHtml(b.batch_name)} — ${t('monitor.queueEntityCount', { count: b.progress })}`;
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
            // résultat" instead of the dynamic confirmation-rate label.
            if (ringLabel) {
                const isTerminalNoResult = terminalStatuses.has(job.status) && scraped === 0;
                if (isTerminalNoResult) {
                    ringLabel.textContent = t('monitor.ringLabelNoResult');
                } else {
                    // Dynamic label per scope (Change 4)
                    let ringSuffix = '';
                    if (isStrictMode && isDeptFiltered) {
                        ringSuffix = ` — strict + dép. ${job.filters_json.department}`;
                    } else if (isStrictMode) {
                        ringSuffix = ' — strict';
                    } else if (isDeptFiltered) {
                        ringSuffix = ` — dép. ${job.filters_json.department}`;
                    }
                    ringLabel.textContent = t('monitor.ringLabelDynamic', { suffix: ringSuffix });
                }
            }
        }

        // ── Metric Counters — animate only on change ────────────
        // Change 3: show confirmedValue (exportable, link_confidence=confirmed).
        // Fall back to triage_green display if no confirmed entities yet (all-GREEN batch).
        const completedValue = confirmedValue > 0 ? confirmedValue : (job.triage_green || 0);
        const completedLabel = confirmedValue > 0 ? t('monitor.metricConfirmed') : (job.triage_green || 0) > 0 ? t('monitor.metricKnown') : t('monitor.metricConfirmed');
        if (completedValue !== previousValues.scraped) {
            previousValues.scraped = completedValue;
            animateCounter($.scraped, completedValue);
            // Update the label below the counter
            const scrapedLabel = document.getElementById('mon-scraped-label');
            if (scrapedLabel) {
                scrapedLabel.textContent = completedLabel;
            }
        }
        // Always update scope label (may change as batch progresses)
        const scopeEl = document.getElementById('mon-scraped-scope');
        if (scopeEl) {
            scopeEl.textContent = scopeLabel;
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
                else if ((job.link_stats?.total || 0) > 0) stage = 'inpi'; // mid-pipeline (INPI is the slow representative)
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
                ${renderGauge(q.officers_pct || 0, t('monitor.gaugeOfficers'))}
                ${renderGauge(q.financials_pct || 0, t('monitor.gaugeFinancials'))}
                ${renderGauge(q.social_pct || 0, t('monitor.gaugeSocial'))}
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
            // 2026-05-10: bridge the interrupted message to the metrics shown above.
            // Old message only said "64 / 2000 traitées" without explaining the
            // 35 CONFIRMÉES tile. New message reconciles both numbers in one sentence.
            const _scrapedI = job.companies_scraped || 0;
            const _confirmedI = qualified;  // already mode-aware (link_stats.total in strict, etc.)
            const _targetI = job.batch_size || 0;
            const _bridgeMsg = (_scrapedI > 0 && _confirmedI >= 0)
                ? t('monitor.interruptedBridge', {
                    scraped: _scrapedI,
                    target: _targetI,
                    confirmed: _confirmedI,
                  })
                : (job.shortfall_reason || t('monitor.interruptedSubtext'));
            $.completion.style.display = 'block';
            $.completion.innerHTML = `
                <div class="completion-card" style="border-left: 4px solid #F97316; background: color-mix(in srgb, var(--bg-surface) 90%, #F97316);">
                    <div class="completion-icon" style="background:rgba(249,115,22,0.1); color:#F97316">⚠️</div>
                    <div class="completion-title" style="color:#F97316">${t('monitor.interruptedTitle')}</div>
                    <div class="completion-subtitle">${_bridgeMsg}</div>
                    <a href="#/job/${encodeURIComponent(batchId)}" class="btn" style="border:1px solid #F97316; color:var(--text)">${t('monitor.interruptedCta')}</a>
                </div>
            `;
        }

        // ── Batch Summary ───────────────────────────────────────
        if ($.summary) {
            $.summary.innerHTML = renderSummary(job);
        }

        // ── Queries Panel (live: done + running + queued) ──────────
        if ($.queriesList) {
            try {
                const queriesResp = await fetch(`/api/jobs/${encodeURIComponent(batchId)}/queries`, { credentials: 'include' });
                if (queriesResp.ok) {
                    const queriesData = await queriesResp.json();
                    $.queriesList.innerHTML = renderQueriesPanel(
                        queriesData.queries || [],
                        {
                            collapsible: false,
                            capMin: queriesData.time_cap_min,
                            running: queriesData.running || null,
                            queued: queriesData.queued || [],
                        }
                    );
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

        // Change 7: refresh trigger uses composite key (link_stats.total + companies_scraped)
        // to avoid silent freeze when confirmedValue stops changing mid-batch.
        const _refreshKey = `${job.link_stats?.total || 0}:${scraped}`;
        if (_refreshKey !== lastScrapedCount) {
            lastScrapedCount = _refreshKey;
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

    // ── Bind drill-down click handlers ONCE (E4.A) ──────────────
    // $.queriesList is a persistent DOM node across polls — only its innerHTML
    // is replaced inside update(). Binding here (not inside update()) prevents
    // listener accumulation. Per E4 merge brief §5.
    if ($.queriesList) {
        bindQueriesPanelClicks($.queriesList);
        $.queriesList.addEventListener('qp:filter', (ev) => {
            const sq = ev.detail.searchQuery;
            window.location.hash = `#/job/${encodeURIComponent(batchId)}?q=${encodeURIComponent(sq)}`;
        });
    }

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
