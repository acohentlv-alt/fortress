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

import { getJobs, getJob, getJobQuality, getJobCompanies, cancelJob } from '../api.js';
import {
    breadcrumb, statusBadge, formatDateTime, escapeHtml,
    renderGauge, companyCard, renderTriageBar, renderPipelineStages,
    animateCounter, renderProgressRing, showConfirmModal, showToast,
} from '../components.js';
import { registerCleanup } from '../app.js';
import { getCachedUser } from '../api.js';

let pollInterval = null;

export async function renderMonitor(container, batchId) {
    // Clear any previous polling (safety net — cleanup system handles this too)
    if (pollInterval) {
        clearInterval(pollInterval);
        pollInterval = null;
    }

    if (batchId) {
        batchId = decodeURIComponent(batchId);
        await renderJobMonitor(container, batchId);
    } else {
        await renderMonitorList(container);
    }
}

async function renderMonitorList(container) {
    const jobs = await getJobs();
    const jobsList = Array.isArray(jobs) ? jobs : [];
    const runningJobs = jobsList.filter(j =>
        j.status === 'in_progress' || j.status === 'queued' || j.status === 'triage'
    );

    container.innerHTML = `
        <h1 class="page-title">Pipeline Live</h1>
        <p class="page-subtitle">Suivi en temps réel des batchs en cours</p>

        ${runningJobs.length === 0 ? `
            <div class="empty-state">
                <div class="empty-state-icon">📡</div>
                <div class="empty-state-text">Aucun batch en cours</div>
                <p style="color: var(--text-muted)">Les batchs actifs apparaîtront ici automatiquement</p>
                <a href="#/new-batch" class="btn btn-primary" style="margin-top:var(--space-lg)">🚀 Lancer un batch</a>
            </div>
        ` : `
            <div class="job-list">
                ${runningJobs.map(j => {
        const batchSize = j.batch_size || j.total_companies || 1;
        const qualified = j.companies_qualified || 0;
        const scraped = j.companies_scraped || 0;
        const pct = Math.min(100, Math.round((qualified / batchSize) * 100));
        return `
                        <div class="job-card" onclick="window.location.hash='#/monitor/${encodeURIComponent(j.batch_id)}'">
                            <div class="job-card-info">
                                <div class="job-card-name">${escapeHtml(j.batch_name)}</div>
                                <div class="job-card-meta">
                                    <span>${formatDateTime(j.created_at)}</span>
                                    <span>${scraped}/${batchSize} entreprises</span>
                                    ${j.wave_total ? `<span>Vague ${j.wave_current || 0}/${j.wave_total}</span>` : ''}
                                </div>
                            </div>
                            <div class="job-card-stats">
                                ${statusBadge(j.status)}
                                <div style="width:100px">
                                    <div class="progress-bar">
                                        <div class="progress-bar-fill animated" style="width:${pct}%"></div>
                                    </div>
                                </div>
                            </div>
                        </div>
                    `;
    }).join('')}
            </div>
        `}

        ${jobsList.filter(j => j.status === 'completed').length > 0 ? `
            <h2 style="font-size:var(--font-lg); font-weight:600; margin-top:var(--space-2xl); margin-bottom:var(--space-lg)">
                Terminés récemment
            </h2>
            <div class="job-list">
                ${jobsList.filter(j => j.status === 'completed').slice(0, 5).map(j => `
                    <div class="job-card" onclick="window.location.hash='#/job/${encodeURIComponent(j.batch_id)}'">
                        <div class="job-card-info">
                            <div class="job-card-name">${escapeHtml(j.batch_name)}</div>
                            <div class="job-card-meta">
                                <span>${formatDateTime(j.updated_at)}</span>
                                <span>${j.companies_scraped || 0} entreprises</span>
                            </div>
                        </div>
                        ${statusBadge(j.status)}
                    </div>
                `).join('')}
            </div>
        ` : ''}
    `;
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
                <h1 class="page-title" id="mon-title">Chargement...</h1>
                <div style="display:flex; align-items:center; gap:var(--space-md); margin-top:var(--space-sm)" id="mon-status-row"></div>
            </div>
            <div style="display:flex; align-items:center; gap:var(--space-sm)">
                <button id="mon-cancel-btn" class="btn btn-secondary" style="color:var(--danger); display:none" title="Arrêter ce batch">⏹ Arrêter</button>
                <a href="#/job/${encodeURIComponent(batchId)}" class="btn btn-secondary">📋 Détail complet</a>
            </div>
        </div>

        <!-- Poll warning (hidden by default) -->
        <div id="mon-poll-warning" style="display:none; margin-bottom:var(--space-lg)"></div>

        <!-- Progress Ring + Metrics -->
        <div class="card" style="margin-bottom:var(--space-xl)">
            <div style="display:flex; align-items:center; gap:var(--space-2xl); flex-wrap:wrap">
                <!-- Progress Ring -->
                <div style="flex-shrink:0" id="mon-ring">
                    ${renderProgressRing(0, 140)}
                </div>

                <!-- Metric Cards -->
                <div style="flex:1; min-width:280px">
                    <div class="monitor-metrics">
                        <div class="monitor-metric">
                            <div class="monitor-metric-value" id="mon-scraped">0</div>
                            <div class="monitor-metric-label">Complétées</div>
                        </div>
                        <div class="monitor-metric">
                            <div class="monitor-metric-value" id="mon-failed">0</div>
                            <div class="monitor-metric-label">Échouées</div>
                        </div>
                        <div class="monitor-metric">
                            <div class="monitor-metric-value" id="mon-batch">0</div>
                            <div class="monitor-metric-label">Cible</div>
                        </div>
                        ${isAdmin ? `
                        <div class="monitor-metric">
                            <div class="monitor-metric-value" id="mon-replaced">0</div>
                            <div class="monitor-metric-label">Substitutions</div>
                        </div>
                        ` : '<div id="mon-replaced" style="display:none">0</div>'}
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

        <!-- Pool Breakdown (admin only) -->
        <div class="card" style="margin-bottom:var(--space-xl)">
            <h3 class="detail-section-title">Répartition du pool</h3>
            <div id="mon-triage">—</div>
        </div>
        ` : `
        <div id="mon-pipeline" style="display:none"></div>
        <div id="mon-wave" style="display:none"></div>
        <div id="mon-triage" style="display:none"></div>
        `}

        <!-- Quality Gauges -->
        <div class="card" style="margin-bottom:var(--space-xl)">
            <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                Qualité des données enrichies
            </h3>
            <div style="display:flex; gap:var(--space-2xl); justify-content:center" id="mon-gauges">—</div>
        </div>

        <!-- Completion CTA (hidden by default) -->
        <div id="mon-completion" style="display:none; margin-bottom:var(--space-xl)"></div>

        <div style="font-size:var(--font-xs); color:var(--text-muted); text-align:center" id="mon-footer">
            Actualisation automatique toutes les 1.5 secondes
        </div>

        <!-- Live Company Cards -->
        <div style="margin-top:var(--space-2xl)">
            <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)" id="mon-cards-title">
                📋 Entreprises collectées (0)
            </h3>
            <div class="company-grid" id="mon-cards">
                <div style="text-align:center; color:var(--text-muted); font-style:italic; padding:var(--space-lg); grid-column:1/-1">
                    ⏳ En attente des premières données...
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
        batch: document.getElementById('mon-batch'),
        replaced: document.getElementById('mon-replaced'),
        pipeline: document.getElementById('mon-pipeline'),
        wave: document.getElementById('mon-wave'),
        triage: document.getElementById('mon-triage'),
        gauges: document.getElementById('mon-gauges'),
        footer: document.getElementById('mon-footer'),
        cardsTitle: document.getElementById('mon-cards-title'),
        cards: document.getElementById('mon-cards'),
        breadcrumb: document.getElementById('mon-breadcrumb'),
        pollWarning: document.getElementById('mon-poll-warning'),
        completion: document.getElementById('mon-completion'),
        cancelBtn: document.getElementById('mon-cancel-btn'),
    };

    // ── State tracking ──────────────────────────────────────────
    const renderedSirens = new Set();
    let lastScrapedCount = 0;
    let failedPolls = 0;
    let previousValues = { scraped: -1, failed: -1, batch: -1, replaced: -1, pct: -1 };

    // ── Cancel button handler ───────────────────────────────────────────
    if ($.cancelBtn) {
        $.cancelBtn.addEventListener('click', () => {
            const scraped = previousValues.scraped > 0 ? previousValues.scraped : 0;
            const batchSize = previousValues.batch > 0 ? previousValues.batch : 0;
            const remaining = Math.max(0, batchSize - scraped);
            const pct = batchSize > 0 ? Math.round((scraped / batchSize) * 100) : 0;
            showConfirmModal({
                title: '\u23f9 Arr\u00eater ce batch ?',
                body: `
                    <p><strong>Batch :</strong> ${escapeHtml($.title.textContent)}</p>
                    <p><strong>Progression :</strong> ${scraped}/${batchSize} entreprises (${pct}%)</p>
                    <p style="color:var(--success)">\u2705 Les ${scraped} entreprises d\u00e9j\u00e0 collect\u00e9es seront conserv\u00e9es.</p>
                    <p style="color:var(--warning)">\u26a0\ufe0f Les ${remaining} restantes ne seront pas trait\u00e9es.</p>
                `,
                confirmLabel: 'Arr\u00eater le batch',
                danger: true,
                onConfirm: async () => {
                    const result = await cancelJob(batchId);
                    if (result._ok !== false) {
                        showToast('Batch arr\u00eat\u00e9', 'success');
                    } else {
                        showToast('Erreur lors de l\'arr\u00eat', 'error');
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
                $.pollWarning.innerHTML = '<div class="poll-warning">⚠️ Connexion interrompue — nouvelle tentative...</div>';
            } else if (failedPolls >= 2) {
                $.pollWarning.innerHTML = '<div class="poll-warning">⚠️ Connexion perdue — nouvelles tentatives en cours...</div>';
            }
            return; // Keep last valid state
        }
        if (!job || job.error) return;

        const batchSize = job.batch_size || job.total_companies || 1;
        const scraped = job.companies_scraped || 0;
        const qualified = job.companies_qualified || 0;
        const failed = job.companies_failed || 0;
        const replaced = job.replaced_count || 0;
        // Progress = qualified companies / batch_size (only phone-confirmed count)
        const pct = Math.min(100, Math.round((qualified / batchSize) * 100));
        const isRunning = job.status === 'in_progress' || job.status === 'queued' || job.status === 'triage';
        const isUpload = job.mode === 'upload';

        // ── Breadcrumb + Title ──────────────────────────────────
        $.breadcrumb.innerHTML = breadcrumb([
            { label: 'Pipeline Live', href: '#/monitor' },
            { label: job.batch_name },
        ]);
        $.title.textContent = job.batch_name || 'Batch en cours';
        $.statusRow.innerHTML = `
            ${statusBadge(job.status || 'queued')}
            ${isRunning ? '<span class="live-badge"><span class="live-badge-dot"></span> EN DIRECT</span>' : ''}
        `;

        // Show/hide cancel button
        if ($.cancelBtn) {
            $.cancelBtn.style.display = isRunning ? '' : 'none';
        }

        // ── Progress Ring — only update if pct changed ──────────
        if (pct !== previousValues.pct) {
            previousValues.pct = pct;
            const ringCircle = document.getElementById('progress-ring-circle');
            const ringPct = document.getElementById('progress-ring-pct');
            if (ringCircle && ringPct) {
                // Update SVG dashoffset for smooth transition
                const r = (140 - 6) / 2; // match renderProgressRing defaults
                const circumference = 2 * Math.PI * r;
                const offset = circumference - (pct / 100) * circumference;
                ringCircle.setAttribute('stroke-dashoffset', offset);
                ringPct.textContent = pct + '%';
            }
        }

        // ── Metric Counters — animate only on change ────────────
        if (qualified !== previousValues.scraped) {
            previousValues.scraped = qualified;  // Show qualified count as "Complétées"
            animateCounter($.scraped, qualified);
        }
        if (failed !== previousValues.failed) {
            previousValues.failed = failed;
            animateCounter($.failed, failed);
        }
        if (batchSize !== previousValues.batch) {
            previousValues.batch = batchSize;
            animateCounter($.batch, batchSize);
        }
        if (replaced !== previousValues.replaced) {
            previousValues.replaced = replaced;
            animateCounter($.replaced, replaced);
        }

        // ── Pipeline Stage (hide for uploads) ──────────────────
        if (isUpload) {
            $.pipeline.style.display = 'none';
            if ($.pipeline.parentElement) $.pipeline.parentElement.style.display = 'none';
        } else {
            let stage = null;
            if (isRunning) {
                if (job.status === 'triage') stage = null;
                else if (qualified > 0) stage = 'inpi';
                else stage = 'maps';
            } else if (job.status === 'completed') {
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
                    <span class="wave-chip-text">🌊 Vague ${waveCurrent}/${waveTotal}</span>
               </div>`
            : '';

        // ── Triage Bar (hide for uploads) ────────────────────────
        if (isUpload) {
            $.triage.style.display = 'none';
            if ($.triage.parentElement) $.triage.parentElement.style.display = 'none';
        } else {
            $.triage.innerHTML = renderTriageBar({
                green: job.triage_green,
                yellow: job.triage_yellow,
                red: job.triage_red,
                black: job.triage_black,
                blue: job.triage_blue,
            });
        }

        // ── Quality Gauges ──────────────────────────────────────
        try {
            const q = await getJobQuality(batchId) || {};
            $.gauges.innerHTML = `
                ${renderGauge(q.phone_pct || 0, '📞 Tél.')}
                ${renderGauge(q.email_pct || 0, '✉️ Email')}
                ${renderGauge(q.website_pct || 0, '🌐 Web')}
                ${renderGauge(q.officers_pct || 0, '👤 Dirigeants')}
                ${renderGauge(q.financials_pct || 0, '💰 Financier')}
                ${renderGauge(q.siret_pct || q.social_pct || 0, '🔗 Social')}
            `;
        } catch { /* gauges are optional */ }

        // ── Completion State ────────────────────────────────────
        if (!isRunning && job.status === 'completed') {
            $.completion.style.display = 'block';
            $.completion.innerHTML = `
                <div class="completion-card">
                    <div class="completion-icon">${qualified > 0 ? '🎉' : '⚠️'}</div>
                    <div class="completion-title">${qualified > 0 ? 'Batch terminé !' : 'Batch terminé — aucun résultat qualifié'}</div>
                    <div class="completion-subtitle">${qualified} entreprises qualifiées sur ${scraped} tentées</div>
                    <a href="#/job/${encodeURIComponent(batchId)}" class="btn btn-primary">📋 Voir les résultats</a>
                </div>
            `;
        } else if (!isRunning && job.status === 'failed') {
             $.completion.style.display = 'block';
             $.completion.innerHTML = `
                 <div class="completion-card" style="border-left: 4px solid var(--danger); background: color-mix(in srgb, var(--bg-surface) 90%, var(--danger));">
                     <div class="completion-icon" style="background:var(--danger-bg); color:var(--danger)">⚠️</div>
                     <div class="completion-title" style="color:var(--danger)">Batch interrompu</div>
                     <div class="completion-subtitle">Le processus s'est arrêté de manière inattendue. Les ${qualified} entreprises qualifiées ont été conservées.</div>
                     <a href="#/job/${encodeURIComponent(batchId)}" class="btn" style="border:1px solid var(--danger); color:var(--text)">📋 Voir les résultats partiels</a>
                 </div>
             `;
        }

        // ── Footer ──────────────────────────────────────────────
        let footerText = `Actualisation automatique · ${formatDateTime(job.updated_at)}`;
        if (!isRunning) {
            footerText = job.status === 'failed' 
                ? `Batch interrompu · ${formatDateTime(job.updated_at)}` 
                : `Batch terminé · ${formatDateTime(job.updated_at)}`;
        }
        $.footer.textContent = footerText;

        // ── Company Cards — append-only (track rendered SIRENs) ──
        $.cardsTitle.textContent = `📋 Entreprises collectées (${qualified})`;

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
        } else if (scraped === 0) {
            $.cards.innerHTML = `
                <div style="text-align:center; color:var(--text-muted); font-style:italic; padding:var(--space-lg); grid-column:1/-1">
                    ${isRunning ? '⏳ En attente des premières données...' : 'Aucune entreprise collectée'}
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

    // ── Register cleanup ────────────────────────────────────────
    registerCleanup(() => {
        if (pollInterval) {
            clearInterval(pollInterval);
            pollInterval = null;
        }
    });
}
