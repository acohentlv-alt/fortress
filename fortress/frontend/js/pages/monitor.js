/**
 * Pipeline Monitor Page — Live progress for running jobs
 *
 * Architecture (Issue 2+3 fixes):
 *   - Page skeleton is rendered ONCE (no full DOM rebuild on poll)
 *   - update() patches only the changing elements by ID
 *   - Polling interval is registered via registerCleanup() so it
 *     is automatically cleared when the user navigates away
 *   - Poll rate: 1.5s for snappy feeling
 */

import { getJobs, getJob, getJobQuality, getJobCompanies } from '../api.js';
import { breadcrumb, statusBadge, formatDateTime, escapeHtml, renderGauge, companyCard } from '../components.js';
import { registerCleanup } from '../app.js';

let pollInterval = null;

export async function renderMonitor(container, queryId) {
    // Clear any previous polling (safety net — cleanup system handles this too)
    if (pollInterval) {
        clearInterval(pollInterval);
        pollInterval = null;
    }

    if (queryId) {
        queryId = decodeURIComponent(queryId);
        await renderJobMonitor(container, queryId);
    } else {
        await renderMonitorList(container);
    }
}

async function renderMonitorList(container) {
    const jobs = await getJobs();
    const runningJobs = (jobs || []).filter(j =>
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
            </div>
        ` : `
            <div class="job-list">
                ${runningJobs.map(j => {
        const batchSize = j.batch_size || j.total_companies || 1;
        const scraped = j.companies_scraped || 0;
        const pct = Math.min(100, Math.round((scraped / batchSize) * 100));
        return `
                        <div class="job-card" onclick="window.location.hash='#/monitor/${encodeURIComponent(j.query_id)}'">
                            <div class="job-card-info">
                                <div class="job-card-name">${escapeHtml(j.query_name)}</div>
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

        ${(jobs || []).filter(j => j.status === 'completed').length > 0 ? `
            <h2 style="font-size:var(--font-lg); font-weight:600; margin-top:var(--space-2xl); margin-bottom:var(--space-lg)">
                Terminés récemment
            </h2>
            <div class="job-list">
                ${(jobs || []).filter(j => j.status === 'completed').slice(0, 5).map(j => `
                    <div class="job-card" onclick="window.location.hash='#/job/${encodeURIComponent(j.query_id)}'">
                        <div class="job-card-info">
                            <div class="job-card-name">${escapeHtml(j.query_name)}</div>
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

async function renderJobMonitor(container, queryId) {
    // ── Render skeleton ONCE ───────────────────────────────────────
    container.innerHTML = `
        <div id="mon-breadcrumb"></div>

        <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:var(--space-xl); margin-bottom:var(--space-2xl)">
            <div>
                <h1 class="page-title" id="mon-title">Chargement...</h1>
                <div style="display:flex; align-items:center; gap:var(--space-md); margin-top:var(--space-sm)" id="mon-status-row"></div>
            </div>
            <a href="#/job/${encodeURIComponent(queryId)}" class="btn btn-secondary">📋 Détail complet</a>
        </div>

        <!-- Big Progress -->
        <div class="card" style="margin-bottom:var(--space-xl); text-align:center; padding:var(--space-2xl)">
            <div style="font-size:var(--font-3xl); font-weight:800; margin-bottom:var(--space-sm)" id="mon-pct">0%</div>
            <div class="progress-bar" style="height:12px; max-width:500px; margin:0 auto var(--space-lg)">
                <div class="progress-bar-fill animated" style="width:0%; transition:width 0.5s ease" id="mon-bar"></div>
            </div>
            <div style="display:flex; justify-content:center; gap:var(--space-2xl); font-size:var(--font-sm); color:var(--text-secondary)" id="mon-counts">
                ⏳ Chargement...
            </div>
        </div>

        <!-- Wave + Triage -->
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:var(--space-xl); margin-bottom:var(--space-xl)">
            <div class="card">
                <h3 class="detail-section-title">Progression des vagues</h3>
                <div style="font-size:var(--font-2xl); font-weight:700; margin-bottom:var(--space-sm)" id="mon-waves">0 / ?</div>
                <div style="font-size:var(--font-sm); color:var(--text-secondary)">Vagues complétées</div>
            </div>
            <div class="card">
                <h3 class="detail-section-title">Triage</h3>
                <div style="display:flex; gap:var(--space-xl); font-size:var(--font-base)" id="mon-triage">—</div>
            </div>
        </div>

        <!-- Quality Gauges -->
        <div class="card" style="margin-bottom:var(--space-xl)">
            <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                Qualité des données enrichies
            </h3>
            <div style="display:flex; gap:var(--space-2xl); justify-content:center" id="mon-gauges">—</div>
        </div>

        <div style="font-size:var(--font-xs); color:var(--text-muted); text-align:center" id="mon-footer">
            Actualisation automatique toutes les 1.5 secondes
        </div>

        <!-- Live Company Cards -->
        <div style="margin-top:var(--space-2xl)">
            <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)" id="mon-cards-title">
                📋 Entreprises collectées (0)
            </h3>
            <div id="mon-cards">
                <div style="text-align:center; color:var(--text-muted); font-style:italic; padding:var(--space-lg)">
                    ⏳ En attente des premières données...
                </div>
            </div>
        </div>
    `;

    // ── Smart update function — patches by ID, no full rebuild ─────
    let lastCardCount = 0;

    async function update() {
        // Guard: if user navigated away, stop polling
        const currentHash = window.location.hash || '#/';
        if (!currentHash.includes('/monitor/')) {
            if (pollInterval) { clearInterval(pollInterval); pollInterval = null; }
            return;
        }

        let job;
        try {
            job = await getJob(queryId);
        } catch { return; }
        if (!job || job.error) return;

        const batchSize = job.batch_size || job.total_companies || 1;
        const scraped = job.companies_scraped || 0;
        const failed = job.companies_failed || 0;
        const pct = Math.min(100, Math.round((scraped / batchSize) * 100));
        const isRunning = job.status === 'in_progress' || job.status === 'queued' || job.status === 'triage';

        // Patch elements by ID
        const $title = document.getElementById('mon-title');
        const $statusRow = document.getElementById('mon-status-row');
        const $pct = document.getElementById('mon-pct');
        const $bar = document.getElementById('mon-bar');
        const $counts = document.getElementById('mon-counts');
        const $waves = document.getElementById('mon-waves');
        const $triage = document.getElementById('mon-triage');
        const $gauges = document.getElementById('mon-gauges');
        const $footer = document.getElementById('mon-footer');
        const $cardsTitle = document.getElementById('mon-cards-title');
        const $breadcrumb = document.getElementById('mon-breadcrumb');

        if (!$title) return; // Container was replaced by another page

        $breadcrumb.innerHTML = breadcrumb([
            { label: 'Pipeline Live', href: '#/monitor' },
            { label: job.query_name },
        ]);
        $title.textContent = job.query_name;
        $statusRow.innerHTML = `
            ${statusBadge(job.status)}
            ${isRunning ? '<span style="color:var(--warning); animation:pulse 2s infinite">● EN DIRECT</span>' : ''}
        `;

        // Progress — smooth CSS transition on the bar
        $pct.textContent = pct + '%';
        $bar.style.width = pct + '%';
        if (!isRunning) $bar.classList.remove('animated');

        $counts.innerHTML = `
            <span>✅ Complétées: <strong>${scraped}</strong></span>
            <span>❌ Échouées: <strong>${failed}</strong></span>
            <span>📊 Batch: <strong>${batchSize}</strong></span>
            ${job.replaced_count ? `<span>🔄 Remplacées: <strong>${job.replaced_count}</strong></span>` : ''}
        `;

        $waves.textContent = `${job.wave_current || 0} / ${job.wave_total || '?'}`;
        $triage.innerHTML = `
            <span>🟢 ${job.triage_green || 0}</span>
            <span>🔵 ${job.triage_blue || 0}</span>
            <span>🟡 ${job.triage_yellow || 0}</span>
            <span>🔴 ${job.triage_red || 0}</span>
            <span>⚫ ${job.triage_black || 0}</span>
        `;

        // Quality gauges — refresh every poll
        try {
            const q = await getJobQuality(queryId) || {};
            $gauges.innerHTML = `
                ${renderGauge(q.phone_pct || 0, '📞 Tél.')}
                ${renderGauge(q.email_pct || 0, '✉️ Email')}
                ${renderGauge(q.website_pct || 0, '🌐 Web')}
                ${renderGauge(q.siret_pct || 0, '🏢 SIRET')}
            `;
        } catch { /* gauges are optional */ }

        $footer.textContent = isRunning
            ? `Actualisation automatique toutes les 1.5 secondes · Dernière mise à jour: ${formatDateTime(job.updated_at)}`
            : `Batch terminé · ${formatDateTime(job.updated_at)}`;

        // Company cards — only rebuild if count changed
        $cardsTitle.textContent = `📋 Entreprises collectées (${scraped})`;
        if (scraped > 0 && scraped !== lastCardCount) {
            lastCardCount = scraped;
            try {
                const cardData = await getJobCompanies(queryId, { page: 1, pageSize: 50, sort: 'completude' });
                const $cards = document.getElementById('mon-cards');
                if ($cards && cardData && cardData.companies && cardData.companies.length > 0) {
                    $cards.innerHTML = `
                        <div class="company-grid">
                            ${cardData.companies.map(c => companyCard(c)).join('')}
                        </div>
                    `;
                }
            } catch { /* cards are optional display */ }
        } else if (scraped === 0) {
            const $cards = document.getElementById('mon-cards');
            if ($cards) {
                $cards.innerHTML = `
                    <div style="text-align:center; color:var(--text-muted); font-style:italic; padding:var(--space-lg)">
                        ${isRunning ? '⏳ En attente des premières données...' : 'Aucune entreprise collectée'}
                    </div>
                `;
            }
        }

        // Stop polling if job is done
        if (!isRunning && pollInterval) {
            clearInterval(pollInterval);
            pollInterval = null;
        }
    }

    // Initial render
    await update();

    // Auto-poll every 1.5s (faster than before for snappier UX)
    pollInterval = setInterval(update, 1500);

    // Register cleanup so navigating away clears the interval
    registerCleanup(() => {
        if (pollInterval) {
            clearInterval(pollInterval);
            pollInterval = null;
        }
    });
}

// Add pulse animation
if (!document.getElementById('pulse-style')) {
    const style = document.createElement('style');
    style.id = 'pulse-style';
    style.textContent = `
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.3; }
        }
    `;
    document.head.appendChild(style);
}
