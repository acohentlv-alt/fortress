/**
 * Activity Log Page — Admin-only audit trail
 *
 * Shows all user actions: batch launches, uploads, deletions, cancellations.
 * Time-filtered with auto-refresh every 30 seconds.
 */

import { getActivityLog, extractApiError, getCachedUser } from '../api.js';
import { escapeHtml, showToast } from '../components.js';
import { registerCleanup } from '../app.js';

const ACTION_ICONS = {
    batch_launched: '🚀',
    upload: '📤',
    delete_job: '🗑️',
    cancel_job: '⏹️',
    delete_tags: '🏷️',
    export: '📥',
    note_added: '📝',
    note_deleted: '🗑️',
};

const ACTION_LABELS = {
    batch_launched: 'Recherche lancée',
    upload: 'Fichier importé',
    delete_job: 'Batch supprimé',
    cancel_job: 'Batch annulé',
    delete_tags: 'Tags supprimés',
    export: 'Export effectué',
    note_added: 'Note ajoutée',
    note_deleted: 'Note supprimée',
};

function _timeAgo(dateStr) {
    if (!dateStr) return '';
    const d = new Date(dateStr);
    const now = new Date();
    const diffMs = now - d;
    const mins = Math.floor(diffMs / 60000);
    if (mins < 1) return "à l'instant";
    if (mins < 60) return `il y a ${mins} min`;
    const hours = Math.floor(mins / 60);
    if (hours < 24) return `il y a ${hours}h`;
    const days = Math.floor(hours / 24);
    if (days === 1) return 'hier';
    return `il y a ${days} jours`;
}

export async function renderActivity(container) {
    // Admin guard
    const user = getCachedUser();
    if (!user || user.role !== 'admin') {
        container.innerHTML = `
            <div class="empty-state" style="margin-top:var(--space-2xl)">
                <div class="empty-state-icon">🔒</div>
                <div class="empty-state-text">Accès réservé aux administrateurs</div>
            </div>
        `;
        return;
    }

    let currentPeriod = 'week';
    let currentOffset = 0;
    const PAGE_SIZE = 50;

    container.innerHTML = `
        <h1 class="page-title">📋 Suivi d'activité</h1>
        <p class="page-subtitle">Journal des actions effectuées par les utilisateurs</p>

        <div style="display:flex; gap:var(--space-sm); margin-bottom:var(--space-xl); flex-wrap:wrap">
            <button class="view-toggle-btn" id="period-day">Aujourd'hui</button>
            <button class="view-toggle-btn active" id="period-week">Cette semaine</button>
            <button class="view-toggle-btn" id="period-month">Ce mois</button>
            <button class="view-toggle-btn" id="period-all">Tout</button>
        </div>

        <div id="activity-feed">
            <div class="loading"><div class="spinner"></div></div>
        </div>
    `;

    const feedEl = document.getElementById('activity-feed');

    async function loadFeed(offset = 0) {
        currentOffset = offset;
        feedEl.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

        const data = await getActivityLog({ period: currentPeriod, limit: PAGE_SIZE, offset: currentOffset });

        if (!data || (data._status && !data._ok)) {
            feedEl.innerHTML = `
                <div class="error-state text-center" style="padding:var(--space-xl)">
                    <div style="font-size:2rem; margin-bottom:var(--space-md)">⚠️</div>
                    <div style="color:var(--text-secondary)">${escapeHtml(extractApiError(data))}</div>
                </div>
            `;
            return;
        }

        const entries = data.entries || [];
        const total = data.total || entries.length;

        if (entries.length === 0) {
            feedEl.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">📋</div>
                    <div class="empty-state-text">Aucune activité pour cette période</div>
                </div>
            `;
            return;
        }

        const currentPage = Math.floor(currentOffset / PAGE_SIZE) + 1;
        const totalPages = Math.ceil(total / PAGE_SIZE);
        const hasNext = currentOffset + PAGE_SIZE < total;
        const hasPrev = currentOffset > 0;

        feedEl.innerHTML = `
            <div style="font-size:var(--font-sm); color:var(--text-muted); margin-bottom:var(--space-md)">
                ${total} action${total > 1 ? 's' : ''} ${totalPages > 1 ? `— page ${currentPage}/${totalPages}` : ''}
            </div>
            <div class="activity-list">
                ${entries.map(e => `
                    <div class="activity-item card" style="padding:var(--space-md) var(--space-lg); margin-bottom:var(--space-sm); display:flex; gap:var(--space-md); align-items:flex-start">
                        <span style="font-size:1.5rem; flex-shrink:0">${ACTION_ICONS[e.action] || '📌'}</span>
                        <div style="flex:1; min-width:0">
                            <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:var(--space-xs)">
                                <span style="font-weight:600; color:var(--text-primary)">
                                    👤 ${escapeHtml(e.username || 'système')}
                                </span>
                                <span style="font-size:var(--font-xs); color:var(--text-muted); white-space:nowrap">
                                    ${_timeAgo(e.created_at)}
                                </span>
                            </div>
                            <div style="font-size:var(--font-sm); color:var(--text-secondary); margin-top:2px">
                                <strong>${ACTION_LABELS[e.action] || e.action}</strong>
                                ${e.details ? ` — ${escapeHtml(e.details)}` : ''}
                            </div>
                            ${e.target_id ? `
                                <div style="font-size:var(--font-xs); color:var(--text-muted); margin-top:2px; font-family:var(--font-mono)">
                                    ${escapeHtml(e.target_id)}
                                </div>
                            ` : ''}
                        </div>
                    </div>
                `).join('')}
            </div>

            ${totalPages > 1 ? `
                <div style="display:flex; justify-content:center; align-items:center; gap:var(--space-lg); margin-top:var(--space-xl)">
                    <button class="btn btn-secondary" id="activity-prev" ${hasPrev ? '' : 'disabled'}
                        style="${hasPrev ? '' : 'opacity:0.4; cursor:not-allowed'}">← Précédent</button>
                    <span style="font-size:var(--font-sm); color:var(--text-secondary); font-weight:600">${currentPage} / ${totalPages}</span>
                    <button class="btn btn-secondary" id="activity-next" ${hasNext ? '' : 'disabled'}
                        style="${hasNext ? '' : 'opacity:0.4; cursor:not-allowed'}">Suivant →</button>
                </div>
            ` : ''}
        `;

        const prevBtn = document.getElementById('activity-prev');
        const nextBtn = document.getElementById('activity-next');
        if (prevBtn && hasPrev) prevBtn.addEventListener('click', () => loadFeed(currentOffset - PAGE_SIZE));
        if (nextBtn && hasNext) nextBtn.addEventListener('click', () => loadFeed(currentOffset + PAGE_SIZE));
    }

    // Period toggle
    ['day', 'week', 'month', 'all'].forEach(p => {
        document.getElementById(`period-${p}`)?.addEventListener('click', () => {
            document.querySelectorAll('.view-toggle-btn').forEach(b => b.classList.remove('active'));
            document.getElementById(`period-${p}`).classList.add('active');
            currentPeriod = p;
            loadFeed(0);
        });
    });

    // Auto-refresh every 30s
    const refreshTimer = setInterval(() => {
        if (window.location.hash === '#/activity') {
            loadFeed(currentOffset);
        }
    }, 30000);
    registerCleanup(() => clearInterval(refreshTimer));

    // Initial load
    loadFeed(0);
}
