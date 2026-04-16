/**
 * Activity Log Page — Admin-only audit trail
 *
 * Shows all user actions: batch launches, uploads, deletions, cancellations.
 * Time-filtered with auto-refresh every 30 seconds.
 */

import { getActivityLog, extractApiError, getCachedUser } from '../api.js';
import { escapeHtml, showToast } from '../components.js';
import { registerCleanup } from '../app.js';
import { t, getLang } from '../i18n.js';

const ACTION_ICONS = {
    batch_launched: '🚀',
    batch_completed: '✅',
    batch_failed: '❌',
    upload: '📤',
    delete_job: '🗑️',
    cancel_job: '⏹️',
    delete_tags: '🏷️',
    export: '📥',
    note_added: '📝',
    note_deleted: '🗑️',
    contact_request: '✉️',
    conflict_resolved: '✅',
    conflict_dismissed: '❌',
    merge: '🔀',
    link: '🔗',
    approve_link: '🔗',
    reject_link: '❌',
    unlink: '🔓',
    manual_edit: '✏️',
};

function getActionLabels() {
    return {
        batch_launched: t('activity.batch_launched'),
        batch_completed: t('activity.batch_completed'),
        batch_failed: t('activity.batch_failed'),
        upload: t('activity.upload'),
        delete_job: t('activity.delete_job'),
        cancel_job: t('activity.cancel_job'),
        delete_tags: t('activity.delete_tags'),
        export: t('activity.export'),
        note_added: t('activity.note_added'),
        note_deleted: t('activity.note_deleted'),
        contact_request: t('activity.contact_request'),
        conflict_resolved: t('activity.conflict_resolved'),
        conflict_dismissed: t('activity.conflict_dismissed'),
        merge: t('activity.merge'),
        link: t('activity.link'),
        approve_link: t('activity.approve_link'),
        reject_link: t('activity.reject_link'),
        unlink: t('activity.unlink'),
        manual_edit: t('activity.manual_edit'),
    };
}

function _timeAgo(dateStr) {
    if (!dateStr) return '';
    const d = new Date(dateStr);
    const now = new Date();
    const diffMs = now - d;
    const mins = Math.floor(diffMs / 60000);
    if (mins < 1) return t('activity.justNow');
    if (mins < 60) return t('activity.minutesAgo', { minutes: mins });
    const hours = Math.floor(mins / 60);
    if (hours < 24) return t('activity.hoursAgo', { hours });
    const days = Math.floor(hours / 24);
    if (days === 1) return t('activity.yesterday');
    return t('activity.daysAgo', { days });
}

export async function renderActivity(container) {
    // Admin + head guard
    const user = getCachedUser();
    if (!user || (user.role !== 'admin' && user.role !== 'head')) {
        container.innerHTML = `
            <div class="empty-state" style="margin-top:var(--space-2xl)">
                <div class="empty-state-icon">🔒</div>
                <div class="empty-state-text">${t('activity.accessDenied')}</div>
            </div>
        `;
        return;
    }

    let currentPeriod = 'week';
    let currentActionType = 'all';
    let currentOffset = 0;
    const PAGE_SIZE = 50;

    container.innerHTML = `
        <h1 class="page-title">📋 ${t('activity.title')}</h1>
        <p class="page-subtitle">${t('activity.subtitle')}</p>

        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:var(--space-xl); flex-wrap:wrap; gap:var(--space-md)">
            <div style="display:flex; gap:var(--space-sm); flex-wrap:wrap">
                <button class="view-toggle-btn type-toggle active" data-type="all">${t('activity.allActions')}</button>
                <button class="view-toggle-btn type-toggle" data-type="batches">${t('activity.batchesOps')}</button>
                <button class="view-toggle-btn type-toggle" data-type="notes">${t('activity.notesComments')}</button>
            </div>
            <div style="display:flex; gap:var(--space-sm); flex-wrap:wrap">
                <button class="view-toggle-btn period-toggle" data-period="day">${t('activity.today')}</button>
                <button class="view-toggle-btn period-toggle active" data-period="week">${t('activity.thisWeek')}</button>
                <button class="view-toggle-btn period-toggle" data-period="month">${t('activity.thisMonth')}</button>
                <button class="view-toggle-btn period-toggle" data-period="all">${t('activity.allTime')}</button>
            </div>
        </div>

        <div id="activity-feed">
            <div class="loading"><div class="spinner"></div></div>
        </div>
    `;

    const feedEl = document.getElementById('activity-feed');

    async function loadFeed(offset = 0) {
        currentOffset = offset;
        feedEl.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

        const data = await getActivityLog({ period: currentPeriod, action_type: currentActionType, limit: PAGE_SIZE, offset: currentOffset });

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
                    <div class="empty-state-text">${t('activity.noActivity')}</div>
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
                    <div class="activity-item card" style="padding:var(--space-md) var(--space-lg); margin-bottom:var(--space-sm); display:flex; gap:var(--space-md); align-items:flex-start; ${e.action === 'batch_failed' ? 'border-left:4px solid var(--text-error)' : ''}">
                        <span style="font-size:1.5rem; flex-shrink:0">${ACTION_ICONS[e.action] || '📌'}</span>
                        <div style="flex:1; min-width:0">
                            <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:var(--space-xs)">
                                <span style="font-weight:600; color:var(--text-primary)">
                                    👤 ${escapeHtml(e.username || t('activity.systemUser'))}
                                </span>
                                <span style="font-size:var(--font-xs); color:var(--text-muted); white-space:nowrap">
                                    ${_timeAgo(e.created_at)}
                                </span>
                            </div>
                            <div style="font-size:var(--font-sm); color:var(--text-secondary); margin-top:4px">
                                <strong>${getActionLabels()[e.action] || e.action}</strong>
                                ${e.details ? `<div style="margin-top:4px; padding:var(--space-xs) var(--space-sm); background:var(--bg-secondary); border-radius:4px; ${e.action === 'batch_failed' ? 'color:var(--text-error); background:rgba(239, 68, 68, 0.1);' : ''}">${escapeHtml(e.details)}</div>` : ''}
                            </div>
                            ${e.target_id ? `
                                <div style="font-size:var(--font-xs); color:var(--text-muted); margin-top:2px; font-family:var(--font-mono)">
                                    ${e.target_type === 'company' ? `<a href="#/company/${encodeURIComponent(e.target_id)}" style="color:var(--accent); text-decoration:none; font-weight:600" title="Voir la fiche entreprise">${escapeHtml(e.target_id)} ↗</a>` : escapeHtml(e.target_id)}
                                </div>
                            ` : ''}
                        </div>
                    </div>
                `).join('')}
            </div>

            ${totalPages > 1 ? `
                <div style="display:flex; justify-content:center; align-items:center; gap:var(--space-lg); margin-top:var(--space-xl)">
                    <button class="btn btn-secondary" id="activity-prev" ${hasPrev ? '' : 'disabled'}
                        style="${hasPrev ? '' : 'opacity:0.4; cursor:not-allowed'}">← ${t('common.previous')}</button>
                    <span style="font-size:var(--font-sm); color:var(--text-secondary); font-weight:600">${currentPage} / ${totalPages}</span>
                    <button class="btn btn-secondary" id="activity-next" ${hasNext ? '' : 'disabled'}
                        style="${hasNext ? '' : 'opacity:0.4; cursor:not-allowed'}">${t('common.next')} →</button>
                </div>
            ` : ''}
        `;

        const prevBtn = document.getElementById('activity-prev');
        const nextBtn = document.getElementById('activity-next');
        if (prevBtn && hasPrev) prevBtn.addEventListener('click', () => loadFeed(currentOffset - PAGE_SIZE));
        if (nextBtn && hasNext) nextBtn.addEventListener('click', () => loadFeed(currentOffset + PAGE_SIZE));
    }

    document.querySelectorAll('.type-toggle').forEach(btn => {
        btn.addEventListener('click', (e) => {
            document.querySelectorAll('.type-toggle').forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
            currentActionType = e.target.dataset.type;
            loadFeed(0);
        });
    });

    document.querySelectorAll('.period-toggle').forEach(btn => {
        btn.addEventListener('click', (e) => {
            document.querySelectorAll('.period-toggle').forEach(b => b.classList.remove('active'));
            e.target.classList.add('active');
            currentPeriod = e.target.dataset.period;
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
