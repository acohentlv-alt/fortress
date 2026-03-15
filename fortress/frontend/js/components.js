/**
 * Reusable UI component helpers for Fortress.
 */

// ── Format Helpers ───────────────────────────────────────────────
export function formatSiren(siren) {
    if (!siren) return '—';
    const s = siren.replace(/\s/g, '');
    return `${s.slice(0, 3)} ${s.slice(3, 6)} ${s.slice(6, 9)}`;
}

export function formatSiret(siret) {
    if (!siret) return '—';
    const s = siret.replace(/\s/g, '');
    return `${s.slice(0, 3)} ${s.slice(3, 6)} ${s.slice(6, 9)} ${s.slice(9)}`;
}

export function formatDate(d) {
    if (!d) return '—';
    try {
        const date = new Date(d);
        return date.toLocaleDateString('fr-FR', { day: '2-digit', month: '2-digit', year: 'numeric' });
    } catch { return '—'; }
}

export function formatDateTime(d) {
    if (!d) return '—';
    try {
        const date = new Date(d);
        return date.toLocaleDateString('fr-FR', {
            day: '2-digit', month: '2-digit', year: 'numeric',
            hour: '2-digit', minute: '2-digit',
        });
    } catch { return '—'; }
}

// ── Gauge Component ──────────────────────────────────────────────
export function renderGauge(pct, label, color = null) {
    const r = 20;
    const circumference = 2 * Math.PI * r;
    const offset = circumference - (pct / 100) * circumference;
    const c = color || (pct >= 80 ? 'var(--success)' : pct >= 50 ? 'var(--warning)' : 'var(--danger)');

    return `
        <div class="gauge">
            <div class="gauge-circle">
                <svg viewBox="0 0 48 48">
                    <circle class="gauge-circle-bg" cx="24" cy="24" r="${r}"></circle>
                    <circle class="gauge-circle-fill" cx="24" cy="24" r="${r}"
                        stroke="${c}"
                        stroke-dasharray="${circumference}"
                        stroke-dashoffset="${offset}">
                    </circle>
                </svg>
                <span class="gauge-value">${pct}%</span>
            </div>
            <span class="gauge-label">${label}</span>
        </div>
    `;
}

// ── Status Badge ─────────────────────────────────────────────────
export function statusBadge(status) {
    const map = {
        'completed': ['badge-success', '✅ Terminé'],
        'in_progress': ['badge-warning', '⏳ En cours'],
        'queued': ['badge-info', '📋 En attente'],
        'triage': ['badge-info', '🔍 Triage'],
        'new': ['badge-muted', '🆕 Nouveau'],
        'paused': ['badge-accent', '⏸ Pause'],
        'failed': ['badge-danger', '❌ Échoué'],
    };
    const [cls, text] = map[status] || ['badge-muted', status];
    return `<span class="badge ${cls}">${text}</span>`;
}

// ── Company Statut Badge ─────────────────────────────────────────
export function statutBadge(statut) {
    if (statut === 'A') return '<span class="badge badge-success">Actif</span>';
    if (statut === 'C') return '<span class="badge badge-danger">Cessé</span>';
    return `<span class="badge badge-muted">${statut || '—'}</span>`;
}

// ── Forme Juridique Badge ────────────────────────────────────────
const FORME_LABELS = {
    '1000': 'EI', '5306': 'EURL', '5307': 'SA', '5370': 'SAS',
    '5498': 'EURL', '5499': 'SARL', '5505': 'SA', '5510': 'SAS',
    '5515': 'SNC', '5520': 'SCS', '5522': 'SCA', '5525': 'SARL',
    '5530': 'SELASU', '5532': 'SELAS', '5560': 'SCI', '5599': 'SA',
    '5710': 'SAS', '5720': 'SASU', '9220': 'Asso',
};

export function formeJuridiqueBadge(code) {
    const label = FORME_LABELS[code] || code || '';
    if (!label) return '';
    return `<span class="badge badge-accent">${label}</span>`;
}

// ── Contact Indicators (enhanced — shows actual values) ─────────
export function contactIndicators(contact) {
    if (!contact) contact = {};

    // Truncate long values for card display
    const truncate = (val, max = 20) => {
        if (!val) return null;
        const s = String(val);
        return s.length > max ? s.slice(0, max) + '…' : s;
    };

    const phoneDisplay = contact.phone
        ? (() => {
            const cleanPhone = String(contact.phone).replace(/[\n\r\s]+/g, '').trim();
            return `<a href="tel:${escapeHtml(cleanPhone)}" onclick="event.stopPropagation()" style="color:var(--success);text-decoration:none;white-space:nowrap">${escapeHtml(cleanPhone)}</a>`;
        })()
        : '—';

    const emailDisplay = contact.email
        ? `<a href="mailto:${escapeHtml(contact.email)}" onclick="event.stopPropagation()" style="color:var(--accent-hover);text-decoration:none">${escapeHtml(truncate(contact.email, 24))}</a>`
        : '—';

    const webDisplay = contact.website
        ? `<a href="${contact.website.startsWith('http') ? contact.website : 'https://' + contact.website}" target="_blank" rel="noopener" onclick="event.stopPropagation()" style="color:var(--info);text-decoration:none">${escapeHtml(truncate(contact.website, 22))}</a>`
        : '—';

    // Social icons row
    const socials = [];
    if (contact.social_linkedin) socials.push(`<a href="${contact.social_linkedin}" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="LinkedIn" style="color:var(--text-muted);text-decoration:none;font-size:14px">🔗</a>`);
    if (contact.social_facebook) socials.push(`<a href="${contact.social_facebook}" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="Facebook" style="color:var(--text-muted);text-decoration:none;font-size:14px">📘</a>`);
    if (contact.maps_url) socials.push(`<a href="${contact.maps_url}" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="Google Maps" style="color:var(--text-muted);text-decoration:none;font-size:14px">🗺️</a>`);

    return `
        <div class="company-card-contacts">
            <span class="contact-indicator ${contact.phone ? 'has-data' : 'no-data'}" title="Téléphone">
                📞 ${phoneDisplay}
            </span>
            <span class="contact-indicator ${contact.email ? 'has-data' : 'no-data'}" title="Email">
                ✉️ ${emailDisplay}
            </span>
            <span class="contact-indicator ${contact.website ? 'has-data' : 'no-data'}" title="Site web">
                🌐 ${webDisplay}
            </span>
        </div>
        ${contact.rating ? `<div style="margin-top:var(--space-xs)">${renderStarRating(contact.rating, contact.review_count)}</div>` : ''}
        ${socials.length > 0 ? `<div style="display:flex;gap:var(--space-sm);margin-top:var(--space-xs)">${socials.join('')}</div>` : ''}
    `;
}

// ── Completude Bar ───────────────────────────────────────────────
export function completudeBar(company) {
    let score = 0;
    const fields = ['phone', 'email', 'website', 'address', 'rating', 'maps_url'];
    for (const f of fields) {
        if (company[f]) score++;
    }
    if (company.siret_siege) score++;
    if (company.naf_code) score++;
    const pct = Math.round((score / 8) * 100);
    const cls = pct >= 80 ? 'high' : pct >= 50 ? 'medium' : 'low';
    return `
        <div class="completude-bar">
            <div class="completude-bar-fill ${cls}" style="width: ${pct}%"></div>
        </div>
    `;
}

// ── Compact Company Card ─────────────────────────────────────────
export function companyCard(company, opts = {}) {
    const siren = company.siren || '';
    const removeBtn = opts.removable ? `
        <button class="card-remove-btn" data-siren="${siren}" title="Retirer de cette requête"
            onclick="event.stopPropagation();">×</button>
    ` : '';

    // Detect if company has ANY enrichment data
    const hasData = company.phone || company.email || company.website;

    // Compact row for SIRENE-only companies (no enrichment)
    if (!hasData && !opts.forceExpand) {
        return `
            <div class="company-card company-card-compact" data-siren="${siren}" onclick="window.location.hash='#/company/${siren}'"
                 style="padding:var(--space-md) var(--space-lg); min-height:auto">
                <div style="display:flex; align-items:center; justify-content:space-between; gap:var(--space-md)">
                    <div style="min-width:0; flex:1">
                        <div style="display:flex; align-items:baseline; gap:var(--space-sm); min-width:0">
                            <span class="company-card-name" style="font-size:var(--font-sm); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; min-width:0; flex:1">${escapeHtml(company.denomination || '—')}</span>
                            <span style="color:var(--text-muted); font-size:var(--font-xs); white-space:nowrap; flex-shrink:0">${formatSiren(siren)}</span>
                        </div>
                        <div style="display:flex; gap:var(--space-sm); margin-top:2px; font-size:var(--font-xs); color:var(--text-secondary)">
                            <span style="white-space:nowrap">📍 ${escapeHtml(company.ville || '—')}${company.departement ? ` (${company.departement})` : ''}</span>
                            ${company.naf_libelle ? `<span style="color:var(--text-muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:200px">· ${escapeHtml(company.naf_libelle)}</span>` : ''}
                        </div>
                    </div>
                    <div style="display:flex; gap:6px; align-items:center; flex-shrink:0">
                        ${removeBtn}
                        ${company.maps_url ? `<a href="${company.maps_url}" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="Google Maps" style="color:var(--text-muted);text-decoration:none;font-size:14px">🗺️</a>` : ''}
                        ${statutBadge(company.statut)}
                    </div>
                </div>
            </div>
        `;
    }

    // Full card for enriched companies
    return `
        <div class="company-card" data-siren="${siren}" onclick="window.location.hash='#/company/${siren}'">
            <div class="company-card-header">
                <div>
                    <div class="company-card-name">${escapeHtml(company.denomination || '—')}</div>
                    <div class="company-card-siren">${formatSiren(siren)}</div>
                </div>
                <div style="display:flex; gap:6px; align-items:center;">
                    ${removeBtn}
                    ${statutBadge(company.statut)}
                    ${formeJuridiqueBadge(company.forme_juridique)}
                </div>
            </div>
            <div class="company-card-location">
                📍 ${escapeHtml(company.ville || '—')}${company.departement ? ` (${company.departement})` : ''}
                ${company.naf_libelle ? ` · ${escapeHtml(company.naf_libelle)}` : ''}
            </div>
            ${contactIndicators(company)}
            ${completudeBar(company)}
        </div>
    `;
}

// ── Pagination ───────────────────────────────────────────────────
// Uses event delegation on a stable container instead of per-button
// listeners. A single click handler is attached to the parent; the
// onPageChange callback is stored on a mutable ref so re-renders
// update the callback without adding new listeners.

const _paginationState = new WeakMap();

export function renderPagination(currentPage, totalPages, onPageChange) {
    if (totalPages <= 1) return '';

    const pages = [];
    const range = 2;
    for (let i = Math.max(1, currentPage - range); i <= Math.min(totalPages, currentPage + range); i++) {
        pages.push(i);
    }

    setTimeout(() => {
        const el = document.querySelector('.pagination');
        if (!el) return;
        const container = el.parentElement;
        if (!container) return;

        // If we already attached a handler to this container, just update the callback ref
        const existing = _paginationState.get(container);
        if (existing) {
            existing.callback = onPageChange;
            return;
        }

        // First time: attach ONE delegated handler
        const state = { callback: onPageChange };
        const handler = (e) => {
            const btn = e.target.closest('.pagination-btn');
            if (!btn || btn.disabled) return;
            const p = parseInt(btn.dataset.page);
            if (p && !isNaN(p)) state.callback(p);
        };
        container.addEventListener('click', handler);
        _paginationState.set(container, state);
    }, 0);

    return `
        <div class="pagination">
            <button class="pagination-btn" data-page="${currentPage - 1}" ${currentPage <= 1 ? 'disabled' : ''}>‹</button>
            ${pages.map(p => `
                <button class="pagination-btn ${p === currentPage ? 'active' : ''}" data-page="${p}">${p}</button>
            `).join('')}
            <button class="pagination-btn" data-page="${currentPage + 1}" ${currentPage >= totalPages ? 'disabled' : ''}>›</button>
        </div>
    `;
}

// ── Escape HTML ──────────────────────────────────────────────────
export function escapeHtml(str) {
    if (!str) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

// ── Toast Notifications ──────────────────────────────────────────
export function showToast(message, type = 'success') {
    // Ensure container exists
    let container = document.querySelector('.toast-container');
    if (!container) {
        container = document.createElement('div');
        container.className = 'toast-container';
        document.body.appendChild(container);
    }

    const icons = { success: '✅', error: '❌', info: 'ℹ️', warning: '⚠️' };
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `
        <span class="toast-icon">${icons[type] || icons.info}</span>
        <span class="toast-message">${message}</span>
        <button class="toast-close" onclick="this.parentElement.remove()">✕</button>
    `;
    container.appendChild(toast);

    // Auto-dismiss after 4s
    setTimeout(() => {
        toast.classList.add('toast-leaving');
        setTimeout(() => toast.remove(), 300);
    }, 4000);
}

// ── Breadcrumb ───────────────────────────────────────────────────
export function breadcrumb(items) {
    return `
        <nav class="breadcrumb">
            ${items.map((item, i) => {
        if (i < items.length - 1) {
            return `<a href="${item.href}">${item.label}</a><span class="breadcrumb-sep">›</span>`;
        }
        return `<span style="color: var(--text-primary)">${item.label}</span>`;
    }).join('')}
        </nav>
    `;
}

// ── Star Rating ──────────────────────────────────────────────────
// Edge cases: null rating → "—", rating > 5 → capped, 0 reviews → shows "(0 avis)"
export function renderStarRating(rating, reviewCount) {
    if (rating === null || rating === undefined) return '<span style="color:var(--text-muted)">—</span>';
    const r = Math.min(5, Math.max(0, parseFloat(rating) || 0));
    const full = Math.round(r);
    const empty = 5 - full;
    const count = reviewCount != null ? reviewCount : 0;
    return `
        <span class="star-rating">
            <span class="star-rating-value">${r.toFixed(1)}</span>
            <span class="star-rating-stars">${'★'.repeat(full)}${'☆'.repeat(empty)}</span>
            <span class="star-rating-count">(${count} avis)</span>
        </span>
    `;
}

// ── Triage Bar ───────────────────────────────────────────────────
// Edge cases: all zero → empty bar, missing categories → 0, single category → full width
export function renderTriageBar(triage) {
    if (!triage) triage = {};
    const green = triage.green || triage.triage_green || 0;
    const yellow = triage.yellow || triage.triage_yellow || 0;
    const red = triage.red || triage.triage_red || 0;
    const black = triage.black || triage.triage_black || 0;
    const blue = triage.blue || triage.triage_blue || 0;
    const total = green + yellow + red + black + blue;

    const pct = (v) => total > 0 ? ((v / total) * 100).toFixed(1) : 0;

    const segments = [
        { cls: 'green', val: green, label: 'Complet' },
        { cls: 'yellow', val: yellow, label: 'Partiel' },
        { cls: 'red', val: red, label: 'Nouveau' },
        { cls: 'black', val: black, label: 'Blacklisté' },
        { cls: 'blue', val: blue, label: 'Client' },
    ].filter(s => s.val > 0);

    return `
        <div class="triage-bar">
            ${segments.map(s => `<div class="triage-bar-segment ${s.cls}" style="width:${pct(s.val)}%" title="${s.label}: ${s.val}"></div>`).join('')}
        </div>
        <div class="triage-legend">
            ${segments.map(s => `
                <span class="triage-legend-item">
                    <span class="triage-legend-dot ${s.cls}"></span>
                    ${s.label}: ${s.val}
                </span>
            `).join('')}
        </div>
    `;
}

// ── Pipeline Stage Indicator ─────────────────────────────────────
// Edge cases: unknown/null stage → all grey (safe default)
export function renderPipelineStages(activeStage) {
    const stages = [
        { id: 'maps', icon: '🗺️', label: 'Maps' },
        { id: 'crawl', icon: '🌐', label: 'Crawl' },
        { id: 'save', icon: '💾', label: 'Sauvegarde' },
    ];

    let foundActive = false;
    const stageHTML = stages.map((s, i) => {
        let cls = '';
        if (s.id === activeStage) {
            cls = 'active';
            foundActive = true;
        } else if (!foundActive && activeStage) {
            cls = 'completed';
        }
        // If activeStage is null/unknown, all stay default (grey)

        const arrow = i < stages.length - 1
            ? `<span class="pipeline-stage-arrow ${cls === 'completed' ? 'completed' : ''}">→</span>`
            : '';

        return `
            <span class="pipeline-stage ${cls}">
                <span class="pipeline-stage-icon">${s.icon}</span>
                ${s.label}
            </span>
            ${arrow}
        `;
    }).join('');

    return `<div class="pipeline-stages">${stageHTML}</div>`;
}

// ── Animate Counter ──────────────────────────────────────────────
// Edge cases: overlapping calls → cancels previous, same value → no-op, null → "—"
const _counterAnimations = new WeakMap();

export function animateCounter(element, targetValue, duration = 600) {
    if (!element) return;

    // Handle null/undefined
    if (targetValue === null || targetValue === undefined) {
        element.textContent = '—';
        return;
    }

    const target = parseInt(targetValue) || 0;
    const current = parseInt(element.textContent) || 0;

    // Same value → no-op
    if (current === target) return;

    // Cancel any previous animation on this element
    const prev = _counterAnimations.get(element);
    if (prev) cancelAnimationFrame(prev);

    const startTime = performance.now();
    const diff = target - current;

    function step(now) {
        const elapsed = now - startTime;
        const progress = Math.min(elapsed / duration, 1);
        // Ease-out expo
        const eased = 1 - Math.pow(1 - progress, 3);
        const value = Math.round(current + diff * eased);
        element.textContent = value.toLocaleString('fr-FR');

        if (progress < 1) {
            const id = requestAnimationFrame(step);
            _counterAnimations.set(element, id);
        } else {
            element.textContent = target.toLocaleString('fr-FR');
            _counterAnimations.delete(element);
        }
    }

    const id = requestAnimationFrame(step);
    _counterAnimations.set(element, id);
}

// ── Progress Ring (SVG) ──────────────────────────────────────────
export function renderProgressRing(pct, size = 120, strokeWidth = 6) {
    const r = (size - strokeWidth) / 2;
    const circumference = 2 * Math.PI * r;
    const offset = circumference - (Math.min(100, Math.max(0, pct)) / 100) * circumference;

    return `
        <div class="progress-ring" style="width:${size}px;height:${size}px">
            <svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}">
                <circle class="progress-ring-bg" cx="${size/2}" cy="${size/2}" r="${r}"></circle>
                <circle class="progress-ring-fill" cx="${size/2}" cy="${size/2}" r="${r}"
                    stroke-dasharray="${circumference}"
                    stroke-dashoffset="${offset}"
                    id="progress-ring-circle"></circle>
            </svg>
            <div class="progress-ring-text">
                <div class="progress-ring-pct" id="progress-ring-pct">${Math.round(pct)}%</div>
                <div class="progress-ring-label">Progression</div>
            </div>
        </div>
    `;
}

// ── Confirmation Modal ───────────────────────────────────────────
/**
 * Show a reusable confirmation modal.
 * @param {object} opts
 * @param {string} opts.title     - Modal title
 * @param {string} opts.body      - HTML body content
 * @param {string} opts.confirmLabel - Confirm button text (default: 'Confirmer')
 * @param {boolean} opts.danger   - If true, confirm button is red
 * @param {Function} opts.onConfirm - Async callback when confirmed
 */
export function showConfirmModal({ title, body, confirmLabel = 'Confirmer', danger = false, onConfirm }) {
    // Remove any existing modal
    const existing = document.getElementById('confirm-modal-overlay');
    if (existing) existing.remove();

    const overlay = document.createElement('div');
    overlay.id = 'confirm-modal-overlay';
    overlay.className = 'modal-overlay';
    overlay.innerHTML = `
        <div class="modal-content">
            <h3 class="modal-title">${title}</h3>
            <div class="modal-body">${body}</div>
            <div class="modal-actions">
                <button id="modal-cancel" class="btn btn-secondary">Annuler</button>
                <button id="modal-confirm" class="btn ${danger ? 'btn-danger' : 'btn-primary'}">${confirmLabel}</button>
            </div>
        </div>
    `;
    document.body.appendChild(overlay);
    // Fade in
    requestAnimationFrame(() => overlay.classList.add('visible'));

    const close = () => {
        overlay.classList.remove('visible');
        setTimeout(() => overlay.remove(), 200);
    };

    // Backdrop click
    overlay.addEventListener('click', (e) => {
        if (e.target === overlay) close();
    });

    // Escape key
    const onKey = (e) => { if (e.key === 'Escape') { close(); document.removeEventListener('keydown', onKey); } };
    document.addEventListener('keydown', onKey);

    // Cancel button
    document.getElementById('modal-cancel').addEventListener('click', close);

    // Confirm button
    document.getElementById('modal-confirm').addEventListener('click', async () => {
        const btn = document.getElementById('modal-confirm');
        btn.disabled = true;
        btn.innerHTML = '<span class="spinner" style="width:14px;height:14px;border-width:2px;display:inline-block;vertical-align:middle;margin-right:6px"></span> …';
        try {
            await onConfirm();
        } catch (err) {
            console.error('Modal confirm error:', err);
        }
        close();
    });
}

