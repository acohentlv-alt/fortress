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

// ── Contact Indicators ───────────────────────────────────────────
export function contactIndicators(contact) {
    if (!contact) contact = {};
    return `
        <div class="company-card-contacts">
            <span class="contact-indicator ${contact.phone ? 'has-data' : 'no-data'}" title="Téléphone">
                📞 ${contact.phone ? 'Oui' : '—'}
            </span>
            <span class="contact-indicator ${contact.email ? 'has-data' : 'no-data'}" title="Email">
                ✉️ ${contact.email ? 'Oui' : '—'}
            </span>
            <span class="contact-indicator ${contact.website ? 'has-data' : 'no-data'}" title="Site web">
                🌐 ${contact.website ? 'Oui' : '—'}
            </span>
            <span class="contact-indicator ${contact.address ? 'has-data' : 'no-data'}" title="Adresse">
                📍 ${contact.address ? 'Oui' : '—'}
            </span>
            <span class="contact-indicator ${contact.rating ? 'has-data' : 'no-data'}" title="Note Google">
                ⭐ ${contact.rating || '—'}
            </span>
            ${contact.maps_url ? `<a href="${contact.maps_url}" target="_blank" rel="noopener" class="contact-indicator has-data" title="Voir sur Google Maps" onclick="event.stopPropagation()" style="text-decoration:none">
                🗺️ Maps
            </a>` : ''}
        </div>
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
export function companyCard(company) {
    const siren = company.siren || '';
    return `
        <div class="company-card" onclick="window.location.hash='#/company/${siren}'">
            <div class="company-card-header">
                <div>
                    <div class="company-card-name">${escapeHtml(company.denomination || '—')}</div>
                    <div class="company-card-siren">${formatSiren(siren)}</div>
                </div>
                <div style="display:flex; gap:6px; align-items:center;">
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
