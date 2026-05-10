/**
 * Company Detail Page — Pappers-style full view
 * Displays the enriched data model:
 *   Identity, Forme Juridique, Code NAF, Headcount, Dirigeants,
 *   Contacts, Revenue (graceful null → "Non public")
 *
 * Features:
 *   - Smart Enrichment Panel with goal-oriented checkboxes
 *   - Actionable empty states for unenriched fields
 *   - Notes (comments) system per company
 *   - 2-column Contact + Dirigeants layout at top
 *   - Context-aware breadcrumb (search vs upload vs batch)
 */

import { getCompany, enrichCompany, updateCompany, getCompanyEnrichHistory,
         getCompanyNotes, addCompanyNote, deleteCompanyNote,
         extractApiError, getCachedUser, getSuggestedMatches } from '../api.js';
import {
    breadcrumb, formatSiren, formatSiret, formatDate,
    statutBadge, formeJuridiqueBadge, escapeHtml, showToast, sourceLabel,
} from '../components.js';
import { t, getLang } from '../i18n.js';

// Effectif labels (INSEE codes)
const EFFECTIF_LABELS = {
    '00': '0 salarié', '01': '1-2 sal.', '02': '3-5 sal.', '03': '6-9 sal.',
    '11': '10-19 sal.', '12': '20-49 sal.', '21': '50-99 sal.', '22': '100-199 sal.',
    '31': '200-249 sal.', '32': '250-499 sal.', '41': '500-999 sal.', '42': '1 000+ sal.',
    '51': '2 000-4 999 sal.', '52': '5 000-9 999 sal.', '53': '10 000+ sal.',
};

function effectifLabel(code) {
    if (!code) return null;
    return EFFECTIF_LABELS[code] || code;
}

function nafStatusBadge(status, ctx) {
    // ctx = { link_confidence, link_method, link_signals, rescued_by } — optional, from company detail API
    const _STRONG_METHODS = new Set(['inpi', 'siren_website', 'enseigne', 'phone', 'address', 'inpi_fuzzy_agree', 'inpi_mentions_legales', 'chain', 'gemini_judge', 'geo_proximity', 'siret_address_naf']);

    // Existing variable name `map` preserved (was already `const map = {...}` at line 41 pre-fix).
    const map = {
        verified:  { cls: 'glass-badge--green',  icon: '✓', labelKey: 'company.nafVerified' },
        mismatch:  { cls: 'glass-badge--amber',  icon: '⚠', labelKey: 'company.nafMismatch' },  // Path D default
        maps_only: { cls: 'glass-badge--grey',   icon: '○', labelKey: 'company.nafMapsOnly' },
        no_filter: null,
    };
    const m = map[status];
    if (!m) return '';

    // Non-mismatch paths: render plain badge, no tooltip — unchanged.
    if (status !== 'mismatch') {
        return `<span class="glass-badge ${m.cls}">${m.icon} ${t(m.labelKey)}</span>`;
    }

    // No ctx → defensive plain badge (shouldn't hit on company detail page).
    if (!ctx) {
        return `<span class="glass-badge ${m.cls}">${m.icon} ${t(m.labelKey)}</span>`;
    }

    const { link_confidence, link_method, link_signals, rescued_by } = ctx;

    // ── Provenance branching: Path A (Phase 4 INPI) → B (Track 2/chain/footer-SIREN) → C (Gemini active) → D (Phase A default) ──
    // GUARD: Paths A/B/C are "informative" cyan badges that signal "the link IS correct, the NAF just describes a different head/operating split."
    // That semantic only holds when link_confidence='confirmed'. Pending rows (e.g., 20 ws174 rows with siren_website + naf_status='mismatch' + pending)
    // must NOT receive the informative badge — pending means the match itself is unconfirmed, so we must NOT signal "structurally fine."
    // Pending mismatches fall through to Path D's existing rich/terse warning logic, which already handles pending+strong+inpi_corroboration correctly.

    if (link_confidence === 'confirmed') {

        // Path A: INPI Phase 4 corroboration (rescued_by overrides link_method).
        if (rescued_by === 'inpi_validated') {
            const tooltip = `<span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>${t('company.nafMismatchInpiTitle')}</strong><br>${t('company.nafMismatchInpiBody')}</span></span>`;
            return `<span class="glass-badge glass-badge--cyan">ℹ ${t('company.nafMismatchInpi')}${tooltip}</span>`;
        }

        // Path B: Track 2 / chain / footer-SIREN — structural mismatch by design.
        if (link_method === 'siret_address_naf' || link_method === 'chain' || link_method === 'siren_website') {
            // Tooltip body varies by sub-path so Cindy gets the right structural reason.
            let bodyKey = 'company.nafMismatchEtablissementBodyTrack2';
            if (link_method === 'chain') bodyKey = 'company.nafMismatchEtablissementBodyChain';
            else if (link_method === 'siren_website') bodyKey = 'company.nafMismatchEtablissementBodyFooterSiren';
            const tooltip = `<span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>${t('company.nafMismatchEtablissementTitle')}</strong><br>${t(bodyKey)}</span></span>`;
            return `<span class="glass-badge glass-badge--cyan">ℹ ${t('company.nafMismatchEtablissement')}${tooltip}</span>`;
        }

        // Path C: Gemini active (D1b swap/rescue, Phase 2 promote).
        if (link_method === 'gemini_judge') {
            const tooltip = `<span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>${t('company.nafMismatchGeminiTitle')}</strong><br>${t('company.nafMismatchGeminiBody')}</span></span>`;
            return `<span class="glass-badge glass-badge--cyan">ℹ ${t('company.nafMismatchGemini')}${tooltip}</span>`;
        }

    }  // end if (link_confidence === 'confirmed')

    // Path D: Phase A 2-signal (default fallback) — preserve existing rich/terse tooltip logic VERBATIM.
    // Case D.1: rich tooltip with signal list. Triggers when EITHER:
    //   (a) confirmed + strong method + link_signals — the original rescued case
    //   (b) pending + strong method + link_signals.inpi_corroboration — Phase 4 enseigne fallback diagnostic
    const _hasInpiCorroboration = link_signals
        && link_signals.inpi_corroboration
        && typeof link_signals.inpi_corroboration === 'object';
    if ((link_confidence === 'confirmed' && _STRONG_METHODS.has(link_method) && link_signals)
        || (link_confidence === 'pending' && _STRONG_METHODS.has(link_method) && _hasInpiCorroboration)) {
        const signalKeys = {
            siren_website_match: 'company.signalSirenWebsite',
            phone_match:         'company.signalPhone',
            address_match:       'company.signalAddress',
            enseigne_match:      'company.signalEnseigne',
        };
        const trueSignals = new Set();
        for (const [k, v] of Object.entries(link_signals)) {
            if (v === true) trueSignals.add(k);
        }
        const corroboration = link_signals.inpi_corroboration;
        if (corroboration && typeof corroboration === 'object') {
            for (const [k, v] of Object.entries(corroboration)) {
                if (v === true) trueSignals.add(k);
            }
        }
        const agreed = [...trueSignals]
            .map(k => `<li>${t(signalKeys[k] || k)}</li>`)
            .join('');
        const signalList = agreed
            ? `<br><strong>${t('company.signalsAgreed')} :</strong><ul style="margin:4px 0 0 12px; padding:0">${agreed}</ul>`
            : '';
        const tooltip = `<span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>${t('company.nafMismatchStrongTitle')}</strong><br>${t('company.nafMismatchStrongBody')}${signalList}</span></span>`;
        return `<span class="glass-badge ${m.cls}">${m.icon} ${t(m.labelKey)}${tooltip}</span>`;
    }

    // Case D.2: pending + mismatch OR confirmed without link_signals (manual /link, NULL signals) → terse tooltip
    const terse = `<span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card">${t('company.nafMismatchTerse')}</span></span>`;
    return `<span class="glass-badge ${m.cls}">${m.icon} ${t(m.labelKey)}${terse}</span>`;
}

// Forme juridique — human-readable labels for common codes
const FORME_LABELS = {
    '1000': 'Entrepreneur individuel', '5306': 'EURL', '5307': 'SA à conseil d\'administration',
    '5370': 'SAS', '5498': 'EURL', '5499': 'SARL',
    '5505': 'SA à directoire', '5510': 'SAS', '5515': 'SNC',
    '5520': 'SCS', '5522': 'SCA', '5525': 'SARL unipersonnelle',
    '5530': 'SELASU', '5532': 'SELAS', '5560': 'SCI', '5599': 'SA',
    '5710': 'SAS', '5720': 'SASU', '9220': 'Association loi 1901',
    '9221': 'Association déclarée', '6316': 'SCOP', '6317': 'SCOP',
};
function _formeLabel(code) {
    if (!code) return '';
    const label = FORME_LABELS[code];
    return label ? `${label} <span style="color:var(--text-disabled); font-size:var(--font-xs)">(${code})</span>` : code;
}

function formatCurrency(val) {
    if (val === null || val === undefined) return null;
    return Number(val).toLocaleString(getLang() === 'fr' ? 'fr-FR' : 'en-US', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 });
}

// ── Unenriched field helper ──────────────────────────────────────
function unenrichedField() {
    return `<span class="unenriched-badge" style="opacity:0.6">—</span>`;
}

// ── Enrichment Panel — Crawl only (Maps already ran in batch) ────
function enrichmentPanelHTML() {
    return `
        <button class="btn-liquid enrich-submit" id="enrich-submit-btn">
            <span class="liquid-spinner"></span>
            <span class="liquid-text enrich-submit-text">🚀 ${t('company.enrichBtn')} ${t('components.website').toLowerCase()}</span>
        </button>
    `;
}

function renderNotesDirect(notes, limit = 2) {
    if (!notes || notes.length === 0) {
        return `<div style="color:var(--text-disabled); font-style:italic; padding:var(--space-sm) 0">${t('company.noNotesFull')}</div>`;
    }
    const currentUser = getCachedUser();
    
    // Slice notes if limit provided
    const displayNotes = limit ? notes.slice(0, limit) : notes;
    const hasMore = limit && notes.length > limit;
    
    let html = displayNotes.map(n => {
        const canDelete = currentUser && (currentUser.id === n.user_id || currentUser.role === 'admin');
        // Change 13: relative timestamps for recent notes (< 7 days), absolute for older
        const _noteTimestamp = (ts) => {
            if (!ts) return '';
            const now = Date.now();
            const then = new Date(ts).getTime();
            const diffMs = now - then;
            const diffDays = diffMs / (1000 * 60 * 60 * 24);
            const lang = getLang();
            const locale = lang === 'fr' ? 'fr-FR' : 'en-US';
            if (diffDays < 7) {
                const rtf = new Intl.RelativeTimeFormat(locale, { numeric: 'auto' });
                if (diffMs < 1000 * 60 * 60) {
                    return rtf.format(-Math.floor(diffMs / (1000 * 60)), 'minute');
                }
                if (diffDays < 1) {
                    return rtf.format(-Math.floor(diffMs / (1000 * 60 * 60)), 'hour');
                }
                return rtf.format(-Math.floor(diffDays), 'day');
            }
            return new Intl.DateTimeFormat(locale, { day: '2-digit', month: '2-digit', year: '2-digit', hour: '2-digit', minute: '2-digit' }).format(new Date(ts));
        };
        const timestampStr = _noteTimestamp(n.created_at);
        
        const username = n.username || 'Utilisateur';
        const initials = username.substring(0, 2).toUpperCase();
        
        return `
        <div class="note-item" style="padding:var(--space-md); background:var(--bg-tertiary, var(--bg-secondary)); border-radius:var(--radius-sm); margin-bottom:var(--space-sm); border:1px solid var(--border-subtle)">
            <div style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:var(--space-sm)">
                <div style="display:flex; align-items:center; gap:var(--space-sm)">
                    <div class="note-avatar" title="${escapeHtml(username)}" style="width:28px; height:28px; border-radius:50%; background:var(--accent-subtle); color:var(--accent-hover); display:flex; align-items:center; justify-content:center; font-size:10px; font-weight:700">
                        ${initials}
                    </div>
                    <div style="font-size:var(--font-xs); color:var(--text-secondary); display:flex; gap:var(--space-xs); align-items:center">
                        <span style="font-weight:600; color:var(--text-primary)" title="${escapeHtml(username)}">${escapeHtml(username)}</span>
                        <span style="opacity:0.5">•</span>
                        <span title="${n.created_at || ''}">${timestampStr}</span>
                    </div>
                </div>
                ${canDelete ? `<button class="btn-delete-note" data-note-id="${n.id}" style="background:none; border:none; color:var(--text-disabled); cursor:pointer; padding:4px" title="${t('company.deleteNote')}">🗑️</button>` : ''}
            </div>
            <div style="font-size:var(--font-sm); white-space:pre-wrap; color:var(--text-primary); line-height:1.5">${escapeHtml(n.text)}</div>
        </div>
        `;
    }).join('');

    if (hasMore) {
        html += `
            <button class="btn-show-all-notes" style="width:100%; padding:var(--space-sm); margin-top:var(--space-xs); background:transparent; border:1px solid var(--border-subtle); border-radius:var(--radius-sm); color:var(--text-muted); font-size:var(--font-xs); font-weight:600; cursor:pointer; transition:all 0.2s">
                ${t('company.showAllNotes', { count: notes.length })}
            </button>
        `;
    }
    return html;
}

// ── Social media section — only show filled fields ──────────────
function _buildSocialSection(mc, siren) {
    const socials = [
        { key: 'social_linkedin', label: 'LinkedIn', linkLabel: 'Profil LinkedIn' },
        { key: 'social_facebook', label: 'Facebook', linkLabel: 'Page Facebook' },
        { key: 'social_twitter', label: 'Twitter', linkLabel: 'Profil Twitter' },
        { key: 'social_instagram', label: 'Instagram', linkLabel: 'Profil Instagram' },
        { key: 'social_tiktok', label: 'TikTok', linkLabel: 'Profil TikTok' },
        { key: 'social_whatsapp', label: 'WhatsApp', linkLabel: 'WhatsApp' },
        { key: 'social_youtube', label: 'YouTube', linkLabel: 'YouTube' },
    ];

    const filled = socials.filter(s => mc[s.key]);

    if (filled.length === 0) {
        // Collapsed single line when no socials
        return `
            <div class="detail-row social-collapsed" style="border-top:1px solid var(--border-subtle); padding-top:var(--space-sm); margin-top:var(--space-xs)">
                <span class="detail-label" style="color:var(--text-disabled)">${t('company.socialNetworks')}</span>
                <span class="detail-value" style="color:var(--text-disabled); font-style:italic">${t('company.noSocials')}</span>
            </div>`;
    }

    return filled.map(s =>
        detailRow(s.label, formatSocial(mc[s.key], s.linkLabel), sourceLabel(mc[s.key + '_source']), s.key, mc[s.key] || '')
    ).join('');
}

// ── Context-aware breadcrumb ─────────────────────────────────────
// ── Entity Link Banner ───────────────────────────────────────────────

function _buildEntityLinkBanner(co, linkedCo, suggestedMatches, linkMethod, contacts, linkConfidence) {
    if (!co.siren || !co.siren.startsWith('MAPS')) return '';

    // Extract website crawl data for third column
    const crawl = (contacts || []).find(c => c.source === 'website_crawl') || {};
    const mapsContact = (contacts || []).find(c => c.source === 'google_maps') || {};

    const _buildCrawlColumn = () => {
        const sirenFromWebsite = crawl.siren_from_website || null;
        const hasCrawl = sirenFromWebsite || crawl.phone || crawl.email || crawl.website || crawl.social_linkedin || crawl.social_facebook;
        return `
        <div style="padding:var(--space-md); background:rgba(139,92,246,0.06); border:1px solid rgba(139,92,246,0.2); border-radius:var(--radius-md)">
            <div style="font-weight:700; font-size:var(--font-sm); color:#8b5cf6; margin-bottom:var(--space-sm)">${t('company.websiteCrawlLabel')}</div>
            <div style="font-size:var(--font-sm); display:flex; flex-direction:column; gap:4px; color:var(--text-primary)">
                ${hasCrawl ? `
                    ${sirenFromWebsite ? `<div style="font-weight:700; color:var(--success)">SIREN: ${escapeHtml(sirenFromWebsite)}</div>` : ''}
                    ${crawl.phone ? `<div>📞 ${escapeHtml(crawl.phone)}</div>` : ''}
                    ${crawl.email ? `<div>✉️ ${escapeHtml(crawl.email)}</div>` : ''}
                    ${crawl.website ? `<div>🌐 ${escapeHtml(crawl.website)}</div>` : ''}
                    ${crawl.social_linkedin ? `<div style="color:var(--text-secondary)">🔗 LinkedIn</div>` : ''}
                    ${crawl.social_facebook ? `<div style="color:var(--text-secondary)">📘 Facebook</div>` : ''}
                ` : `<div style="color:var(--text-disabled); font-style:italic">—</div>`}
            </div>
        </div>`;
    };

    const _linkReasonLabel = (method) => {
        if (method === 'enseigne') return t('company.linkReasonEnseigne');
        if (method === 'enseigne_weak') return t('company.linkReasonEnseigneWeak');
        if (method === 'phone') return t('company.linkReasonPhone');
        if (method === 'phone_weak') return t('company.linkReasonPhoneWeak');
        if (method === 'address') return t('company.linkReasonAddress');
        if (method === 'siren_website') return t('company.linkReasonSirenWebsite');
        if (method === 'fuzzy_name') return t('company.linkReasonFuzzy');
        if (method === 'manual') return t('company.linkReasonManual');
        if (method === 'surname') return t('company.linkReasonSurname');
        if (method === 'inpi') return t('company.linkReasonInpi');
        if (method === 'inpi_fuzzy_agree') return t('company.linkReasonInpiFuzzyAgree');
        if (method === 'inpi_mentions_legales') return t('company.linkReasonInpiMentionsLegales');
        if (method === 'chain') return t('company.linkReasonChain');
        if (method === 'gemini_judge') return t('company.linkReasonGeminiJudge');
        if (method === 'gemini_promoted') return t('company.linkReasonGeminiPromoted');
        if (method === 'geo_proximity') return t('company.linkReasonGeoProximity');
        return t('company.linkReasonAuto');
    };

    if (linkedCo) {
        // Confirmed link → banner is redundant (data lives in the CRM sections below,
        // SIREN in Identity, Maps address in Contact). Hide entirely. The Unlink
        // action lives in the Identity section's footer instead.
        if (linkConfidence === 'confirmed') {
            return '';
        }
        // Pending link (user still needs to confirm) — keep the 3-column comparison
        // so they can review before clicking "Finalize merge".
        const reasonText = _linkReasonLabel(linkMethod);
        return `
        <div id="entity-link-banner" class="card" style="background:linear-gradient(135deg, rgba(16,185,129,0.08), rgba(59,130,246,0.04)); border:1px solid rgba(16,185,129,0.3); margin-bottom:var(--space-lg); padding:var(--space-lg); border-radius:var(--radius-lg)">
            <div style="display:flex; align-items:center; gap:var(--space-sm); margin-bottom:var(--space-md)">
                <span style="font-size:1.3rem">🔗</span>
                <span style="font-weight:700; color:var(--success); font-size:var(--font-base)">${t('company.linkedEnriched', { name: escapeHtml(linkedCo.denomination || '') })}</span>
                <span style="font-size:var(--font-sm); color:var(--text-secondary); margin-left:var(--space-sm); font-weight:600">${reasonText}</span>
            </div>
            <div style="font-size:var(--font-sm); color:var(--text-secondary); margin-bottom:var(--space-md); margin-top:calc(-1 * var(--space-xs))">${t('company.linkedEnrichedSubtitle')}</div>
            <div class="entity-banner-grid">
                <!-- Left: Maps data -->
                <div style="padding:var(--space-md); background:rgba(59,130,246,0.06); border:1px solid rgba(59,130,246,0.2); border-radius:var(--radius-md)">
                    <div style="font-weight:700; font-size:var(--font-sm); color:var(--accent); margin-bottom:var(--space-sm)">${t('company.mapsDataLabel')}</div>
                    <div style="font-size:var(--font-sm); display:flex; flex-direction:column; gap:4px; color:var(--text-primary)">
                        <div><strong>${escapeHtml(co.denomination)}</strong></div>
                        ${co.adresse ? `<div style="color:var(--text-secondary)">${escapeHtml(co.adresse)}</div>` : ''}
                        ${mapsContact.phone ? `<div>📞 ${escapeHtml(mapsContact.phone)}</div>` : ''}
                        ${mapsContact.website ? `<div>🌐 ${escapeHtml(mapsContact.website)}</div>` : ''}
                    </div>
                </div>
                <!-- Right: SIRENE data -->
                <div style="padding:var(--space-md); background:rgba(16,185,129,0.06); border:1px solid rgba(16,185,129,0.2); border-radius:var(--radius-md)">
                    <div style="font-weight:700; font-size:var(--font-sm); color:var(--success); margin-bottom:var(--space-sm)">${t('company.sireneDataLabel')}</div>
                    <div style="font-size:var(--font-sm); display:flex; flex-direction:column; gap:4px; color:var(--text-primary)">
                        <div><strong>${escapeHtml(linkedCo.denomination || '')}</strong></div>
                        ${linkedCo.adresse ? `<div style="color:var(--text-secondary)">${escapeHtml(linkedCo.adresse)}</div>` : ''}
                        <div style="color:var(--text-secondary)">SIREN: ${linkedCo.siren}</div>
                        ${linkedCo.naf_code ? `<div style="color:var(--text-secondary)">NAF: ${escapeHtml(linkedCo.naf_code)}</div>` : ''}
                        ${linkedCo.ville ? `<div style="color:var(--text-secondary)">${escapeHtml(linkedCo.ville)}</div>` : ''}
                    </div>
                </div>
                <!-- Third column: Website crawl data -->
                ${_buildCrawlColumn()}
            </div>
            <div style="display:flex; gap:var(--space-md); justify-content:center">
                <button class="btn btn-primary btn-sm" id="btn-merge-entity" data-maps="${co.siren}" data-target="${linkedCo.siren}" style="font-size:var(--font-sm)">${t('company.btnMerge')}</button>
                <button class="btn btn-secondary btn-sm" id="btn-unlink-entity" data-maps="${co.siren}" style="font-size:var(--font-sm); opacity:0.7">${t('company.btnUnlink')}</button>
            </div>
        </div>`;
    }

    if (suggestedMatches.length > 0) {
        const m = suggestedMatches[0]; // Show the best match
        const methodLabel = m.method === 'address' ? t('company.methodLabelAddress')
            : m.method === 'fuzzy_name' ? t('company.methodLabelFuzzy')
            : m.method === 'phone' ? t('company.methodLabelPhone')
            : m.method === 'phone_weak' ? t('company.methodLabelPhoneWeak')
            : m.method === 'enseigne' ? t('company.methodLabelEnseigne')
            : m.method === 'enseigne_weak' ? t('company.methodLabelEnseigneWeak')
            : m.method === 'siren_website' ? t('company.methodLabelSirenWebsite')
            : m.method === 'surname' ? t('company.methodLabelSurname')
            : m.method;

        // Build additional context hints
        const hints = [];
        if (m.ville && co.adresse && co.adresse.toLowerCase().includes(m.ville.toLowerCase())) {
            hints.push(t('company.contextSameCity'));
        }
        if (m.address && co.adresse) {
            const streetOnly = (s) =>
                (s.split(',')[0] || '').toUpperCase().replace(/[.,\-]/g, ' ').replace(/\s+/g, ' ').trim();
            const mapsStreet = streetOnly(co.adresse);
            const sireneStreet = streetOnly(m.address);
            if (mapsStreet && sireneStreet && mapsStreet === sireneStreet) {
                hints.push(t('company.contextSameAddress'));
            } else {
                const mapsWords = co.adresse.toLowerCase().split(/[\s,]+/).filter(w => w.length > 3);
                const sireneWords = m.address.toLowerCase().split(/[\s,]+/).filter(w => w.length > 3);
                const commonWords = mapsWords.filter(w => sireneWords.includes(w));
                if (commonWords.length > 0) {
                    hints.push(t('company.contextSimilarAddress'));
                }
            }
        }
        const contextStr = hints.length > 0 ? ` · ${hints.join(' · ')}` : '';

        return `
        <div id="entity-link-banner" class="card" style="background:linear-gradient(135deg, rgba(251,191,36,0.08), rgba(59,130,246,0.04)); border:1px solid rgba(251,191,36,0.3); margin-bottom:var(--space-lg); padding:var(--space-lg); border-radius:var(--radius-lg)">
            <div style="display:flex; align-items:center; gap:var(--space-sm); margin-bottom:var(--space-md)">
                <span style="font-size:1.3rem">💡</span>
                <span style="font-weight:700; color:var(--warning); font-size:var(--font-base)">${t('company.possibleMatch')}</span>
                <span style="font-size:var(--font-sm); color:var(--text-secondary); margin-left:var(--space-sm); font-weight:600">${escapeHtml(m.reason || methodLabel)}${contextStr}</span>
            </div>
            <div class="entity-banner-grid">
                <!-- Left: Maps data -->
                <div style="padding:var(--space-md); background:rgba(59,130,246,0.06); border:1px solid rgba(59,130,246,0.2); border-radius:var(--radius-md)">
                    <div style="font-weight:700; font-size:var(--font-sm); color:var(--accent); margin-bottom:var(--space-sm)">${t('company.mapsDataLabel')}</div>
                    <div style="font-size:var(--font-sm); display:flex; flex-direction:column; gap:4px; color:var(--text-primary)">
                        <div><strong>${escapeHtml(co.denomination)}</strong></div>
                        ${co.adresse ? `<div style="color:var(--text-secondary)">${escapeHtml(co.adresse)}</div>` : ''}
                        ${mapsContact.phone ? `<div>📞 ${escapeHtml(mapsContact.phone)}</div>` : ''}
                        ${mapsContact.website ? `<div>🌐 ${escapeHtml(mapsContact.website)}</div>` : ''}
                    </div>
                </div>
                <!-- Right: SIRENE candidate -->
                <div style="padding:var(--space-md); background:rgba(16,185,129,0.06); border:1px solid rgba(16,185,129,0.2); border-radius:var(--radius-md)">
                    <div style="font-weight:700; font-size:var(--font-sm); color:var(--success); margin-bottom:var(--space-sm)">${t('company.sireneCandidateLabel')}</div>
                    <div style="font-size:var(--font-sm); display:flex; flex-direction:column; gap:4px; color:var(--text-primary)">
                        <div><strong>${escapeHtml(m.denomination || '')}</strong></div>
                        ${m.address ? `<div style="color:var(--text-secondary)">${escapeHtml(m.address)}</div>` : ''}
                        <div style="color:var(--text-secondary)">SIREN: ${m.siren}</div>
                        ${m.naf_code ? `<div style="color:var(--text-secondary)">NAF: ${escapeHtml(m.naf_code)}</div>` : ''}
                        ${m.ville ? `<div style="color:var(--text-secondary)">${escapeHtml(m.ville)}</div>` : ''}
                    </div>
                </div>
                <!-- Third column: Website crawl data -->
                ${_buildCrawlColumn()}
            </div>
            <div style="display:flex; gap:var(--space-md); justify-content:center">
                <button class="btn btn-primary btn-sm" id="btn-link-entity" data-maps="${co.siren}" data-target="${m.siren}" style="font-size:var(--font-sm)">${t('company.btnLinkYes')}</button>
                <button class="btn btn-secondary btn-sm" id="btn-reject-match" data-maps="${co.siren}" style="font-size:var(--font-sm); color:var(--error)">${t('company.btnLinkNo')}</button>
            </div>
        </div>`;
    }

    return '';
}

function _initEntityLinkHandlers(container, siren) {
    // Merge button
    const mergeBtn = container.querySelector('#btn-merge-entity');
    if (mergeBtn) {
        mergeBtn.addEventListener('click', async () => {
            const mapsSiren = mergeBtn.dataset.maps;
            const targetSiren = mergeBtn.dataset.target;
            if (!confirm(t('company.mergeConfirm', { maps: mapsSiren, target: targetSiren }))) return;
            mergeBtn.disabled = true;
            mergeBtn.textContent = t('company.merging');
            if (_currentBatchId) sessionStorage.setItem('fortress_merge_from_batch', _currentBatchId);
            try {
                const res = await fetch(`${window.__API_BASE || ''}/api/companies/${mapsSiren}/merge`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ target_siren: targetSiren }),
                });
                const data = await res.json();
                if (res.ok && data.redirect_to) {
                    showToast(t('company.mergeSuccess', { name: data.target_name }), 'success');
                    window.location.hash = `#/company/${data.redirect_to}`;
                } else {
                    showToast(data.error || t('company.mergeError'), 'error');
                    mergeBtn.disabled = false;
                    mergeBtn.textContent = t('company.btnMerge');
                }
            } catch (err) {
                showToast(`Erreur: ${err.message}`, 'error');
                mergeBtn.disabled = false;
                mergeBtn.textContent = t('company.btnMerge');
            }
        });
    }

    // Link button (for suggested matches)
    const linkBtn = container.querySelector('#btn-link-entity');
    if (linkBtn) {
        linkBtn.addEventListener('click', async () => {
            const mapsSiren = linkBtn.dataset.maps;
            const targetSiren = linkBtn.dataset.target;
            linkBtn.disabled = true;
            linkBtn.textContent = '⏳...';
            try {
                const res = await fetch(`${window.__API_BASE || ''}/api/companies/${mapsSiren}/link`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ target_siren: targetSiren }),
                });
                if (res.ok) {
                    showToast(t('company.linkSuccess'), 'success');
                    await renderCompany(container, siren);
                } else {
                    const data = await res.json();
                    showToast(data.error || 'Erreur', 'error');
                    linkBtn.disabled = false;
                    linkBtn.textContent = t('company.btnLinkYes');
                }
            } catch (err) {
                showToast(`Erreur: ${err.message}`, 'error');
                linkBtn.disabled = false;
                linkBtn.textContent = t('company.btnLinkYes');
            }
        });
    }

    // Reject button — permanently reject the match
    const rejectBtn = container.querySelector('#btn-reject-match');
    if (rejectBtn) {
        rejectBtn.addEventListener('click', async () => {
            const mapsSiren = rejectBtn.dataset.maps;
            rejectBtn.disabled = true;
            rejectBtn.textContent = '⏳...';
            try {
                const res = await fetch(`${window.__API_BASE || ''}/api/companies/${mapsSiren}/reject-link`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                });
                if (res.ok) {
                    showToast(t('company.rejectSuccess'), 'success');
                    const banner = container.querySelector('#entity-link-banner');
                    if (banner) banner.remove();
                } else {
                    const data = await res.json();
                    showToast(data.error || 'Erreur', 'error');
                    rejectBtn.disabled = false;
                    rejectBtn.textContent = t('company.btnLinkNo');
                }
            } catch (err) {
                showToast(`Erreur: ${err.message}`, 'error');
                rejectBtn.disabled = false;
                rejectBtn.textContent = t('company.btnLinkNo');
            }
        });
    }

    // Legacy ignore button (for backwards compatibility)
    const ignoreBtn = container.querySelector('#btn-ignore-match');
    if (ignoreBtn) {
        ignoreBtn.addEventListener('click', () => {
            const banner = container.querySelector('#entity-link-banner');
            if (banner) banner.remove();
        });
    }

    // Unlink button
    const unlinkBtn = container.querySelector('#btn-unlink-entity');
    if (unlinkBtn) {
        unlinkBtn.addEventListener('click', async () => {
            const mapsSiren = unlinkBtn.dataset.maps;
            try {
                const res = await fetch(`${window.__API_BASE || ''}/api/companies/${mapsSiren}/unlink`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                });
                if (res.ok) {
                    showToast(t('company.unlinkSuccess'), 'success');
                    await renderCompany(container, siren);
                } else {
                    const data = await res.json();
                    showToast(data.error || 'Erreur', 'error');
                }
            } catch (err) {
                showToast(`Erreur: ${err.message}`, 'error');
            }
        });
    }
}

function _buildBreadcrumb(co, tags) {
    const items = [{ label: 'Dashboard', href: '#/' }];

    // Determine origin from tags
    // MAPS entities without tags → show "Dashboard" (they came from a batch, just lost their tag)
    // Real SIREN companies without tags → show "Recherche" (found via SIRENE search)
    const isMaps = co.siren && co.siren.startsWith('MAPS');
    let parentLabel = isMaps ? 'Dashboard' : 'Recherche';
    let parentHref = isMaps ? '#/' : '#/search';

    if (tags && tags.length > 0) {
        const tag = tags[0];
        const firstTag = tag.batch_name || '';
        if (firstTag.startsWith('upload_') || firstTag.startsWith('Import: ')) {
            parentLabel = 'Import / Export';
            parentHref = '#/upload';
        } else if (firstTag.startsWith('enrich ')) {
            parentLabel = 'Recherche';
            parentHref = '#/search';
        } else if (firstTag) {
            // Batch result — link to the job using batch_id (UUID) for correct routing
            parentLabel = firstTag;
            if (tag.batch_id) {
                parentHref = `#/job/${encodeURIComponent(tag.batch_id)}`;
            } else {
                // Fallback for old rows without batch_id
                parentHref = '#/';
            }
        }
    }

    // After merge, check sessionStorage for batch context
    // (merged company is a real SIREN with no batch tags, parentHref would be '#/search')
    const fromBatch = sessionStorage.getItem('fortress_merge_from_batch');
    if (fromBatch && (!tags || tags.length === 0)) {
        parentHref = '#/job/' + encodeURIComponent(fromBatch);
        parentLabel = 'Batch';
        sessionStorage.removeItem('fortress_merge_from_batch');
    }

    items.push({ label: parentLabel, href: parentHref });
    items.push({ label: co.denomination });
    return breadcrumb(items);
}

// ── Async SIRENE match suggestions loader ────────────────────────
async function _loadSuggestedMatchesAsync(container, siren, co, mc) {
    const placeholder = container.querySelector('#entity-match-placeholder');
    if (!placeholder) return;

    try {
        const result = await getSuggestedMatches(siren);
        const matches = (result && result.matches) || [];

        if (matches.length === 0) {
            placeholder.innerHTML = `
                <span style="font-size:var(--font-sm); color:var(--text-muted)">${t('company.noSireneMatch')}</span>
            `;
            return;
        }

        // Render the best match using the same format as _buildEntityLinkBanner
        const fakeData = { suggested_matches: matches };
        const bannerHtml = _buildEntityLinkBanner(
            Object.assign({}, co, {_merged_contact: mc}),
            null,
            matches,
            null,
            _currentContacts,
            null
        );

        placeholder.outerHTML = bannerHtml || `<div id="entity-link-banner" class="card" style="background:linear-gradient(135deg, rgba(251,191,36,0.08), rgba(59,130,246,0.04)); border:1px solid rgba(251,191,36,0.3); margin-bottom:var(--space-lg); padding:var(--space-lg); border-radius:var(--radius-lg)">
            <div style="font-size:var(--font-sm); color:var(--text-muted)">${t('company.matchesLoaded')}</div>
        </div>`;

        // Re-wire the buttons that were just inserted
        _initEntityLinkHandlers(container, siren);
    } catch (_e) {
        if (placeholder && placeholder.parentNode) {
            placeholder.remove();
        }
    }
}

// ── Decision chart helpers (Phase 2) ────────────────────────────────────────

// Duration helper — formats ms into "1.2s" / "120ms" / "—"
function formatDuration(ms) {
    if (ms == null) return '—';
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(1)}s`;
}

// ── Per-step findings helpers (Brief 10) ─────────────────────────────────────

/** Safely parse detail — returns parsed object if JSON, else returns string as-is. */
function _parseDetail(detail) {
    if (!detail) return null;
    if (typeof detail === 'object') return detail;
    const s = String(detail).trim();
    if (s.startsWith('{') || s.startsWith('[')) {
        try { return JSON.parse(s); } catch (_) { /* fall through */ }
    }
    return s;
}

/** Truncate a string to maxLen characters, appending "…" if cut. */
function _truncate(str, maxLen) {
    if (!str) return '';
    const s = String(str);
    if (s.length <= maxLen) return s;
    return s.slice(0, maxLen) + '…';
}

/** Extract domain from URL for display. */
function _shortDomain(url) {
    if (!url) return '';
    try {
        const u = new URL(url.startsWith('http') ? url : 'https://' + url);
        return u.hostname.replace(/^www\./, '');
    } catch (_) {
        return String(url).replace(/^https?:\/\/(www\.)?/, '').split('/')[0];
    }
}

/** Format a chiffre_affaires number as "1,2M€" / "350k€" / etc. */
function _formatCA(val) {
    if (!val && val !== 0) return '';
    const n = Number(val);
    if (isNaN(n)) return String(val);
    if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1).replace('.', ',')}M€`;
    if (n >= 1_000) return `${Math.round(n / 1_000)}k€`;
    return `${n}€`;
}

/** Signal key → human label for the final decision row. */
function _signalDisplayName(key) {
    const map = {
        phone: '📞 tél', enseigne: '🏷 enseigne', address: '📍 adresse',
        siren_website: '🌐 SIREN site', denomination: '📋 dénomination',
        maps_cp: '📮 CP Maps', naf_section: '🔤 section NAF',
        siret_naf: '🔬 SIRET NAF', inpi_enseigne: '🏷 enseigne INPI',
        inpi_address: '📍 adresse INPI',
    };
    return map[key] || key;
}

// ── Format functions per action type ──────────────────────────────────────────

function _formatMapsFinding(detail) {
    const parts = [];
    if (typeof detail === 'object' && detail) {
        if (detail.phone) parts.push(`📞 ${detail.phone}`);
        if (detail.website) parts.push(`🌐 ${_shortDomain(detail.website)}`);
        if (detail.rating) parts.push(`⭐ ${detail.rating} (${detail.review_count || '?'} avis)`);
        if (parts.length === 0 && detail.address) parts.push(`📍 ${detail.address}`);
    } else if (typeof detail === 'string' && detail) {
        // Plain text detail — surface as-is compactly
        return _truncate(detail, 120);
    }
    return parts.slice(0, 4).join(' · ');
}

function _formatWebsiteCrawlFinding(detail) {
    const parts = [];
    if (typeof detail === 'object' && detail) {
        if (detail.emails && detail.emails.length > 0) {
            parts.push(t('companyDetail.decisionChart.findings.emailsFound', { count: detail.emails.length }));
        }
        if (detail.social) {
            const networks = Object.keys(detail.social).filter(k => detail.social[k]);
            if (networks.length > 0) parts.push(`📱 ${networks.join(', ')}`);
        }
        if (detail.mentions_legales_director) {
            parts.push(t('companyDetail.decisionChart.findings.mentionsLegalesDirector', { name: detail.mentions_legales_director }));
        }
    } else if (typeof detail === 'string' && detail) {
        return _truncate(detail, 120);
    }
    return parts.slice(0, 4).join(' · ');
}

function _formatFinancialFinding(detail) {
    const parts = [];
    if (typeof detail === 'object' && detail) {
        if (detail.chiffre_affaires) {
            parts.push(`💶 ${t('companyDetail.decisionChart.findings.ca')} ${_formatCA(detail.chiffre_affaires)}`);
        }
        if (detail.effectif) parts.push(`👥 ${detail.effectif} ${t('companyDetail.decisionChart.findings.effectifSuffix')}`);
        if (detail.last_filing_year) parts.push(`📅 ${t('companyDetail.decisionChart.findings.lastFiling')} ${detail.last_filing_year}`);
    } else if (typeof detail === 'string' && detail) {
        // e.g. "Données financières: chiffre_affaires, resultat_net, tranche_effectif"
        return _truncate(detail, 120);
    }
    return parts.slice(0, 4).join(' · ');
}

function _formatOfficersFinding(detail) {
    if (typeof detail === 'object' && detail) {
        if (detail.officers && Array.isArray(detail.officers)) {
            return detail.officers.slice(0, 3).map(o => {
                const name = o.full_name || [o.nom, o.prenom].filter(Boolean).join(' ');
                const role = o.role || 'mandataire';
                return `👤 ${name} (${role})`;
            }).join(' · ');
        }
        return '';
    }
    if (typeof detail === 'string' && detail) {
        // e.g. "2 dirigeant(s) — Registre National (API)"
        return _truncate(detail, 120);
    }
    return '';
}

function _formatAutoLinkFinding(action, detail) {
    const suffix = action.replace('auto_linked_', '');
    const methodKey = `companyDetail.decisionChart.findings.method_${suffix}`;
    const translated = t(methodKey);
    // If translation not found (returns raw key), fall back gracefully
    const methodLabel = translated !== methodKey ? translated : suffix;
    const parts = [methodLabel];

    const d = detail;
    if (typeof d === 'object' && d) {
        if (d.signals && Array.isArray(d.signals)) {
            parts.push(`✓ ${d.signals.slice(0, 3).join(', ')}`);
        }
        if (d.matched_siren) parts.push(`SIREN ${d.matched_siren}`);
    } else if (typeof d === 'string' && d) {
        // e.g. "Auto-confirmé → 977705284 (method=address, naf_status=verified, ...)"
        const sirenMatch = d.match(/→\s*(\d{9})/);
        if (sirenMatch) parts.push(`SIREN ${sirenMatch[1]}`);
        const methodMatch = d.match(/method=(\w+)/);
        if (methodMatch) parts.push(`via ${methodMatch[1]}`);
    }
    return parts.slice(0, 4).join(' · ');
}

function _formatGeminiJudgeFinding(detail) {
    const parts = [];
    const d = typeof detail === 'object' && detail ? detail : (() => {
        try { return typeof detail === 'string' ? JSON.parse(detail) : null; } catch (_) { return null; }
    })();
    if (d) {
        if (d.verdict !== undefined) {
            const vKey = `companyDetail.decisionChart.findings.geminiVerdict_${d.verdict}`;
            const vLabel = t(vKey);
            parts.push(`🤖 ${vLabel !== vKey ? vLabel : d.verdict}`);
        }
        if (d.confidence != null) parts.push(`${Math.round(Number(d.confidence) * 100)}%`);
        if (d.reasoning) parts.push(`"${_truncate(d.reasoning, 80)}"`);
    } else if (typeof detail === 'string' && detail) {
        parts.push(_truncate(detail, 120));
    }
    return parts.slice(0, 3).join(' · ');
}

function _formatGeminiQuarantineFinding(detail) {
    const d = (() => {
        if (typeof detail === 'object' && detail) return detail;
        if (typeof detail === 'string') {
            try { return JSON.parse(detail); } catch (_) { return null; }
        }
        return null;
    })();
    const parts = [];
    if (d) {
        if (d.quarantined_siren) parts.push(`⚠️ SIREN ${d.quarantined_siren} mis en quarantaine`);
        if (d.gemini_confidence != null) parts.push(`${Math.round(Number(d.gemini_confidence) * 100)}%`);
        if (d.gemini_reasoning) parts.push(`"${_truncate(d.gemini_reasoning, 80)}"`);
    } else if (typeof detail === 'string' && detail) {
        parts.push(_truncate(detail, 120));
    }
    return parts.slice(0, 3).join(' · ');
}

function _formatA2Finding(action, detail) {
    if (typeof detail === 'string' && detail) {
        if (action === 'a2_legal_name_extracted') {
            // "maps_name=... | legal_name=..." → extract legal name
            const match = detail.match(/legal_name=([^|]+)/);
            if (match) {
                return t('companyDetail.decisionChart.findings.a2LegalNameFound', { name: match[1].trim() });
            }
        }
        if (action === 'a2_match_confirmed') {
            const methodMatch = detail.match(/method=(\w+)/);
            if (methodMatch) {
                return t('companyDetail.decisionChart.findings.a2MatchConfirmed', { method: methodMatch[1] });
            }
            return t('companyDetail.decisionChart.findings.a2EntryAttempted');
        }
        return _truncate(detail, 120);
    }
    return t('companyDetail.decisionChart.findings.a2EntryAttempted');
}

function _formatRelevanceFilterFinding(detail) {
    if (detail && typeof detail === 'string') {
        return _truncate(detail, 120);
    }
    return t('companyDetail.decisionChart.findings.relevanceRejected');
}

/**
 * Returns a compact translated findings string for a given audit action.
 * detail is the raw value from the timeline (string or null).
 * Returns empty string if no extractable findings — never crashes.
 */
function getFindingForAction(action, rawDetail) {
    if (!action) return '';
    const detail = _parseDetail(rawDetail);
    if (!detail && detail !== 0) return '';

    try {
        switch (action) {
            case 'maps_lookup':
                return _formatMapsFinding(detail);
            case 'website_crawl':
                return _formatWebsiteCrawlFinding(detail);
            case 'financial_data':
                return _formatFinancialFinding(detail);
            case 'officers_found':
                return _formatOfficersFinding(detail);
            case 'a2_entry':
            case 'a2_legal_name_extracted':
            case 'a2_match_confirmed':
                return _formatA2Finding(action, detail);
            case 'auto_linked_verified':
            case 'auto_linked_expanded':
            case 'auto_linked_strong_no_filter':
            case 'auto_linked_inpi_agree':
            case 'auto_linked_chain':
            case 'auto_linked_geo_proximity':
            case 'auto_linked_cp_name_disamb':
            case 'auto_linked_cp_name_disamb_indiv':
            case 'auto_linked_siret_address_naf':
            case 'auto_linked_inpi_validated':
            case 'auto_linked_mismatch_accepted':
            case 'auto_linked_gemini_swap':
            case 'auto_linked_gemini_rescue':
            case 'auto_linked_strong':
            case 'auto_linked_mentions_legales':
            case 'auto_linked_municipal':
            case 'auto_linked_individual_match':
            case 'auto_linked_gemini_promoted':
            case 'auto_linked_gemini_promoted_retroactive':
                return _formatAutoLinkFinding(action, detail);
            case 'gemini_quarantine':
            case 'gemini_frankenstein_protected':
                return _formatGeminiQuarantineFinding(detail);
            case 'gemini_shadow_yes':
            case 'gemini_shadow_no':
            case 'gemini_shadow_ambiguous':
            case 'gemini_shadow_no_candidates':
                return _formatGeminiJudgeFinding(detail);
            case 'relevance_filter':
                return _formatRelevanceFilterFinding(detail);
            case 'siren_website_rejected_franchise_live':
                return typeof detail === 'string' ? _truncate(detail, 120) :
                    t('companyDetail.decisionChart.findings.franchiseRejected');
            default:
                return '';
        }
    } catch (_) {
        // Never crash the chart — graceful degradation
        return '';
    }
}

/**
 * Build the summary findings line for the final decision row using link_signals.
 */
function _buildDecisionSummary(linkSignals) {
    if (!linkSignals || typeof linkSignals !== 'object') return '';
    const agreed = Object.entries(linkSignals)
        .filter(([, v]) => v === true || (typeof v === 'object' && v && v.agreed === true))
        .map(([k]) => _signalDisplayName(k));
    if (agreed.length === 0) return '';
    return `${t('companyDetail.decisionChart.findings.matchedSignals')} : ${agreed.slice(0, 4).join(', ')}`;
}

function buildDecisionChart(timeline, pickerNafs, linkSignals, linkMethod, linkConfidence, rescuedBy, nafStatus, totalDurationMs) {
    // Even if timeline is empty, render the chart skeleton with the synthetic NAF-gate step + final row
    // when there's something meaningful to show (linkConfidence not null OR nafStatus meaningful)
    const hasContent = (timeline && timeline.length > 0) || (nafStatus && nafStatus !== 'maps_only') || linkConfidence;
    if (!hasContent) return '';

    // Synthetic NAF-gate step injected before timeline rows (since naf_status isn't an audit action)
    const syntheticSteps = [];
    if (nafStatus && nafStatus !== 'maps_only') {
        syntheticSteps.push({
            num: 0,
            action: `naf_gate_${nafStatus}`,
            icon: nafStatus === 'verified' ? '✅' : '⚠️',
            label: t(`companyDetail.decisionChart.step.naf_gate_${nafStatus}`),
            tooltipKey: `companyDetail.decisionChart.tooltip.naf_gate_${nafStatus}`,
            detail: pickerNafs && pickerNafs.length ? `picker: ${pickerNafs.join(', ')}` : 'no picker',
            timestamp: null,
            duration_ms: null,
            isSynthetic: true,
        });
    }

    // Map real timeline rows to step objects
    const realSteps = (timeline || []).map((row, idx) => {
        const action = row.action;
        const result = row.result;
        let icon = 'ℹ️';
        // Operational pipeline steps (distinct symbols for at-a-glance recognition)
        // These must come BEFORE prefix checks — a2_entry would otherwise match startsWith('a2_')
        if (action === 'maps_lookup') icon = '🗺️';
        else if (action === 'website_crawl') icon = '🌐';
        else if (action === 'financial_data') icon = '💰';
        else if (action === 'officers_found') icon = '👥';
        else if (action === 'a2_entry') icon = '🔍';
        // Filtering / outcome rejections
        else if (action === 'relevance_filter') icon = '❌';
        else if (action === 'siren_website_rejected_franchise_live') icon = '❌';
        else if (action === 'gemini_budget_exceeded') icon = '⚠️';
        // Existing dispatch — unchanged from prior code
        else if (action.startsWith('auto_linked_')) icon = '✅';
        else if (action.startsWith('gemini_shadow_yes')) icon = '✅';
        else if (action === 'gemini_shadow_no' || action === 'gemini_rescue_rejected_naf') icon = '❌';
        else if (action === 'gemini_quarantine' || action === 'gemini_frankenstein_protected') icon = '⚠️';
        else if (action.startsWith('pending_')) icon = '⏳';
        else if (action.startsWith('a2_')) icon = result === 'success' ? '✅' : 'ℹ️';
        else if (action === 'rollback_phase3_retroactive') icon = '↩️';

        const stepLabelKey = `companyDetail.decisionChart.step.${action}`;
        const tooltipKey = `companyDetail.decisionChart.tooltip.${action}`;

        return {
            num: syntheticSteps.length + idx,
            action,
            icon,
            label: t(stepLabelKey, { num: syntheticSteps.length + idx, detail: row.detail || '' }),
            tooltipKey,
            detail: row.detail,
            timestamp: row.timestamp,
            duration_ms: row.duration_ms,
            isSynthetic: false,
        };
    });

    const allSteps = [...syntheticSteps, ...realSteps];
    // Re-number sequential
    allSteps.forEach((s, i) => { s.num = i; });

    // Final-decision row
    let finalIcon = '✅';
    let finalKey = 'companyDetail.decisionChart.final.confirmed';
    if (linkConfidence === 'pending') { finalIcon = '⏳'; finalKey = 'companyDetail.decisionChart.final.pending'; }
    else if (linkConfidence === 'rejected') { finalIcon = '❌'; finalKey = 'companyDetail.decisionChart.final.rejected'; }
    else if (linkConfidence === null && !rescuedBy) { finalIcon = '➖'; finalKey = 'companyDetail.decisionChart.final.unmatched'; }

    // Optional Gemini sub-panel — when rescued_by is gemini_*
    const geminiPanel = (rescuedBy && rescuedBy.startsWith('gemini')) ? `
        <div class="gemini-panel">
            <h5 class="gemini-panel-title">${t('companyDetail.decisionChart.geminiPanel.title')}</h5>
            <div class="gemini-panel-row">
                <span class="gemini-panel-label">${t('companyDetail.decisionChart.geminiPanel.method')}</span>
                <span class="gemini-panel-value">${rescuedBy}</span>
            </div>
            ${linkSignals && typeof linkSignals === 'object' ? `
                <div class="gemini-panel-row">
                    <span class="gemini-panel-label">${t('companyDetail.decisionChart.geminiPanel.signals')}</span>
                    <span class="gemini-panel-value">${Object.entries(linkSignals).filter(([k, v]) => v === true).map(([k]) => k).join(', ') || '-'}</span>
                </div>
            ` : ''}
        </div>
    ` : '';

    // Total elapsed badge in header
    const elapsed = (totalDurationMs && totalDurationMs > 0)
        ? ` · ${formatDuration(totalDurationMs)}`
        : '';

    // Final decision summary line using link_signals (Change 5)
    const decisionSummaryFindings = _buildDecisionSummary(linkSignals);

    // Return just the inner content (steps + final row + Gemini panel)
    // The outer <details> wrapper is now built by the renderCompany template (Change 4)
    return `
        <div class="decision-chart-content">
            ${allSteps.map(s => {
                const durationLabel = (s.duration_ms != null && s.duration_ms > 0) ? `<span class="decision-chart-duration">${formatDuration(s.duration_ms)}</span>` : '';
                const finding = s.isSynthetic ? '' : getFindingForAction(s.action, s.detail);
                return `
                    <div class="decision-chart-step" data-step="${s.num}">
                        <span class="decision-chart-step-icon">${s.icon}</span>
                        <span class="decision-chart-step-num">${s.num}</span>
                        <span class="decision-chart-step-label">${s.label}</span>
                        ${durationLabel}
                        <span class="decision-chart-step-tooltip-trigger" tabindex="0"
                              aria-describedby="decision-tooltip-${s.num}">ⓘ</span>
                        <div id="decision-tooltip-${s.num}" class="decision-chart-step-tooltip" role="tooltip">
                            ${t(s.tooltipKey, { detail: s.detail || '' })}
                        </div>
                        ${finding ? `<div class="decision-finding-sub-line">${finding}</div>` : ''}
                    </div>
                `;
            }).join('')}
            <div class="decision-chart-step decision-chart-final">
                <span class="decision-chart-step-icon">${finalIcon}</span>
                <span class="decision-chart-step-num">→</span>
                <span class="decision-chart-step-label">${t(finalKey, { method: linkMethod || '-', rescuedBy: rescuedBy || '-' })}</span>
                ${decisionSummaryFindings ? `<div class="decision-finding-sub-line">${decisionSummaryFindings}</div>` : ''}
            </div>
            ${geminiPanel}
        </div>
    `;
}

// ── AbortController — cancel stale renders on rapid navigation ───
let _companyAbortCtrl = null;
let _currentContacts = [];
let _currentBatchId = null;

export async function renderCompany(container, siren) {
    // Cancel any in-flight render from a previous company
    if (_companyAbortCtrl) _companyAbortCtrl.abort();
    _companyAbortCtrl = new AbortController();
    const thisCtrl = _companyAbortCtrl;

    const data = await getCompany(siren);

    // If user navigated away while we were fetching, bail silently
    if (thisCtrl.signal.aborted) return;

    if (!data || data.error || data.detail === 'Not found') {
        showToast(t('company.notFound'), 'error');
        window.location.hash = '#/';
        return;
    }

    // Item Path2 (May 4) — if user landed on a MAPS URL but the entity
    // is now confirmed/pending-linked, redirect once to the canonical real-SIREN URL.
    // Stub responses (cross-workspace SIRENE-only fall-through) have data.link_confidence == null
    // — the existing ?. chain skips redirect correctly. No extra null-guard needed.
    if (siren.startsWith('MAPS')
        && data.company?.linked_siren
        && (data.link_confidence === 'confirmed' || data.link_confidence === 'pending')) {
        history.replaceState(null, '', `#/company/${data.company.linked_siren}`);
    }

    const co = data.company;
    const mc = data.merged_contact || {};
    const officers = data.officers || [];
    const tags = data.batch_tags || [];
    const linkedCo = data.linked_company;
    const suggestedMatches = data.suggested_matches || [];

    // Store module-level context for later use
    _currentContacts = data.contacts || [];
    _currentBatchId = (tags[0] && tags[0].batch_id) || null;

    // Change 5: admin check for extra_data + Change 6: SIREN dedup
    const currentUser = getCachedUser();
    const isAdmin = currentUser?.role === 'admin';

    // Change 4: decision chart status pill computation
    const _decisionPill = (() => {
        if (!co.siren || !co.siren.startsWith('MAPS')) {
            return { cls: 'blue', text: t('company.decisionChart.statusSireneNative') };
        }
        if (data.link_confidence === 'confirmed') {
            return { cls: 'green', text: t('company.decisionChart.statusConfirmed', { method: data.link_method || '-' }) };
        }
        if (data.link_confidence === 'pending') {
            return { cls: 'amber', text: t('company.decisionChart.statusPending') };
        }
        return { cls: 'grey', text: t('company.decisionChart.statusUnmatched') };
    })();

    // Change 3: RNE banner closure date
    const _rneDateStr = co.date_fermeture
        ? new Intl.DateTimeFormat(getLang() === 'fr' ? 'fr-FR' : 'en-US', { day: '2-digit', month: '2-digit', year: 'numeric' }).format(new Date(co.date_fermeture))
        : '—';

    container.innerHTML = `
        <!-- Change 1: Sticky header -->
        <div class="company-sticky-header" id="company-sticky-header">
            <div class="sticky-name">${escapeHtml(co.denomination)}</div>
            <div class="sticky-siren">${co.siren && co.siren.startsWith('MAPS') && linkedCo ? formatSiren(linkedCo.siren) : formatSiren(co.siren)}</div>
            <div class="sticky-statut">${statutBadge(co.statut)}</div>
            <button class="btn-liquid btn-sticky-rescan">${t('company.rescanShort')}</button>
        </div>

        ${_buildBreadcrumb(co, tags)}

        <!-- Change 3: RNE banner — full-width, above entity link banner -->
        ${co.etat_administratif_inpi === 'F' ? `
        <div class="company-rne-banner">
            ⚠️ ${t('company.rneBanner.title')}
            <p>${t('company.rneBanner.description', { date: _rneDateStr })}</p>
        </div>` : ''}

        ${_buildEntityLinkBanner(Object.assign({}, co, {_merged_contact: mc}), linkedCo, suggestedMatches, data.link_method, data.contacts || [], data.link_confidence)}

        ${data.matching_available && suggestedMatches.length === 0 ? `
        <div id="entity-match-placeholder" class="card" style="background:rgba(59,130,246,0.04); border:1px solid rgba(59,130,246,0.15); margin-bottom:var(--space-lg); padding:var(--space-md) var(--space-lg); display:flex; align-items:center; gap:var(--space-sm)">
            <span style="display:inline-block; width:14px; height:14px; border:2px solid rgba(255,255,255,0.15); border-top-color:var(--accent); border-radius:50%; animation:spin 1s linear infinite; flex-shrink:0"></span>
            <span style="font-size:var(--font-sm); color:var(--text-secondary)">${t('company.sireneLookup')}</span>
        </div>` : ''}

        <!-- Top Header Panel -->
        <div class="company-detail-header" style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:var(--space-lg)">
            <div class="company-detail-name-block" style="flex:1">
                <div class="company-detail-name" style="font-size:2rem; font-weight:800; letter-spacing:-0.03em; margin-bottom:var(--space-xs)">
                    ${escapeHtml(co.denomination)}
                    ${linkedCo && linkedCo.denomination !== co.denomination ? `<span style="font-size:var(--font-sm); font-weight:400; color:var(--text-secondary); margin-left:var(--space-md)">— ${escapeHtml(linkedCo.denomination)}</span>` : ''}
                </div>
                <!-- Badge row 1: Identity -->
                <div class="company-detail-siren" style="font-size:var(--font-sm); color:var(--text-secondary); display:flex; align-items:center; flex-wrap:wrap; gap:var(--space-sm); margin-top:var(--space-xs)">
                    ${co.siren && co.siren.startsWith('MAPS') && linkedCo
                        ? `<span class="glass-badge glass-badge--blue">🏢 SIREN ${formatSiren(linkedCo.siren)}<span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>SIREN</strong><br>${t('company.sirenTooltip')}<span class="info-tip-source">${t('company.sirenTooltipSource')}</span></span></span></span>
                          <span class="glass-badge" style="background:var(--bg-elevated); color:var(--text-secondary); font-size:var(--font-xs)">MAPS ${escapeHtml(co.siren)}</span>`
                        : (co.siren && co.siren.startsWith('MAPS')
                            ? `<span class="glass-badge glass-badge--blue">🏢 ${escapeHtml(co.siren)}<span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>${t('company.mapsIdTooltip')}</strong><br>${t('company.mapsIdTooltipDesc')}<span class="info-tip-source">${t('company.mapsIdTooltipSource')}</span></span></span></span>`
                            : `<span class="glass-badge glass-badge--blue">🏢 ${formatSiren(co.siren)}<span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>SIREN</strong><br>${t('company.sirenTooltip')}<span class="info-tip-source">${t('company.sirenTooltipSource')}</span></span></span></span>`)
                    }
                    ${statutBadge(co.statut)}
                    ${co.forme_juridique ? formeJuridiqueBadge(co.forme_juridique) : ''}
                </div>
                <!-- Badge row 2: Activity (Change 7: removed badgeInpiValidated/badgeGeminiPromoted; Change 3: removed RNE badge; Change 8: glass-badge only) -->
                <div class="company-detail-siren" style="font-size:var(--font-sm); color:var(--text-secondary); display:flex; align-items:center; flex-wrap:wrap; gap:var(--space-sm); margin-top:var(--space-xs)">
                    ${co.naf_code ? `<span class="glass-badge glass-badge--violet">📋 ${escapeHtml(co.naf_code)}
                        <span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>${escapeHtml(co.naf_libelle || co.naf_code)}</strong><br>${t('company.nafTooltip')}<span class="info-tip-source">${t('company.nafTooltipSource')}</span></span></span>
                    </span>` : ''}
                    ${co.naf_status ? nafStatusBadge(co.naf_status, { link_confidence: data.link_confidence, link_method: data.link_method, link_signals: data.link_signals, rescued_by: data.rescued_by }) : ''}
                    ${effectifLabel(co.tranche_effectif) ? `<span class="glass-badge glass-badge--green">👥 ${effectifLabel(co.tranche_effectif)}
                        <span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>${t('company.trancheEffectif')}</strong><br>${t('company.effectifTooltip')}<span class="info-tip-source">${t('company.effectifTooltipSource')}</span></span></span>
                    </span>` : ''}
                    ${co.departement ? `<span class="glass-badge glass-badge--cyan">📍 ${escapeHtml(co.departement)}</span>` : ''}
                    ${co.chiffre_affaires ? `<span class="glass-badge glass-badge--gold">💰 ${formatCurrency(co.chiffre_affaires)}
                        <span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>${t('company.caTooltip')}</strong><br>${t('company.caTooltipDesc')}<span class="info-tip-source">${t('company.caTooltipSource')}</span></span></span>
                    </span>` : ''}
                    ${mc.rating ? `<span class="glass-badge glass-badge--gold">⭐ ${mc.rating}<span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>${t('company.ratingTooltip')}</strong><br>${t('company.ratingTooltipDesc')}<span class="info-tip-source">${t('company.ratingTooltipSource')}</span></span></span></span>` : ''}
                    ${mc.maps_url ? `<a href="${mc.maps_url}" target="_blank" rel="noopener" class="glass-badge glass-badge--lg glass-badge--cyan" style="text-decoration:none">🗺️ Google Maps ↗</a>` : ''}
                </div>
            </div>
        </div>

        <!-- Change 2: Action bar — uses company-action-btn class (NOT action-btn — that's a
             pre-existing 28×28 icon button class at components.css:3031 that would collide). -->
        <div class="company-action-bar">
            <button class="company-action-btn company-action-btn--primary" id="rescan-btn">${t('company.actions.rescan')}</button>
            <button class="company-action-btn" id="edit-mode-btn">${t('company.actions.edit')}</button>
            <button class="company-action-btn company-action-btn--warning" id="blacklist-btn">${t('company.actions.blacklist')}</button>
            <button class="company-action-btn company-action-btn--danger" id="remove-from-batch-btn">${t('company.actions.removeFromBatch')}</button>
        </div>

        <!-- ALERT BANNER — unified mismatch/conflict alerts -->
        ${_renderAlertBanner(data.alerts || [], co.siren)}

        <!-- Change 4: Decision chart — full-width section (D3=a), default collapsed -->
        ${(() => {
            const allStepCount = (data.decision_timeline || []).length + (co.naf_status && co.naf_status !== 'maps_only' ? 1 : 0);
            const chartContent = buildDecisionChart(
                data.decision_timeline,
                data.picker_nafs,
                data.link_signals,
                data.link_method,
                data.link_confidence,
                data.rescued_by,
                co.naf_status,
                data.total_duration_ms
            );
            if (!chartContent) return '';
            return `
            <section class="company-decision-section">
                <details class="decision-chart" data-test-id="decision-chart">
                    <summary class="decision-chart-summary">
                        <span class="decision-chart-icon">🔍</span>
                        <span class="decision-chart-title">${t('company.decisionChart.titleNew')}</span>
                        <span class="decision-chart-status-pill decision-chart-status-pill--${_decisionPill.cls}">${_decisionPill.text}</span>
                        <span class="decision-chart-step-count">${allStepCount} ${t('company.decisionChart.steps')}</span>
                    </summary>
                    ${chartContent}
                </details>
            </section>`;
        })()}

        <!-- HERO SECTION: Bento Grid 60/40 -->
        <div class="crm-bento-grid" style="gap:var(--space-xl); margin-bottom:var(--space-2xl)">

            <!-- Left Column: Contact -->
            <div class="bento-col-left" style="display:flex; flex-direction:column; gap:var(--space-xl)">
                <!-- Contact Card -->
                <div class="detail-section" style="margin-bottom:0">
                    <h3 class="detail-section-title">${t('company.sectionContact')}</h3>
                    ${detailRow(t('company.labelPhone'), mc.phone
                        ? `<a href="tel:${mc.phone}" style="color:var(--success); font-weight:600">${escapeHtml(mc.phone)}</a>`
                        : unenrichedField(), sourceLabel(mc.phone_source), 'phone', mc.phone || '')}
                    ${detailRow(t('company.labelEmail'), mc.email
                        ? `<a href="mailto:${mc.email}">${escapeHtml(mc.email)}${mc.email_type ? ` <span class="glass-badge glass-badge--grey" style="font-size:var(--font-xs)">${mc.email_type}</span>` : ''}</a>`
                        : unenrichedField(), sourceLabel(mc.email_source), 'email', mc.email || '')}
                    ${detailRow(t('company.labelWebsite'), mc.website
                        ? `<a href="${mc.website.startsWith('http') ? mc.website : 'https://' + mc.website}" target="_blank" rel="noopener" style="overflow:hidden; text-overflow:ellipsis; white-space:nowrap; display:block; min-width:0">${escapeHtml(mc.website)}</a>`
                        : unenrichedField(), sourceLabel(mc.website_source), 'website', mc.website || '')}
                    ${mc.address ? detailRow(t('company.labelMapsAddress'), `<span style="color:var(--text-primary)">${escapeHtml(mc.address)}</span>`, '🗺️ Google Maps') : ''}
                    ${_buildSocialSection(mc, co.siren)}
                </div>
            </div>

            <!-- Right Column: Dirigeants + Notes -->
            <div class="bento-col-right" style="display:flex; flex-direction:column; gap:var(--space-xl)">
                <!-- Dirigeants Card (Change 14: officer source → tooltip on role badge) -->
                <div class="detail-section" style="margin-bottom:0">
                    <h3 class="detail-section-title">${t('company.sectionOfficers')}</h3>
                    ${officers.length > 0 ? officers.map(o => `
                        <div class="detail-row" style="flex-direction:column; gap:var(--space-xs); padding:var(--space-sm) 0; border-bottom:1px solid var(--border-subtle)">
                            <div style="display:flex; justify-content:space-between; align-items:center">
                                <span style="font-weight:600">
                                    ${o.civilite ? escapeHtml(o.civilite) + ' ' : ''}${escapeHtml(o.prenom ? `${o.prenom} ${o.nom}` : o.nom)}
                                </span>
                                <span class="glass-badge glass-badge--grey" style="font-size:var(--font-xs)" title="${o.source ? t('company.officerSourceTooltip', { source: sourceLabel(o.source) }) : ''}">${escapeHtml(o.role || 'Dirigeant')}</span>
                            </div>
                            ${o.email_direct || o.ligne_directe ? `
                                <div style="display:flex; gap:var(--space-md); font-size:var(--font-sm); color:var(--text-secondary)">
                                    ${o.ligne_directe ? `<span style="color:var(--success); font-weight:600">📞 ${escapeHtml(o.ligne_directe)}</span>` : ''}
                                    ${o.email_direct ? `<span>📧 ${escapeHtml(o.email_direct)}</span>` : ''}
                                </div>
                            ` : ''}
                        </div>
                    `).join('') : `
                        <div style="color:var(--text-disabled); font-style:italic; padding:var(--space-sm) 0">
                            ${t('company.noDirectors')}
                        </div>
                    `}
                </div>

                <!-- Notes Card (Change 11: "Notes CRM" → "Notes") -->
                <div class="detail-section" style="display:flex; flex-direction:column; margin-bottom:0">
                    <h3 class="detail-section-title">${t('company.sectionNotesCRMNew')}</h3>
                    <div id="notes-list" style="margin-bottom:var(--space-md)">
                        ${renderNotesDirect(data.notes || [], 3)}
                    </div>
                    <div style="display:flex; gap:var(--space-sm)">
                        <textarea id="note-input" placeholder="${t('company.notePlaceholder')}"
                            style="flex:1; min-height:44px; padding:var(--space-sm) var(--space-md);
                            background:var(--bg-input); border:1px solid var(--border-default);
                            border-radius:var(--radius-sm); color:var(--text-primary);
                            font-family:var(--font-family); font-size:var(--font-sm);
                            resize:none; outline:none"></textarea>
                        <button id="note-submit-btn" class="btn btn-primary" style="align-self:flex-end; white-space:nowrap; padding:var(--space-sm) var(--space-md)">
                            ${t('company.noteSubmit')}
                        </button>
                    </div>
                </div>
            </div>
        </div>

        <!-- BOTTOM SECTION: Reference Data (2-column symmetric grid) -->
        <div class="company-ref-grid" style="align-items:start; margin-bottom:var(--space-2xl); margin-top:var(--space-lg)">

            <!-- Left: Identité juridique + Localisation (merged) -->
            <div class="detail-section" style="margin-bottom:0">
                <h3 class="detail-section-title">${t('company.sectionIdentity')}</h3>
                ${(() => {
                    const sourceRegistre = t('company.sourceRegistre');
                    const sourceMaps = t('company.sourceMaps');
                    const sourceSystemId = t('company.sourceSystemId');
                    const isMaps = co.siren.startsWith('MAPS');
                    const isLinked = isMaps && !!linkedCo;

                    // Per-field source labels
                    // denomination: Maps for MAPS entities, SIRENE for real SIREN
                    const denomSource = isMaps ? sourceMaps : sourceRegistre;
                    // adresse: Maps for MAPS entities, SIRENE for real SIREN
                    const adresseSource = isMaps ? sourceMaps : sourceRegistre;
                    // code_postal/ville: Maps if unlinked, SIRENE if linked or real SIREN
                    const cpVilleSource = isMaps && !isLinked ? sourceMaps : sourceRegistre;
                    // siren: system ID for MAPS entities, SIRENE for real SIREN
                    const sirenSource = isMaps ? sourceSystemId : sourceRegistre;

                    // Change 6: SIREN dedup — header shows SIREN already; Identity section drops
                    // the plain SIREN row. For linked MAPS entities: only admin sees Identifiant Maps row.
                    // For unlinked MAPS entities: show single SIREN row (the MAPS ID). For real SIRENs: drop duplicate.
                    const sirenRows = isLinked
                        ? `${isAdmin ? detailRow(t('company.mapsId'), `<span style="color:var(--text-muted); font-family:var(--font-mono, monospace); font-size:var(--font-sm)">${escapeHtml(co.siren)}</span>`, sourceSystemId) : ''}`
                        : (isMaps ? `${detailRow(t('company.labelSiren'), formatSiren(co.siren), sirenSource)}` : '');

                    return `
                ${detailRow(t('company.labelDenomination'), `<span style="font-weight:700">${escapeHtml(co.denomination)}</span>`, denomSource, 'denomination', co.denomination || '')}
                ${sirenRows}
                ${detailRow(t('company.labelSiret'), formatSiret(co.siret_siege), isLinked ? sourceRegistre : sirenSource)}
                ${detailRow(t('company.labelLegalForm'), co.forme_juridique ? _formeLabel(co.forme_juridique) : '<span style="color:var(--text-disabled)">—</span>', isMaps && !isLinked ? sourceSystemId : sourceRegistre)}
                ${detailRow(t('company.labelStatut'), statutBadge(co.statut), isMaps ? sourceSystemId : sourceRegistre)}
                ${detailRow(t('company.labelCreated'), formatDate(co.date_creation), isLinked ? sourceRegistre : (isMaps ? sourceSystemId : sourceRegistre))}
                <div style="border-top:1px solid var(--border-subtle); margin:var(--space-sm) 0"></div>
                ${detailRow(t('company.labelAddress'), co.adresse || '<span style="color:var(--text-disabled)">—</span>', adresseSource, 'adresse', co.adresse || '')}
                ${detailRow(t('company.labelPostal'), co.code_postal || '<span style="color:var(--text-disabled)">—</span>', cpVilleSource, 'code_postal', co.code_postal || '')}
                ${detailRow(t('company.labelCity'), co.ville || '<span style="color:var(--text-disabled)">—</span>', cpVilleSource, 'ville', co.ville || '')}
                ${detailRow(t('company.labelDept'), co.departement ? `${escapeHtml(co.departement)}${co.region ? ` · ${escapeHtml(co.region)}` : ''}` : '<span style="color:var(--text-disabled)">—</span>', isMaps ? sourceMaps : sourceRegistre)}
                ${isLinked && data.link_confidence === 'confirmed' ? `
                <div style="border-top:1px solid var(--border-subtle); margin-top:var(--space-md); padding-top:var(--space-sm); display:flex; justify-content:flex-end">
                    <button id="btn-unlink-entity" data-maps="${co.siren}" style="background:none; border:none; color:var(--text-muted); font-size:var(--font-xs); cursor:pointer; padding:4px 8px; text-decoration:underline; opacity:0.7" onmouseover="this.style.opacity=1" onmouseout="this.style.opacity=0.7">${t('company.btnUnlink')}</button>
                </div>
                ` : ''}
                    `;
                })()}
            </div>

            <!-- Right: Financial + Activité (merged "Chiffres Clés") -->
            ${(() => {
                // Check if this is a pending MAPS entity (no confirmed SIRENE link)
                const isPendingMaps = co.siren?.startsWith('MAPS') && data.link_confidence !== 'confirmed';
                return `
            <div style="display:flex; flex-direction:column; gap:var(--space-xl)">
                <div class="detail-section" style="margin-bottom:0">
                    <h3 class="detail-section-title">${t('company.sectionFinancials')}</h3>
                    ${isPendingMaps ? `
                    <div style="padding:var(--space-md); background:rgba(245,158,11,0.1); border:1px solid rgba(245,158,11,0.3); border-radius:var(--radius-sm); color:var(--text-secondary); font-size:var(--font-sm); margin-bottom:var(--space-sm)">
                        Données SIRENE disponibles après validation du lien ci-dessus.
                    </div>
                    ` : ''}
                    ${detailRow(t('company.labelRevenue'),
                        co.chiffre_affaires
                            ? `<span style="font-weight:700; color:var(--success)">${formatCurrency(co.chiffre_affaires)}</span>`
                            : `<span style="color:var(--text-disabled); font-style:italic">${t('company.noRevenue')}</span>`,
                        t('company.srcRechercheEntreprises'))}
                    ${detailRow(t('company.labelNetResult'),
                        co.resultat_net
                            ? `<span style="font-weight:700">${formatCurrency(co.resultat_net)}</span>`
                            : `<span style="color:var(--text-disabled); font-style:italic">${t('company.noRevenue')}</span>`,
                        t('company.srcRechercheEntreprises'))}
                    ${detailRow(t('company.labelNaf'), co.naf_code ? `<strong>${escapeHtml(co.naf_code)}</strong>${co.naf_libelle ? ` <span style="color:var(--text-secondary); font-size:var(--font-sm)">— ${escapeHtml(co.naf_libelle)}</span>` : ''}` : '<span style="color:var(--text-disabled)">—</span>', t('company.sourceRegistre'))}
                    ${detailRow(t('company.labelEffectif'), effectifLabel(co.tranche_effectif) || '<span style="color:var(--text-disabled)">—</span>', t('company.sourceRegistre'))}
                </div>

                <!-- Données supplémentaires (extra_data JSONB) — Change 5: admin only -->
                ${isAdmin && co.extra_data && Object.keys(co.extra_data).length > 0 ? `
                <div class="detail-section" style="margin-bottom:0">
                    <h3 class="detail-section-title">${t('company.sectionExtraData')}</h3>
                    ${Object.entries(co.extra_data).map(([k, v]) => `
                        <div class="detail-row">
                            <span class="detail-label" style="color:var(--text-muted); flex-shrink:0; min-width:140px">${escapeHtml(k)}</span>
                            <span class="detail-value" style="word-break:break-word; overflow-wrap:anywhere">${escapeHtml(String(v))}</span>
                        </div>
                    `).join('')}
                </div>
                ` : ''}
            </div>
            `;
            })()}
        </div>

        <!-- Enrichment History (full-width) -->
        <div class="detail-section">
            <h3 class="detail-section-title">${t('company.sectionHistory')}</h3>
            <div id="enrich-history-container">
                <div class="loading" style="padding:var(--space-lg) 0"><div class="spinner"></div></div>
            </div>
        </div>
    `;

    // ── Wire up Smart Enrichment Panel ───────────────────────────
    _initEnrichmentPanel(siren, container);

    // ── Spider Crawl logic ───────────────────────────────────────
    _initSpiderCrawl(siren, container);

    // ── Merge conflict buttons (Utiliser / Ignorer) ─────────────
    container.querySelectorAll('.btn-merge-use').forEach(btn => {
        btn.addEventListener('click', async () => {
            const field = btn.dataset.field;
            const value = btn.dataset.value;
            const s = btn.dataset.siren;
            btn.disabled = true;
            btn.textContent = '⏳...';
            try {
                const res = await fetch(`${window.__API_BASE || ''}/api/companies/${s}`, {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        [field]: value,
                        _conflict_action: 'accept',
                        _conflict_field: field,
                        _conflict_rejected_value: btn.dataset.rejectedValue || '',
                        _conflict_rejected_source: btn.dataset.rejectedSource || '',
                        _conflict_chosen_source: btn.dataset.chosenSource || '',
                    }),
                });
                if (res.ok) {
                    showToast(t('company.fieldUpdated', { field }), 'success');
                    localStorage.setItem(`dismissed_conflict_${s}_${field}`, '1');
                    await renderCompany(container, siren);
                } else {
                    showToast(t('company.fieldUpdateError'), 'error');
                    btn.disabled = false;
                    btn.textContent = t('company.btnUseAlternativeShort');
                }
            } catch (err) {
                showToast(`Erreur: ${err.message}`, 'error');
                btn.disabled = false;
                btn.textContent = t('company.btnUseAlternativeShort');
            }
        });
    });
    container.querySelectorAll('.btn-merge-dismiss').forEach(btn => {
        btn.addEventListener('click', async () => {
            const field = btn.dataset.field;
            const s = btn.dataset.siren;
            localStorage.setItem(`dismissed_conflict_${s}_${field}`, '1');
            const row = btn.closest('.conflict-row');
            if (row) row.style.display = 'none';
            // Log dismissal to activity journal
            try {
                await fetch(`${window.__API_BASE || ''}/api/companies/${s}`, {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        _conflict_action: 'dismiss',
                        _conflict_field: field,
                        _conflict_rejected_value: btn.dataset.rejectedValue || '',
                        _conflict_rejected_source: btn.dataset.rejectedSource || '',
                    }),
                });
            } catch { /* non-fatal */ }
        });
    });

    // ── Alert banner buttons (accept/dismiss) ──────────────────
    container.querySelectorAll('.btn-alert-accept').forEach(btn => {
        btn.addEventListener('click', async () => {
            const field = btn.dataset.field;
            const value = btn.dataset.value;
            const s = btn.dataset.siren;
            const alertType = btn.dataset.type;
            btn.disabled = true;
            btn.textContent = '⏳...';
            try {
                const res = await fetch(`${window.__API_BASE || ''}/api/companies/${s}`, {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        [field]: value,
                        _conflict_action: 'accept',
                        _conflict_field: field,
                        _conflict_rejected_value: btn.dataset.rejectedValue || '',
                        _conflict_rejected_source: btn.dataset.rejectedSource || '',
                        _conflict_chosen_source: btn.dataset.chosenSource || '',
                    }),
                });
                if (res.ok) {
                    showToast(t('company.fieldUpdated', { field }), 'success');
                    localStorage.setItem(`dismissed_alert_${s}_${alertType}_${field}`, '1');
                    localStorage.setItem(`dismissed_conflict_${s}_${field}`, '1');
                    await renderCompany(container, siren);
                } else {
                    showToast(t('company.fieldUpdateError'), 'error');
                    btn.disabled = false;
                    btn.textContent = t('company.btnUseAlternative');
                }
            } catch (err) {
                showToast(`Erreur: ${err.message}`, 'error');
                btn.disabled = false;
                btn.textContent = t('company.btnUseAlternative');
            }
        });
    });
    container.querySelectorAll('.btn-alert-dismiss').forEach(btn => {
        btn.addEventListener('click', async () => {
            const field = btn.dataset.field;
            const s = btn.dataset.siren;
            const alertType = btn.dataset.type;
            localStorage.setItem(`dismissed_alert_${s}_${alertType}_${field}`, '1');
            const row = btn.closest('.alert-row');
            if (row) row.style.display = 'none';
            // Check if all alerts are dismissed — hide the banner header
            const banner = document.getElementById('alert-banner');
            if (banner && banner.querySelectorAll('.alert-row:not([style*="display: none"])').length === 0) {
                banner.style.display = 'none';
            }
            // Log dismissal to activity journal
            try {
                await fetch(`${window.__API_BASE || ''}/api/companies/${s}`, {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        _conflict_action: 'dismiss',
                        _conflict_field: `${alertType}:${field}`,
                        _conflict_rejected_value: btn.dataset.rejectedValue || '',
                        _conflict_rejected_source: btn.dataset.rejectedSource || '',
                    }),
                });
            } catch { /* non-fatal */ }
        });
    });

    // ── Load Unified History (Enrichments + Notes) ────────────────
    _loadEnrichHistory(siren, data.history || []);

    // ── Inline editing on detail rows ───────────────────────────
    _initInlineEditing(container, siren);
    _initEntityLinkHandlers(container, siren);

    // ── Async SIRENE match suggestions ──────────────────────────
    if (data.matching_available && suggestedMatches.length === 0) {
        _loadSuggestedMatchesAsync(container, siren, co, mc);
    }

    // ── Notes system ────────────────────────────────────────────
    _initNotes(siren);

    // ── Change 1: Sticky header IntersectionObserver ─────────────
    _initStickyHeader(container, siren);

    // ── Change 2: Action bar buttons ────────────────────────────
    _initActionBar(container, siren, co);
}

// ── Spider Crawl Logic ───────────────────────────────────────────
function _initSpiderCrawl(siren, container) {
    const spiderBtn = container.querySelector('.btn-spider-crawl');
    if (!spiderBtn) return;
    
    // Prevent double binding
    if (spiderBtn.dataset.bound) return;
    spiderBtn.dataset.bound = "true";

    spiderBtn.addEventListener('click', async (e) => {
        e.preventDefault();
        e.stopPropagation();

        const originalHtml = spiderBtn.innerHTML;
        spiderBtn.innerHTML = '<span class="enrich-spinner" style="display:inline-block; width:12px; height:12px; border:2px solid rgba(0,0,0,0.1); border-top-color:var(--accent); border-radius:50%; animation:spin 1s linear infinite"></span>';
        spiderBtn.disabled = true;

        try {
            // New direct endpoint call to bypass batch tracking
            const res = await fetch(`/api/companies/${siren}/crawl-website`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' }
            });
            const data = await res.json();
            
            if (res.ok) {
                showToast(data.message || t('company.crawlDone'), 'success');
                // Reload company to see new contacts
                await renderCompany(container, siren);
            } else {
                showToast(data.error || t('company.crawlError'), 'error');
                spiderBtn.innerHTML = originalHtml;
                spiderBtn.disabled = false;
            }
        } catch (err) {
            showToast(t('company.crawlNetworkError'), 'error');
            spiderBtn.innerHTML = originalHtml;
            spiderBtn.disabled = false;
        }
    });
}

// ── Change 1: Sticky header ───────────────────────────────────────
function _initStickyHeader(container, siren) {
    const headerEl = container.querySelector('.company-detail-header');
    const stickyEl = container.querySelector('#company-sticky-header');
    if (!headerEl || !stickyEl) return;

    const observer = new IntersectionObserver(
        ([entry]) => stickyEl.classList.toggle('visible', !entry.isIntersecting),
        { threshold: 0 }
    );
    observer.observe(headerEl);

    // Wire sticky re-scan button to the same endpoint as main rescan
    const stickyRescan = stickyEl.querySelector('.btn-sticky-rescan');
    if (stickyRescan) {
        stickyRescan.addEventListener('click', async () => {
            const originalText = stickyRescan.textContent;
            stickyRescan.disabled = true;
            stickyRescan.textContent = '⏳...';
            try {
                const res = await fetch(`${window.__API_BASE || ''}/api/companies/${siren}/crawl-website`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' }
                });
                const data = await res.json();
                if (res.ok) {
                    showToast(data.message || t('company.crawlDone'), 'success');
                    await renderCompany(container, siren);
                } else {
                    showToast(data.error || t('company.crawlError'), 'error');
                    stickyRescan.textContent = originalText;
                    stickyRescan.disabled = false;
                }
            } catch (err) {
                showToast(t('company.crawlNetworkError'), 'error');
                stickyRescan.textContent = originalText;
                stickyRescan.disabled = false;
            }
        });
    }
}

// ── Change 2: Action bar ─────────────────────────────────────────
function _initActionBar(container, siren, co) {
    // Re-scan button
    const rescanBtn = container.querySelector('#rescan-btn');
    if (rescanBtn) {
        rescanBtn.addEventListener('click', async () => {
            const originalText = rescanBtn.textContent;
            rescanBtn.disabled = true;
            rescanBtn.textContent = '⏳...';
            try {
                const res = await fetch(`${window.__API_BASE || ''}/api/companies/${siren}/crawl-website`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' }
                });
                const data = await res.json();
                if (res.ok) {
                    showToast(data.message || t('company.crawlDone'), 'success');
                    await renderCompany(container, siren);
                } else {
                    showToast(data.error || t('company.crawlError'), 'error');
                    rescanBtn.textContent = originalText;
                    rescanBtn.disabled = false;
                }
            } catch (err) {
                showToast(t('company.crawlNetworkError'), 'error');
                rescanBtn.textContent = originalText;
                rescanBtn.disabled = false;
            }
        });
    }

    // Edit button — toggle pencil visibility for affordance
    const editModeBtn = container.querySelector('#edit-mode-btn');
    if (editModeBtn) {
        editModeBtn.addEventListener('click', () => {
            const pencils = container.querySelectorAll('.btn-inline-edit');
            const isActive = editModeBtn.classList.toggle('active');
            pencils.forEach(p => { p.style.opacity = isActive ? '1' : ''; });
        });
    }

    // Blacklist button → POST /api/blacklist
    const blacklistBtn = container.querySelector('#blacklist-btn');
    if (blacklistBtn) {
        blacklistBtn.addEventListener('click', async () => {
            if (!confirm(t('company.confirmBlacklist'))) return;
            blacklistBtn.disabled = true;
            try {
                const res = await fetch(`${window.__API_BASE || ''}/api/blacklist`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ siren: siren, reason: 'Mis en liste noire depuis fiche entreprise' }),
                });
                if (res.ok) {
                    showToast(t('company.blacklist'), 'success');
                    window.location.hash = '#/';
                } else {
                    const data = await res.json();
                    showToast(data.error || 'Erreur lors du blacklistage', 'error');
                    blacklistBtn.disabled = false;
                }
            } catch (err) {
                showToast(`Erreur: ${err.message}`, 'error');
                blacklistBtn.disabled = false;
            }
        });
    }

    // Remove from batch button → DELETE /api/companies/{siren}/tags/
    const removeBtn = container.querySelector('#remove-from-batch-btn');
    if (removeBtn) {
        removeBtn.addEventListener('click', async () => {
            if (!confirm(t('company.confirmDelete'))) return;
            removeBtn.disabled = true;
            try {
                const res = await fetch(`${window.__API_BASE || ''}/api/companies/${siren}/tags/`, {
                    method: 'DELETE',
                });
                if (res.ok) {
                    showToast('Retiré de tous les batchs', 'success');
                    window.location.hash = '#/';
                } else {
                    const data = await res.json().catch(() => ({}));
                    showToast(data.error || 'Erreur lors de la suppression', 'error');
                    removeBtn.disabled = false;
                }
            } catch (err) {
                showToast(`Erreur: ${err.message}`, 'error');
                removeBtn.disabled = false;
            }
        });
    }
}

// ── Notes System ─────────────────────────────────────────────────
function _initNotes(siren) {
    const container = document.getElementById('notes-list');
    const submitBtn = document.getElementById('note-submit-btn');
    const input = document.getElementById('note-input');

    // Event delegation for delete buttons & show-all
    if (container) {
        if (container.dataset.hasNotesListener === "true") return; // prevent double attach
        container.dataset.hasNotesListener = "true";

        container.addEventListener('click', async (e) => {
            const delBtn = e.target.closest('.btn-delete-note');
            if (delBtn) {
                e.stopPropagation();
                const noteId = delBtn.dataset.noteId;
                delBtn.disabled = true;
                try {
                    const res = await deleteCompanyNote(noteId);
                    if (res._ok !== false) {
                        showToast(t('company.noteDeleted'), 'success');
                        _loadNotes(siren);
                    } else {
                        showToast(extractApiError(res), 'error');
                        delBtn.disabled = false;
                    }
                } catch {
                    showToast(t('company.noteDeleteError'), 'error');
                    delBtn.disabled = false;
                }
                return;
            }

            const showAllBtn = e.target.closest('.btn-show-all-notes');
            if (showAllBtn) {
                e.stopPropagation();
                const data = await getCompanyNotes(siren);
                const allNotes = (data && data.notes) || [];
                _showAllNotesModal(siren, allNotes);
                return;
            }
        });
    }

    // Submit button
    if (submitBtn && input) {
        submitBtn.addEventListener('click', async () => {
            const text = input.value.trim();
            if (!text) return;
            submitBtn.disabled = true;
            try {
                const res = await addCompanyNote(siren, text);
                if (res._ok !== false) {
                    input.value = '';
                    showToast(t('company.noteAdded'), 'success');
                    _loadNotes(siren);
                } else {
                    showToast(extractApiError(res), 'error');
                }
            } catch {
                showToast(t('company.noteAddError'), 'error');
            } finally {
                submitBtn.disabled = false;
            }
        });
    }
}

async function _loadNotes(siren) {
    const container = document.getElementById('notes-list');
    if (!container) return;

    const data = await getCompanyNotes(siren);
    const notes = (data && data.notes) || [];
    container.innerHTML = renderNotesDirect(notes, 2);
}

function _showAllNotesModal(siren, allNotes) {
    const modal = document.createElement('div');
    modal.className = 'notes-modal-wrapper';
    modal.innerHTML = `
        <div class="modal-overlay" style="z-index:1000; position:fixed; top:0; left:0; width:100vw; height:100vh; background:rgba(0,0,0,0.5); display:flex; justify-content:center; align-items:center">
            <div class="modal-content" style="background:var(--bg-primary); border-radius:var(--radius-md); width:90%; max-width:600px; max-height:85vh; display:flex; flex-direction:column; box-shadow:0 10px 40px rgba(0,0,0,0.4)">
                <div style="padding:var(--space-md) var(--space-lg); border-bottom:1px solid var(--border-subtle); display:flex; justify-content:space-between; align-items:center">
                    <h2 style="font-size:var(--font-lg); font-weight:700; margin:0">${t('company.modalNotesTitle', { count: allNotes.length })}</h2>
                    <button id="btn-close-notes-modal" style="background:none; border:none; font-size:1.5rem; cursor:pointer; color:var(--text-muted); padding:0">&times;</button>
                </div>
                <div style="padding:var(--space-lg); overflow-y:auto; flex:1" id="modal-notes-container">
                    ${renderNotesDirect(allNotes, null)}
                </div>
            </div>
        </div>
    `;
    document.body.appendChild(modal);
    document.body.style.overflow = 'hidden';

    const closeHandler = () => {
        modal.remove();
        document.body.style.overflow = '';
    };

    modal.querySelector('#btn-close-notes-modal').onclick = closeHandler;
    modal.querySelector('.modal-overlay').onclick = (e) => {
        if (e.target === modal.querySelector('.modal-overlay')) closeHandler();
    };

    // Modal-specific delete delegator
    const modalContainer = modal.querySelector('#modal-notes-container');
    modalContainer.addEventListener('click', async (e) => {
        const delBtn = e.target.closest('.btn-delete-note');
        if (!delBtn) return;
        
        e.stopPropagation();
        delBtn.disabled = true;
        try {
            const res = await deleteCompanyNote(delBtn.dataset.noteId);
            if (res._ok !== false) {
                showToast(t('company.noteDeleted'), 'success');
                // Refresh both the modal and the underlying page safely
                _loadNotes(siren); // update page
                closeHandler(); // close modal so it forces them to reopen if they want to see it again (cleanest state management)
            } else {
                showToast(extractApiError(res), 'error');
                delBtn.disabled = false;
            }
        } catch {
            showToast(t('company.noteDeleteError'), 'error');
            delBtn.disabled = false;
        }
    });
}

// ── Inline Edit Logic ────────────────────────────────────────────
function _initInlineEditing(container, siren) {
    // Avoid double-attaching the event listener across re-renders
    if (container.dataset.hasEditListener === "true") return;
    container.dataset.hasEditListener = "true";

    container.addEventListener('click', (e) => {
        const btn = e.target.closest('.btn-inline-edit');
        if (!btn) return;
        e.stopPropagation();

        const field = btn.dataset.field;
        const row = btn.closest('.detail-row');
        const valueCell = row.querySelector('.detail-value');
        if (!valueCell || valueCell.querySelector('.inline-edit-input')) return; // Already editing

        // Read raw value securely from the DOM, not a stale JS closure
        const currentVal = row.dataset.rawValue || '';

        // Replace value cell content with input + save/cancel
        const originalHTML = valueCell.innerHTML;
        valueCell.innerHTML = `
            <div style="display:flex; align-items:center; gap:var(--space-sm); width:100%">
                <input type="text" class="inline-edit-input" value="${escapeHtml(currentVal)}"
                    style="flex:1; padding:var(--space-sm) var(--space-md); background:var(--bg-input);
                    border:1px solid var(--accent); border-radius:var(--radius-sm); color:var(--text-primary);
                    font-family:var(--font-family); font-size:var(--font-base); outline:none"
                    placeholder="${t('company.editPlaceholder')}">
                <button class="inline-edit-save" style="background:var(--success); color:white; border:none;
                    border-radius:var(--radius-sm); padding:var(--space-xs) var(--space-sm); cursor:pointer;
                    font-size:var(--font-sm); font-weight:600">✓</button>
                <button class="inline-edit-cancel" style="background:transparent; color:var(--text-muted);
                    border:1px solid var(--border-default); border-radius:var(--radius-sm);
                    padding:var(--space-xs) var(--space-sm); cursor:pointer; font-size:var(--font-sm)">✗</button>
            </div>
        `;

        const input = valueCell.querySelector('.inline-edit-input');
        input.focus();
        // Place cursor at the end instead of selecting all
        input.setSelectionRange(input.value.length, input.value.length);

        // Cancel
        valueCell.querySelector('.inline-edit-cancel').onclick = (ev) => {
            ev.stopPropagation();
            valueCell.innerHTML = originalHTML;
        };

        // Save
        const doSave = async () => {
            const newVal = input.value.trim();
            if (newVal === currentVal) {
                valueCell.innerHTML = originalHTML;
                return;
            }
            input.disabled = true;
            try {
                const res = await updateCompany(siren, { [field]: newVal || null });
                if (res._ok !== false) {
                    // Update cell in-place — NO full page re-render
                    const displayVal = newVal || '—';
                    const editBtn = `<button class="btn-inline-edit" data-field="${field}" title="Modifier">✏️</button>`;
                    
                    // Format display based on field type
                    let formattedVal;
                    if (field === 'phone' && newVal) {
                        formattedVal = `<a href="tel:${escapeHtml(newVal)}" style="color:var(--success)">${escapeHtml(newVal)}</a>`;
                    } else if (field === 'email' && newVal) {
                        formattedVal = `<a href="mailto:${escapeHtml(newVal)}" style="color:var(--accent)">${escapeHtml(newVal)}</a>`;
                    } else if ((field === 'website' || field.startsWith('social_')) && newVal && (newVal.startsWith('http') || newVal.startsWith('www'))) {
                        const href = newVal.startsWith('http') ? newVal : `https://${newVal}`;
                        formattedVal = `<a href="${escapeHtml(href)}" target="_blank" rel="noopener">${escapeHtml(displayVal)} ↗</a>`;
                    } else {
                        formattedVal = newVal ? `<span style="color:var(--text-primary)">${escapeHtml(displayVal)}</span>` : '<span style="color:var(--text-disabled)">—</span>';
                    }
                    
                    valueCell.innerHTML = `${formattedVal} ${editBtn}`;
                    
                    // Update raw value for future edits
                    row.dataset.rawValue = newVal || '';
                    
                    // Green flash to confirm save
                    row.style.transition = 'background 0.3s';
                    row.style.background = 'rgba(34, 197, 94, 0.15)';
                    setTimeout(() => { row.style.background = ''; }, 1200);
                    
                    showToast(t('company.fieldUpdated', { field }), 'success');
                } else {
                    showToast(extractApiError(res), 'error');
                    valueCell.innerHTML = originalHTML;
                }
            } catch {
                showToast(t('company.saveError'), 'error');
                valueCell.innerHTML = originalHTML;
            }
        };

        valueCell.querySelector('.inline-edit-save').onclick = (ev) => {
            ev.stopPropagation();
            doSave();
        };
        input.addEventListener('keydown', (ev) => {
            if (ev.key === 'Enter') doSave();
            if (ev.key === 'Escape') { valueCell.innerHTML = originalHTML; }
        });
    });
}

// ── Enrichment Panel Logic ───────────────────────────────────────
function _initEnrichmentPanel(siren, container) {
    const submitBtn = document.getElementById('enrich-submit-btn');
    if (!submitBtn) return;

    // Direct synchronous website crawl — bypasses batch runner entirely
    submitBtn.addEventListener('click', async () => {
        // Loading state
        submitBtn.classList.add('loading');
        submitBtn.disabled = true;
        const textEl = submitBtn.querySelector('.enrich-submit-text');
        const originalText = textEl ? textEl.textContent : '';
        if (textEl) textEl.textContent = t('company.crawlScanning');

        try {
            const res = await fetch(`/api/companies/${siren}/crawl-website`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' }
            });
            const data = await res.json();

            if (res.ok && data.extracted && Object.keys(data.extracted).length > 0) {
                const found = Object.entries(data.extracted).map(([k, v]) => `${k}: ${v}`).join(', ');
                showToast(`✅ ${data.message} — ${found}`, 'success');
                await renderCompany(container, siren);
                return;
            } else if (res.ok) {
                showToast(data.message || t('company.crawlNoContact'), 'info');
            } else {
                showToast(data.error || t('company.crawlError'), 'error');
            }
        } catch (err) {
            showToast(t('company.crawlNetworkError'), 'error');
        } finally {
            submitBtn.classList.remove('loading');
            submitBtn.disabled = false;
            if (textEl) textEl.textContent = originalText;
        }
    });

    // ── Wire up CTA buttons from empty states ────────────────────
    document.querySelectorAll('.btn-enrich-cta').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            const targetModule = btn.dataset.enrichModule;

            // Scroll to the enrichment button since the panel container is gone
            submitBtn.scrollIntoView({ behavior: 'smooth', block: 'center' });

            // Highlight the button briefly
            submitBtn.style.transform = "scale(1.05)";
            submitBtn.style.transition = "transform 0.2s";
            setTimeout(() => submitBtn.style.transform = "scale(1)", 500);
        });
    });
}

/**
 * Render the unified alert banner for the company card.
 * Shows all data mismatches (SIRET, address, field conflicts) with accept/dismiss actions.
 */
function _renderAlertBanner(alerts, siren) {
    if (!alerts || alerts.length === 0) return '';

    // Filter out dismissed alerts
    const visible = alerts.filter(a => {
        const key = `dismissed_alert_${siren}_${a.type}_${a.field}`;
        return !localStorage.getItem(key);
    });
    if (visible.length === 0) return '';

    const getSrcLabels = () => ({
        google_maps: 'Google Maps', website_crawl: t('company.srcWebsiteCrawl'), mentions_legales: t('company.srcMentionsLegales'),
        upload: t('company.srcUpload'), manual_edit: t('company.srcManualEdit'), recherche_entreprises: t('company.srcRechercheEntreprises'),
        sirene: t('company.sourceRegistre'), inpi: 'INPI',
    });
    const srcLabel = (s) => getSrcLabels()[s] || s || '?';

    const rows = visible.map(alert => {
        const isCritical = alert.severity === 'critical';
        const borderColor = isCritical ? 'var(--error, #ef4444)' : 'var(--warning, #f59e0b)';
        const bgColor = isCritical ? 'rgba(239,68,68,0.08)' : 'rgba(255,193,7,0.06)';
        const icon = isCritical ? '🚨' : '⚠️';
        const titleColor = isCritical ? 'var(--error, #ef4444)' : 'var(--warning, #f59e0b)';

        // For data conflicts, show the two values side by side with accept/dismiss
        const hasValues = alert.current_value && alert.alt_value;
        const valuesHTML = hasValues ? `
            <div style="display:flex; gap:var(--space-md); margin-top:var(--space-sm); flex-wrap:wrap">
                <div style="flex:1; min-width:140px; padding:var(--space-xs) var(--space-sm); background:rgba(16,185,129,0.08); border-radius:var(--radius-sm); border:1px solid rgba(16,185,129,0.2)">
                    <div style="font-size:var(--font-xs); color:var(--text-muted); margin-bottom:2px">${t('company.labelCurrent')} — ${escapeHtml(srcLabel(alert.current_source))}</div>
                    <div style="color:var(--text-primary); font-weight:500; font-size:var(--font-sm); word-break:break-all">${escapeHtml(alert.current_value)}</div>
                </div>
                <div style="flex:1; min-width:140px; padding:var(--space-xs) var(--space-sm); background:${isCritical ? 'rgba(239,68,68,0.08)' : 'rgba(255,193,7,0.08)'}; border-radius:var(--radius-sm); border:1px solid ${isCritical ? 'rgba(239,68,68,0.2)' : 'rgba(255,193,7,0.2)'}">
                    <div style="font-size:var(--font-xs); color:var(--text-muted); margin-bottom:2px">${t('company.labelAlternative')} — ${escapeHtml(srcLabel(alert.alt_source))}</div>
                    <div style="color:var(--text-primary); font-weight:500; font-size:var(--font-sm); word-break:break-all">${escapeHtml(alert.alt_value)}</div>
                </div>
            </div>
        ` : '';

        const actionsHTML = hasValues ? `
            <div style="display:flex; gap:var(--space-sm); justify-content:flex-end; margin-top:var(--space-sm)">
                <button class="btn-alert-accept" data-type="${alert.type}" data-field="${alert.field}" data-siren="${siren}"
                    data-value="${escapeHtml(alert.alt_value)}" data-rejected-value="${escapeHtml(alert.current_value)}"
                    data-rejected-source="${alert.current_source || ''}" data-chosen-source="${alert.alt_source || ''}"
                    style="background:var(--success); color:#fff; border:none; padding:5px 14px; border-radius:var(--radius-sm); cursor:pointer; font-size:var(--font-xs); font-weight:600;">
                    ${t('company.btnUseAlternative')}
                </button>
                <button class="btn-alert-dismiss" data-type="${alert.type}" data-field="${alert.field}" data-siren="${siren}"
                    data-rejected-value="${escapeHtml(alert.alt_value || '')}" data-rejected-source="${alert.alt_source || ''}"
                    style="background:var(--surface-elevated); color:var(--text-secondary); border:1px solid var(--border); padding:5px 14px; border-radius:var(--radius-sm); cursor:pointer; font-size:var(--font-xs);">
                    ${t('company.btnDismiss')}
                </button>
            </div>
        ` : `
            <div style="display:flex; gap:var(--space-sm); justify-content:flex-end; margin-top:var(--space-sm)">
                <button class="btn-alert-dismiss" data-type="${alert.type}" data-field="${alert.field}" data-siren="${siren}"
                    data-rejected-value="" data-rejected-source=""
                    style="background:var(--surface-elevated); color:var(--text-secondary); border:1px solid var(--border); padding:5px 14px; border-radius:var(--radius-sm); cursor:pointer; font-size:var(--font-xs);">
                    ${t('company.btnDismiss')}
                </button>
            </div>
        `;

        return `
            <div class="alert-row" style="
                background:${bgColor}; border-left:3px solid ${borderColor};
                padding:var(--space-sm) var(--space-md); border-radius:var(--radius-sm);
            ">
                <div style="display:flex; align-items:center; gap:var(--space-sm)">
                    <span style="font-size:1.2rem">${icon}</span>
                    <div style="flex:1">
                        <div style="color:${titleColor}; font-weight:600; font-size:var(--font-sm)">${escapeHtml(alert.title)}</div>
                        <div style="color:var(--text-secondary); font-size:var(--font-xs); margin-top:2px">${escapeHtml(alert.description)}</div>
                    </div>
                </div>
                ${valuesHTML}
                ${actionsHTML}
            </div>
        `;
    }).join('');

    return `
        <div id="alert-banner" style="display:flex; flex-direction:column; gap:var(--space-sm); margin-bottom:var(--space-lg)">
            <div style="font-size:var(--font-xs); color:var(--text-muted); font-weight:600; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:var(--space-xs)">
                ${t('company.alertsTitle', { count: visible.length, plural: visible.length > 1 ? 's' : '' })}
            </div>
            ${rows}
        </div>
    `;
}


/** Show an alternate value from a different source (muted sub-row) */
function altValueRow(alt) {
    if (!alt || !alt.value) return '';
    const src = sourceLabel(alt.source) || alt.source;
    return `<div class="detail-row" style="padding-left:var(--space-lg); opacity:0.6; font-size:var(--font-sm)">
        <span class="detail-label">${t('company.alsoLabel')}</span>
        <span class="detail-value">${escapeHtml(alt.value)} <span class="provenance-badge" title="Source : ${src}">ℹ️</span></span>
    </div>`;
}

// conflictRow() removed (Change 10) — had zero call sites; alert banner is the canonical conflict UX

function formatSocial(url, label) {
    if (!url) return '<span style="color:var(--text-disabled)">—</span>';
    if (url.startsWith('http') || url.startsWith('www.')) {
        const href = url.startsWith('http') ? url : `https://${url}`;
        return `<a href="${href}" target="_blank" rel="noopener">${label} ↗</a>`;
    }
    return `<span style="color:var(--text-primary)">${escapeHtml(url)}</span>`;
}

function detailRow(label, value, source = null, editField = null, rawValue = null) {
    const tooltip = source
        ? `<span class="provenance-badge" title="Source : ${source}">ℹ️</span>`
        : '';
    const editBtn = editField
        ? `<button class="btn-inline-edit" data-field="${editField}" title="Modifier">✏️</button>`
        : '';
    const dataRaw = editField && rawValue !== null ? ` data-raw-value="${escapeHtml(rawValue)}"` : '';
    return `
        <div class="detail-row" ${editField ? `data-edit-field="${editField}"` : ''}${dataRaw}>
            <span class="detail-label">${label} ${tooltip}</span>
            <span class="detail-value">${value} ${editBtn}</span>
        </div>
    `;
}

// ── Unified History Timeline (Enrichments + Notes) ──────────────
function _loadEnrichHistory(siren, history) {
    const container = document.getElementById('enrich-history-container');
    if (!container) return;

    if (!history || history.length === 0) {
        container.innerHTML = `
            <div style="color:var(--text-muted); font-style:italic; padding:var(--space-sm) 0; font-size:var(--font-sm)">
                ${t('company.noHistoryFull')}
            </div>
        `;
        return;
    }

    // Change 9: filter notes out of Historique — they show in the Notes section
    const filteredHistory = history.filter(h => h.type !== 'note');

    if (filteredHistory.length === 0) {
        container.innerHTML = `
            <div style="color:var(--text-muted); font-style:italic; padding:var(--space-sm) 0; font-size:var(--font-sm)">
                ${t('company.noHistoryFull')}
            </div>
        `;
        return;
    }

    container.innerHTML = `
        <div style="display:flex; flex-direction:column; gap:var(--space-sm)">
            ${filteredHistory.map(h => {
                if (h.type === 'note') {
                    return `
                        <div style="display:flex; gap:var(--space-md); padding:var(--space-sm); background:var(--surface-raised); border-radius:var(--radius-sm); border-left:3px solid var(--accent)">
                            <div style="font-size:1.2rem; flex-shrink:0">📝</div>
                            <div style="flex:1; min-width:0">
                                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:2px">
                                    <span style="font-weight:700; color:var(--text-primary); font-size:var(--font-xs)">${t('company.historyNoteBy', { username: escapeHtml(h.username) })}</span>
                                    <span style="font-size:10px; color:var(--text-disabled)">${_formatTimelineDate(h.timestamp)}</span>
                                </div>
                                <div style="font-size:var(--font-sm); color:var(--text-primary); white-space:pre-wrap; word-break:break-word">${escapeHtml(h.text)}</div>
                            </div>
                        </div>
                    `;
                } else if (h.type === 'activity') {
                    const actIcons = { manual_edit: '✏️', link: '🔗', merge: '🔀', unlink: '⛓️‍💥', conflict_resolved: '✅', conflict_dismissed: '❌' };
                    const icon = actIcons[h.action] || '📌';
                    return `
                        <div style="display:flex; gap:var(--space-md); padding:var(--space-sm); background:var(--surface-raised); border-radius:var(--radius-sm); border-left:3px solid var(--warning)">
                            <div style="font-size:1.2rem; flex-shrink:0">${icon}</div>
                            <div style="flex:1; min-width:0">
                                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:2px">
                                    <span style="font-weight:700; color:var(--text-primary); font-size:var(--font-xs)">${t('company.historyActionBy', { action: escapeHtml(h.action), username: escapeHtml(h.username) })}</span>
                                    <span style="font-size:10px; color:var(--text-disabled)">${_formatTimelineDate(h.timestamp)}</span>
                                </div>
                                ${h.detail ? `<div style="font-size:var(--font-sm); color:var(--text-secondary)">${escapeHtml(h.detail)}</div>` : ''}
                            </div>
                        </div>
                    `;
                } else {
                    const ACTION_MAP = {
                        'maps_lookup': { icon: '🗺️', label: t('company.actionMapsLookup') },
                        'website_crawl': { icon: '🕸️', label: t('company.actionWebsiteCrawl') },
                        'officers_found': { icon: '👥', label: t('company.actionOfficersFound') },
                        'financial_data': { icon: '💶', label: t('company.actionFinancialData') },
                        'siren_verified': { icon: '✅', label: t('company.actionSirenVerified') },
                        'siren_mismatch': { icon: '⚠️', label: t('company.actionSirenMismatch') },
                        'manual_edit': { icon: '✏️', label: t('company.actionManualEdit') },
                        'conflict_resolved': { icon: '✅', label: t('company.actionConflictResolved') },
                        'conflict_dismissed': { icon: '❌', label: t('company.actionConflictDismissed') },
                        'link': { icon: '🔗', label: t('company.actionLink') },
                        'merge': { icon: '🔀', label: t('company.actionMerge') },
                    };
                    const act = ACTION_MAP[h.action] || { icon: '⚙️', label: h.action };
                    const isSuccess = h.result === 'success';
                    const isAlert = h.action === 'siren_mismatch' || h.result === 'fail' || h.result === 'error';
                    const color = isSuccess ? 'var(--success)' : (isAlert ? 'var(--error)' : 'var(--text-secondary)');
                    const resultLabel = isSuccess ? t('company.historySuccess') : (h.result === 'fail' || h.result === 'error' ? t('company.historyFailure') : h.result);
                    
                    return `
                        <div style="display:flex; gap:var(--space-md); padding:var(--space-sm) 0; border-bottom:1px solid var(--border-subtle)">
                            <div style="font-size:1.2rem; flex-shrink:0; opacity:${isAlert ? '1' : '0.8'}">${act.icon}</div>
                            <div style="flex:1; min-width:0">
                                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:4px">
                                    <span style="font-weight:600; color:var(--text-primary); font-size:var(--font-xs); text-transform:uppercase; letter-spacing:0.02em">
                                        ${escapeHtml(act.label)}
                                    </span>
                                    <span style="font-size:10px; color:var(--text-disabled)">${_formatTimelineDate(h.timestamp)}</span>
                                </div>
                                <div style="font-size:var(--font-sm); line-height:1.4">
                                    ${h.detail 
                                        ? `<div style="color:var(--text-primary); margin-bottom:4px; font-weight:${isAlert ? '500' : 'normal'}">${escapeHtml(h.detail)}</div>` 
                                        : ''}
                                    <div style="color:var(--text-secondary); font-size:calc(var(--font-sm) - 1px); display:flex; gap:var(--space-xs); align-items:center; flex-wrap:wrap">
                                        <span style="color:${color}; font-weight:700">${escapeHtml(resultLabel)}</span>
                                        ${h.search_query ? `<span style="opacity:0.6">•</span><span style="font-style:italic">🔍 "${escapeHtml(h.search_query)}"</span>` : ''}
                                        ${h.source_url ? `<span style="opacity:0.6">•</span><a href="${h.source_url.startsWith('http') ? h.source_url : 'https://'+h.source_url}" target="_blank" rel="noopener" style="color:var(--accent); text-decoration:none">${t('company.historyViewSource')}</a>` : ''}
                                        ${h.duration ? `<span style="opacity:0.6">•</span><span>${h.duration}ms</span>` : ''}
                                    </div>
                                </div>
                            </div>
                        </div>
                    `;
                }
            }).join('')}
        </div>
    `;
}


function _formatTimelineDate(dateStr) {
    if (!dateStr) return '—';
    try {
        const locale = getLang() === 'fr' ? 'fr-FR' : 'en-US';
        const d = new Date(dateStr);
        return d.toLocaleDateString(locale, { day: '2-digit', month: '2-digit', year: 'numeric' })
            + t('company.historyTimeAt')
            + d.toLocaleTimeString(locale, { hour: '2-digit', minute: '2-digit' });
    } catch {
        return dateStr;
    }
}
