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
    statutBadge, formeJuridiqueBadge, escapeHtml, showToast,
} from '../components.js';

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
    return Number(val).toLocaleString('fr-FR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 });
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
            <span class="liquid-text enrich-submit-text">🚀 Enrichir via site web</span>
        </button>
    `;
}

function renderNotesDirect(notes, limit = 2) {
    if (!notes || notes.length === 0) {
        return `<div style="color:var(--text-disabled); font-style:italic; padding:var(--space-sm) 0">Aucune note.</div>`;
    }
    const currentUser = getCachedUser();
    
    // Slice notes if limit provided
    const displayNotes = limit ? notes.slice(0, limit) : notes;
    const hasMore = limit && notes.length > limit;
    
    let html = displayNotes.map(n => {
        const canDelete = currentUser && (currentUser.id === n.user_id || currentUser.role === 'admin');
        const d = new Date(n.created_at);
        const dateStr = d.toLocaleDateString('fr-FR', { day: '2-digit', month: '2-digit', year: '2-digit' }).replace(/\//g, '.');
        const timeStr = d.toLocaleTimeString('fr-FR', { hour: '2-digit', minute: '2-digit' });
        
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
                        <span>${dateStr}</span>
                        <span>${timeStr}</span>
                    </div>
                </div>
                ${canDelete ? `<button class="btn-delete-note" data-note-id="${n.id}" style="background:none; border:none; color:var(--text-disabled); cursor:pointer; padding:4px" title="Supprimer">🗑️</button>` : ''}
            </div>
            <div style="font-size:var(--font-sm); white-space:pre-wrap; color:var(--text-primary); line-height:1.5">${escapeHtml(n.text)}</div>
        </div>
        `;
    }).join('');

    if (hasMore) {
        html += `
            <button class="btn-show-all-notes" style="width:100%; padding:var(--space-sm); margin-top:var(--space-xs); background:transparent; border:1px solid var(--border-subtle); border-radius:var(--radius-sm); color:var(--text-muted); font-size:var(--font-xs); font-weight:600; cursor:pointer; transition:all 0.2s">
                Voir tout l'historique (${notes.length} notes)
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
                <span class="detail-label" style="color:var(--text-disabled)">Réseaux sociaux</span>
                <span class="detail-value" style="color:var(--text-disabled); font-style:italic">Aucun</span>
            </div>`;
    }

    return filled.map(s =>
        detailRow(s.label, formatSocial(mc[s.key], s.linkLabel), sourceLabel(mc[s.key + '_source']), s.key, mc[s.key] || '')
    ).join('');
}

// ── Context-aware breadcrumb ─────────────────────────────────────
// ── Entity Link Banner ───────────────────────────────────────────────

function _buildEntityLinkBanner(co, linkedCo, suggestedMatches, linkMethod) {
    if (!co.siren || !co.siren.startsWith('MAPS')) return '';

    const _linkReasonLabel = (method) => {
        if (method === 'enseigne') return 'Même nom commercial (enseigne)';
        if (method === 'phone') return 'Même numéro de téléphone';
        if (method === 'address') return 'Même adresse détectée';
        if (method === 'siren_website') return 'SIREN trouvé sur le site web';
        if (method === 'fuzzy_name') return 'Nom similaire détecté';
        if (method === 'manual') return 'Lien établi manuellement';
        return 'Correspondance automatique';
    };

    if (linkedCo) {
        // Confirmed link — side-by-side comparison (same layout as suggested match, green theme)
        const reasonText = _linkReasonLabel(linkMethod);
        const mc = co._merged_contact || {};
        return `
        <div id="entity-link-banner" class="card" style="background:linear-gradient(135deg, rgba(16,185,129,0.08), rgba(59,130,246,0.04)); border:1px solid rgba(16,185,129,0.3); margin-bottom:var(--space-lg); padding:var(--space-lg); border-radius:var(--radius-lg)">
            <div style="display:flex; align-items:center; gap:var(--space-sm); margin-bottom:var(--space-md)">
                <span style="font-size:1.3rem">🔗</span>
                <span style="font-weight:700; color:var(--success); font-size:var(--font-base)">Lié à ${escapeHtml(linkedCo.denomination || '')}</span>
                <span style="font-size:var(--font-sm); color:var(--text-secondary); margin-left:var(--space-sm); font-weight:600">${reasonText}</span>
            </div>
            <div class="entity-banner-grid">
                <!-- Left: Maps data -->
                <div style="padding:var(--space-md); background:rgba(59,130,246,0.06); border:1px solid rgba(59,130,246,0.2); border-radius:var(--radius-md)">
                    <div style="font-weight:700; font-size:var(--font-sm); color:var(--accent); margin-bottom:var(--space-sm)">🗺️ Données Maps</div>
                    <div style="font-size:var(--font-sm); display:flex; flex-direction:column; gap:4px; color:var(--text-primary)">
                        <div><strong>${escapeHtml(co.denomination)}</strong></div>
                        ${co.adresse ? `<div style="color:var(--text-secondary)">${escapeHtml(co.adresse)}</div>` : ''}
                        ${mc.phone ? `<div>📞 ${escapeHtml(mc.phone)}</div>` : ''}
                        ${mc.website ? `<div>🌐 ${escapeHtml(mc.website)}</div>` : ''}
                    </div>
                </div>
                <!-- Right: SIRENE data -->
                <div style="padding:var(--space-md); background:rgba(16,185,129,0.06); border:1px solid rgba(16,185,129,0.2); border-radius:var(--radius-md)">
                    <div style="font-weight:700; font-size:var(--font-sm); color:var(--success); margin-bottom:var(--space-sm)">🏢 Données SIRENE</div>
                    <div style="font-size:var(--font-sm); display:flex; flex-direction:column; gap:4px; color:var(--text-primary)">
                        <div><strong>${escapeHtml(linkedCo.denomination || '')}</strong></div>
                        ${linkedCo.adresse ? `<div style="color:var(--text-secondary)">${escapeHtml(linkedCo.adresse)}</div>` : ''}
                        <div style="color:var(--text-secondary)">SIREN: ${linkedCo.siren}</div>
                        ${linkedCo.naf_code ? `<div style="color:var(--text-secondary)">NAF: ${escapeHtml(linkedCo.naf_code)}</div>` : ''}
                        ${linkedCo.ville ? `<div style="color:var(--text-secondary)">${escapeHtml(linkedCo.ville)}</div>` : ''}
                    </div>
                </div>
            </div>
            <div style="display:flex; gap:var(--space-md); justify-content:center">
                <button class="btn btn-primary btn-sm" id="btn-merge-entity" data-maps="${co.siren}" data-target="${linkedCo.siren}" style="font-size:var(--font-sm)">🔀 Fusionner</button>
                <button class="btn btn-secondary btn-sm" id="btn-unlink-entity" data-maps="${co.siren}" style="font-size:var(--font-sm); opacity:0.7">Dissocier</button>
            </div>
        </div>`;
    }

    if (suggestedMatches.length > 0) {
        const m = suggestedMatches[0]; // Show the best match
        const methodLabel = m.method === 'address' ? 'Même adresse'
            : m.method === 'fuzzy_name' ? 'Nom similaire'
            : m.method === 'phone' ? 'Même téléphone'
            : m.method === 'enseigne' ? 'Même enseigne'
            : m.method === 'siren_website' ? 'SIREN sur le site'
            : m.method;

        // Build additional context hints
        const hints = [];
        if (m.ville && co.adresse && co.adresse.toLowerCase().includes(m.ville.toLowerCase())) {
            hints.push('même ville');
        }
        if (m.address && co.adresse) {
            const mapsWords = co.adresse.toLowerCase().split(/[\s,]+/).filter(w => w.length > 3);
            const sireneWords = m.address.toLowerCase().split(/[\s,]+/).filter(w => w.length > 3);
            const commonWords = mapsWords.filter(w => sireneWords.includes(w));
            if (commonWords.length > 0) {
                hints.push('adresse similaire');
            }
        }
        const contextStr = hints.length > 0 ? ` · ${hints.join(' · ')}` : '';

        const mc = co._merged_contact || {};
        return `
        <div id="entity-link-banner" class="card" style="background:linear-gradient(135deg, rgba(251,191,36,0.08), rgba(59,130,246,0.04)); border:1px solid rgba(251,191,36,0.3); margin-bottom:var(--space-lg); padding:var(--space-lg); border-radius:var(--radius-lg)">
            <div style="display:flex; align-items:center; gap:var(--space-sm); margin-bottom:var(--space-md)">
                <span style="font-size:1.3rem">💡</span>
                <span style="font-weight:700; color:var(--warning); font-size:var(--font-base)">Correspondance possible</span>
                <span style="font-size:var(--font-sm); color:var(--text-secondary); margin-left:var(--space-sm); font-weight:600">${escapeHtml(m.reason || methodLabel)}${contextStr}</span>
            </div>
            <div class="entity-banner-grid">
                <!-- Left: Maps data -->
                <div style="padding:var(--space-md); background:rgba(59,130,246,0.06); border:1px solid rgba(59,130,246,0.2); border-radius:var(--radius-md)">
                    <div style="font-weight:700; font-size:var(--font-sm); color:var(--accent); margin-bottom:var(--space-sm)">🗺️ Données Maps</div>
                    <div style="font-size:var(--font-sm); display:flex; flex-direction:column; gap:4px; color:var(--text-primary)">
                        <div><strong>${escapeHtml(co.denomination)}</strong></div>
                        ${co.adresse ? `<div style="color:var(--text-secondary)">${escapeHtml(co.adresse)}</div>` : ''}
                        ${mc.phone ? `<div>📞 ${escapeHtml(mc.phone)}</div>` : ''}
                        ${mc.website ? `<div>🌐 ${escapeHtml(mc.website)}</div>` : ''}
                    </div>
                </div>
                <!-- Right: SIRENE candidate -->
                <div style="padding:var(--space-md); background:rgba(16,185,129,0.06); border:1px solid rgba(16,185,129,0.2); border-radius:var(--radius-md)">
                    <div style="font-weight:700; font-size:var(--font-sm); color:var(--success); margin-bottom:var(--space-sm)">🏢 Candidat SIRENE</div>
                    <div style="font-size:var(--font-sm); display:flex; flex-direction:column; gap:4px; color:var(--text-primary)">
                        <div><strong>${escapeHtml(m.denomination || '')}</strong></div>
                        ${m.address ? `<div style="color:var(--text-secondary)">${escapeHtml(m.address)}</div>` : ''}
                        <div style="color:var(--text-secondary)">SIREN: ${m.siren}</div>
                        ${m.naf_code ? `<div style="color:var(--text-secondary)">NAF: ${escapeHtml(m.naf_code)}</div>` : ''}
                        ${m.ville ? `<div style="color:var(--text-secondary)">${escapeHtml(m.ville)}</div>` : ''}
                    </div>
                </div>
            </div>
            <div style="display:flex; gap:var(--space-md); justify-content:center">
                <button class="btn btn-primary btn-sm" id="btn-link-entity" data-maps="${co.siren}" data-target="${m.siren}" style="font-size:var(--font-sm)">Oui, c'est la même</button>
                <button class="btn btn-secondary btn-sm" id="btn-reject-match" data-maps="${co.siren}" style="font-size:var(--font-sm); color:var(--error)">Non, garder séparé</button>
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
            if (!confirm(`Fusionner ${mapsSiren} dans ${targetSiren} ? Cette action est irréversible.`)) return;
            mergeBtn.disabled = true;
            mergeBtn.textContent = '⏳ Fusion...';
            try {
                const res = await fetch(`${window.__API_BASE || ''}/api/companies/${mapsSiren}/merge`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ target_siren: targetSiren }),
                });
                const data = await res.json();
                if (res.ok && data.redirect_to) {
                    showToast(`✅ Fusionné avec ${data.target_name}`, 'success');
                    window.location.hash = `#/company/${data.redirect_to}`;
                } else {
                    showToast(data.error || 'Erreur lors de la fusion', 'error');
                    mergeBtn.disabled = false;
                    mergeBtn.textContent = '🔀 Fusionner';
                }
            } catch (err) {
                showToast(`Erreur: ${err.message}`, 'error');
                mergeBtn.disabled = false;
                mergeBtn.textContent = '🔀 Fusionner';
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
                    showToast('Entités liées — enrichissement INPI en cours...', 'success');
                    await renderCompany(container, siren);
                } else {
                    const data = await res.json();
                    showToast(data.error || 'Erreur', 'error');
                    linkBtn.disabled = false;
                    linkBtn.textContent = "Oui, c'est la même";
                }
            } catch (err) {
                showToast(`Erreur: ${err.message}`, 'error');
                linkBtn.disabled = false;
                linkBtn.textContent = "Oui, c'est la même";
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
                    showToast('Correspondance rejetée', 'success');
                    const banner = container.querySelector('#entity-link-banner');
                    if (banner) banner.remove();
                } else {
                    const data = await res.json();
                    showToast(data.error || 'Erreur', 'error');
                    rejectBtn.disabled = false;
                    rejectBtn.textContent = 'Non, garder séparé';
                }
            } catch (err) {
                showToast(`Erreur: ${err.message}`, 'error');
                rejectBtn.disabled = false;
                rejectBtn.textContent = 'Non, garder séparé';
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
                    showToast('Lien supprimé', 'success');
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
    let parentLabel = 'Recherche';
    let parentHref = '#/search';

    if (tags && tags.length > 0) {
        const firstTag = tags[0].batch_name || '';
        if (firstTag.startsWith('upload_') || firstTag.startsWith('Import: ')) {
            parentLabel = 'Import / Export';
            parentHref = '#/upload';
        } else if (firstTag.startsWith('enrich ')) {
            parentLabel = 'Recherche';
            parentHref = '#/search';
        } else if (firstTag) {
            // Batch result — link to the job
            parentLabel = firstTag;
            parentHref = `#/job/${encodeURIComponent(firstTag)}`;
        }
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
                <span style="font-size:var(--font-sm); color:var(--text-muted)">Aucune correspondance trouvée dans la base SIRENE.</span>
            `;
            return;
        }

        // Render the best match using the same format as _buildEntityLinkBanner
        const fakeData = { suggested_matches: matches };
        const bannerHtml = _buildEntityLinkBanner(
            Object.assign({}, co, {_merged_contact: mc}),
            null,
            matches,
            null
        );

        placeholder.outerHTML = bannerHtml || `<div id="entity-link-banner" class="card" style="background:linear-gradient(135deg, rgba(251,191,36,0.08), rgba(59,130,246,0.04)); border:1px solid rgba(251,191,36,0.3); margin-bottom:var(--space-lg); padding:var(--space-lg); border-radius:var(--radius-lg)">
            <div style="font-size:var(--font-sm); color:var(--text-muted)">Correspondances chargées.</div>
        </div>`;

        // Re-wire the buttons that were just inserted
        _initEntityLinkHandlers(container, siren);
    } catch (_e) {
        if (placeholder && placeholder.parentNode) {
            placeholder.remove();
        }
    }
}

// ── AbortController — cancel stale renders on rapid navigation ───
let _companyAbortCtrl = null;

export async function renderCompany(container, siren) {
    // Cancel any in-flight render from a previous company
    if (_companyAbortCtrl) _companyAbortCtrl.abort();
    _companyAbortCtrl = new AbortController();
    const thisCtrl = _companyAbortCtrl;

    const data = await getCompany(siren);

    // If user navigated away while we were fetching, bail silently
    if (thisCtrl.signal.aborted) return;

    if (!data || data.error || data.detail === 'Not found') {
        showToast('Entreprise introuvable — elle a peut-être été supprimée ou fusionnée', 'error');
        window.location.hash = '#/';
        return;
    }

    const co = data.company;
    const mc = data.merged_contact || {};
    const officers = data.officers || [];
    const tags = data.batch_tags || [];
    const linkedCo = data.linked_company;
    const suggestedMatches = data.suggested_matches || [];


    container.innerHTML = `
        ${_buildBreadcrumb(co, tags)}

        ${_buildEntityLinkBanner(Object.assign({}, co, {_merged_contact: mc}), linkedCo, suggestedMatches, data.link_method)}

        ${data.matching_available && suggestedMatches.length === 0 ? `
        <div id="entity-match-placeholder" class="card" style="background:rgba(59,130,246,0.04); border:1px solid rgba(59,130,246,0.15); margin-bottom:var(--space-lg); padding:var(--space-md) var(--space-lg); display:flex; align-items:center; gap:var(--space-sm)">
            <span style="display:inline-block; width:14px; height:14px; border:2px solid rgba(255,255,255,0.15); border-top-color:var(--accent); border-radius:50%; animation:spin 1s linear infinite; flex-shrink:0"></span>
            <span style="font-size:var(--font-sm); color:var(--text-secondary)">Recherche de correspondances SIRENE...</span>
        </div>` : ''}

        <!-- Top Header Panel -->
        <div class="company-detail-header" style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:var(--space-2xl)">
            <div class="company-detail-name-block">
                <div class="company-detail-name" style="font-size:2rem; font-weight:800; letter-spacing:-0.03em; margin-bottom:var(--space-xs)">
                    ${escapeHtml(co.denomination)}
                    ${linkedCo && linkedCo.denomination !== co.denomination ? `<span style="font-size:var(--font-sm); font-weight:400; color:var(--text-secondary); margin-left:var(--space-md)">— ${escapeHtml(linkedCo.denomination)}</span>` : ''}
                </div>
                <!-- Badge row 1: Identity -->
                <div class="company-detail-siren" style="font-size:var(--font-sm); color:var(--text-secondary); display:flex; align-items:center; flex-wrap:wrap; gap:6px; margin-top:var(--space-xs)">
                    ${co.siren && co.siren.startsWith('MAPS') && linkedCo
                        ? `<span class="glass-badge glass-badge--blue">🏢 SIREN\u00a0${formatSiren(linkedCo.siren)}<span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>SIREN</strong><br>Identifiant unique à 9 chiffres attribué par l'INSEE à chaque entreprise en France.<span class="info-tip-source">Source : Registre SIRENE</span></span></span></span>
                          <span class="glass-badge" style="background:var(--bg-elevated); color:var(--text-secondary); font-size:var(--font-xs)">MAPS\u00a0${escapeHtml(co.siren)}</span>`
                        : `<span class="glass-badge glass-badge--blue">🏢 ${formatSiren(co.siren)}<span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>SIREN</strong><br>Identifiant unique à 9 chiffres attribué par l'INSEE à chaque entreprise en France.<span class="info-tip-source">Source : Registre SIRENE</span></span></span></span>`
                    }
                    ${statutBadge(co.statut)}
                    ${co.forme_juridique ? formeJuridiqueBadge(co.forme_juridique) : ''}
                </div>
                <!-- Badge row 2: Activity -->
                <div class="company-detail-siren" style="font-size:var(--font-sm); color:var(--text-secondary); display:flex; align-items:center; flex-wrap:wrap; gap:6px; margin-top:var(--space-xs)">
                    ${co.naf_code ? `<span class="glass-badge glass-badge--violet">📋 ${escapeHtml(co.naf_code)}
                        <span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>${escapeHtml(co.naf_libelle || co.naf_code)}</strong><br>Code d'activité principale (NAF/APE) classifiant le secteur économique de l'entreprise.<span class="info-tip-source">Source : INSEE</span></span></span>
                    </span>` : ''}
                    ${effectifLabel(co.tranche_effectif) ? `<span class="glass-badge glass-badge--green">👥 ${effectifLabel(co.tranche_effectif)}
                        <span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>Tranche d'effectif</strong><br>Nombre de salariés déclarés par l'entreprise selon la nomenclature INSEE.<span class="info-tip-source">Source : INSEE</span></span></span>
                    </span>` : ''}
                    ${co.departement ? `<span class="glass-badge glass-badge--cyan">📍 ${escapeHtml(co.departement)}</span>` : ''}
                    ${co.chiffre_affaires ? `<span class="glass-badge glass-badge--gold">💰 ${formatCurrency(co.chiffre_affaires)}
                        <span class="info-tip"><span class="info-tip-icon">i</span><span class="info-tip-card"><strong>Chiffre d'affaires</strong><br>Revenu annuel déclaré de l'entreprise.<span class="info-tip-source">Source : Comptes annuels</span></span></span>
                    </span>` : ''}
                    ${mc.rating ? `<span class="glass-badge glass-badge--gold">⭐ ${mc.rating}</span>` : ''}
                    ${mc.maps_url ? `<a href="${mc.maps_url}" target="_blank" rel="noopener" class="glass-badge glass-badge--lg glass-badge--cyan" style="text-decoration:none">🗺️ Google Maps ↗</a>` : ''}
                </div>
            </div>
            <div class="company-detail-actions">
                ${enrichmentPanelHTML()}
            </div>
        </div>

        <!-- ALERT BANNER — unified mismatch/conflict alerts -->
        ${_renderAlertBanner(data.alerts || [], co.siren)}

        <!-- HERO SECTION: Bento Grid 60/40 -->
        <div class="crm-bento-grid" style="gap:var(--space-xl); margin-bottom:var(--space-2xl)">

            <!-- Left Column: Contact -->
            <div class="bento-col-left" style="display:flex; flex-direction:column; gap:var(--space-xl)">
                <!-- Contact Card -->
                <div class="detail-section" style="margin-bottom:0">
                    <h3 class="detail-section-title">📞 Contact</h3>
                    ${detailRow('Téléphone', mc.phone
                        ? `<a href="tel:${mc.phone}" style="color:var(--success); font-weight:600">${escapeHtml(mc.phone)}</a>`
                        : unenrichedField(), sourceLabel(mc.phone_source), 'phone', mc.phone || '')}
                    ${detailRow('Email', mc.email
                        ? `<a href="mailto:${mc.email}">${escapeHtml(mc.email)}${mc.email_type ? ` <span class="badge badge-muted">${mc.email_type}</span>` : ''}</a>`
                        : unenrichedField(), sourceLabel(mc.email_source), 'email', mc.email || '')}
                    ${detailRow('Site web', mc.website
                        ? `<a href="${mc.website.startsWith('http') ? mc.website : 'https://' + mc.website}" target="_blank" rel="noopener" style="overflow:hidden; text-overflow:ellipsis; white-space:nowrap; display:block; min-width:0">${escapeHtml(mc.website)}</a>`
                        : unenrichedField(), sourceLabel(mc.website_source), 'website', mc.website || '')}
                    ${mc.address ? detailRow('Adresse Maps', `<span style="color:var(--text-primary)">${escapeHtml(mc.address)}</span>`, '🗺️ Google Maps') : ''}
                    ${_buildSocialSection(mc, co.siren)}
                </div>
            </div>

            <!-- Right Column: Dirigeants + Notes -->
            <div class="bento-col-right" style="display:flex; flex-direction:column; gap:var(--space-xl)">
                <!-- Dirigeants Card -->
                <div class="detail-section" style="margin-bottom:0">
                    <h3 class="detail-section-title">👤 Dirigeants</h3>
                    ${officers.length > 0 ? officers.map(o => `
                        <div class="detail-row" style="flex-direction:column; gap:var(--space-xs); padding:var(--space-sm) 0; border-bottom:1px solid var(--border-subtle)">
                            <div style="display:flex; justify-content:space-between; align-items:center">
                                <span style="font-weight:600">
                                    ${o.civilite ? escapeHtml(o.civilite) + ' ' : ''}${escapeHtml(o.prenom ? `${o.prenom} ${o.nom}` : o.nom)}
                                </span>
                                <span class="badge" style="font-size:var(--font-xs)">${escapeHtml(o.role || 'Dirigeant')}</span>
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
                            Aucun dirigeant référencé
                        </div>
                    `}
                </div>

                <!-- Notes Card -->
                <div class="detail-section" style="display:flex; flex-direction:column; margin-bottom:0">
                    <h3 class="detail-section-title">📝 Notes CRM</h3>
                    <div id="notes-list" style="margin-bottom:var(--space-md)">
                        ${renderNotesDirect(data.notes || [], 3)}
                    </div>
                    <div style="display:flex; gap:var(--space-sm)">
                        <textarea id="note-input" placeholder="Ajouter une note…"
                            style="flex:1; min-height:44px; padding:var(--space-sm) var(--space-md);
                            background:var(--bg-input); border:1px solid var(--border-default);
                            border-radius:var(--radius-sm); color:var(--text-primary);
                            font-family:var(--font-family); font-size:var(--font-sm);
                            resize:none; outline:none"></textarea>
                        <button id="note-submit-btn" class="btn btn-primary" style="align-self:flex-end; white-space:nowrap; padding:var(--space-sm) var(--space-md)">
                            Ajouter
                        </button>
                    </div>
                </div>
            </div>
        </div>

        <!-- BOTTOM SECTION: Reference Data (2-column symmetric grid) -->
        <div class="company-ref-grid" style="align-items:start; margin-bottom:var(--space-2xl); margin-top:var(--space-lg)">

            <!-- Left: Identité juridique + Localisation (merged) -->
            <div class="detail-section" style="margin-bottom:0">
                <h3 class="detail-section-title">🏛️ Identité</h3>
                ${(() => {
                    let identitySource;
                    if (!co.siren.startsWith('MAPS')) {
                        identitySource = 'Registre SIRENE';
                    } else if (!linkedCo) {
                        identitySource = 'Google Maps';
                    } else if (data.link_method === 'manual') {
                        identitySource = 'SIRENE (confirmé manuellement)';
                    } else {
                        const methodLabels = {
                            'enseigne': 'même enseigne',
                            'phone': 'même téléphone',
                            'address': 'même adresse',
                            'siren_website': 'SIREN trouvé sur le site'
                        };
                        const reason = methodLabels[data.link_method] || 'correspondance automatique';
                        identitySource = `SIRENE (auto : ${reason})`;
                    }
                    return `
                ${detailRow('Dénomination', `<span style="font-weight:700">${escapeHtml(co.denomination)}</span>`, identitySource, 'denomination', co.denomination || '')}
                ${detailRow('SIREN', formatSiren(co.siren), 'Registre SIRENE')}
                ${detailRow('SIRET siège', formatSiret(co.siret_siege), 'Registre SIRENE')}
                ${detailRow('Forme juridique', co.forme_juridique ? _formeLabel(co.forme_juridique) : '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE')}
                ${detailRow('Statut', statutBadge(co.statut), 'Registre SIRENE')}
                ${detailRow('Date création', formatDate(co.date_creation), 'Registre SIRENE')}
                <div style="border-top:1px solid var(--border-subtle); margin:var(--space-sm) 0"></div>
                ${detailRow('Adresse', co.adresse || '<span style="color:var(--text-disabled)">—</span>', identitySource, 'adresse', co.adresse || '')}
                ${detailRow('Code postal', co.code_postal || '<span style="color:var(--text-disabled)">—</span>', identitySource, 'code_postal', co.code_postal || '')}
                ${detailRow('Ville', co.ville || '<span style="color:var(--text-disabled)">—</span>', identitySource, 'ville', co.ville || '')}
                ${detailRow('Département', co.departement ? `${escapeHtml(co.departement)}${co.region ? ` · ${escapeHtml(co.region)}` : ''}` : '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE')}
                    `;
                })()}
            </div>

            <!-- Right: Financial + Activité (merged "Chiffres Clés") -->
            <div style="display:flex; flex-direction:column; gap:var(--space-xl)">
                <div class="detail-section" style="margin-bottom:0">
                    <h3 class="detail-section-title">💰 Chiffres clés</h3>
                    ${detailRow("Chiffre d'affaires",
                        co.chiffre_affaires
                            ? `<span style="font-weight:700; color:var(--success)">${formatCurrency(co.chiffre_affaires)}</span>`
                            : '<span style="color:var(--text-disabled); font-style:italic">Non disponible</span>')}
                    ${detailRow('Résultat net',
                        co.resultat_net
                            ? `<span style="font-weight:700">${formatCurrency(co.resultat_net)}</span>`
                            : '<span style="color:var(--text-disabled); font-style:italic">Non disponible</span>')}
                    ${detailRow('Code NAF', co.naf_code ? `<strong>${escapeHtml(co.naf_code)}</strong>${co.naf_libelle ? ` <span style="color:var(--text-secondary); font-size:var(--font-sm)">— ${escapeHtml(co.naf_libelle)}</span>` : ''}` : '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE')}
                    ${detailRow('Effectif', effectifLabel(co.tranche_effectif) || '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE')}
                </div>

                <!-- Données supplémentaires (extra_data JSONB) -->
                ${co.extra_data && Object.keys(co.extra_data).length > 0 ? `
                <div class="detail-section" style="margin-bottom:0">
                    <h3 class="detail-section-title">📋 Données supplémentaires</h3>
                    ${Object.entries(co.extra_data).map(([k, v]) => `
                        <div class="detail-row">
                            <span class="detail-label" style="color:var(--text-muted); flex-shrink:0; min-width:140px">${escapeHtml(k)}</span>
                            <span class="detail-value" style="word-break:break-word; overflow-wrap:anywhere">${escapeHtml(String(v))}</span>
                        </div>
                    `).join('')}
                </div>
                ` : ''}
            </div>
        </div>

        <!-- Enrichment History (full-width) -->
        <div class="detail-section">
            <h3 class="detail-section-title">📜 Historique d'enrichissement</h3>
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
                    showToast(`✅ ${field} mis à jour`, 'success');
                    localStorage.setItem(`dismissed_conflict_${s}_${field}`, '1');
                    await renderCompany(container, siren);
                } else {
                    showToast('Erreur lors de la mise à jour', 'error');
                    btn.disabled = false;
                    btn.textContent = '✅ Utiliser';
                }
            } catch (err) {
                showToast(`Erreur: ${err.message}`, 'error');
                btn.disabled = false;
                btn.textContent = '✅ Utiliser';
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
                    showToast(`✅ ${field} mis à jour`, 'success');
                    localStorage.setItem(`dismissed_alert_${s}_${alertType}_${field}`, '1');
                    localStorage.setItem(`dismissed_conflict_${s}_${field}`, '1');
                    await renderCompany(container, siren);
                } else {
                    showToast('Erreur lors de la mise à jour', 'error');
                    btn.disabled = false;
                    btn.textContent = '✅ Utiliser l\'alternative';
                }
            } catch (err) {
                showToast(`Erreur: ${err.message}`, 'error');
                btn.disabled = false;
                btn.textContent = '✅ Utiliser l\'alternative';
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
                showToast(data.message || 'Crawl terminé', 'success');
                // Reload company to see new contacts
                await renderCompany(container, siren);
            } else {
                showToast(data.error || 'Erreur lors du crawl', 'error');
                spiderBtn.innerHTML = originalHtml;
                spiderBtn.disabled = false;
            }
        } catch (err) {
            showToast(`Erreur crawl: ${err.message || 'réseau indisponible'}`, 'error');
            spiderBtn.innerHTML = originalHtml;
            spiderBtn.disabled = false;
        }
    });
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
                        showToast('Note supprimée', 'success');
                        _loadNotes(siren);
                    } else {
                        showToast(extractApiError(res), 'error');
                        delBtn.disabled = false;
                    }
                } catch {
                    showToast('Erreur de suppression', 'error');
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
                    showToast('Note ajoutée ✅', 'success');
                    _loadNotes(siren);
                } else {
                    showToast(extractApiError(res), 'error');
                }
            } catch {
                showToast('Erreur lors de l\'ajout', 'error');
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
                    <h2 style="font-size:var(--font-lg); font-weight:700; margin:0">Historique complet (${allNotes.length} notes)</h2>
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
                showToast('Note supprimée', 'success');
                // Refresh both the modal and the underlying page safely
                _loadNotes(siren); // update page
                closeHandler(); // close modal so it forces them to reopen if they want to see it again (cleanest state management)
            } else {
                showToast(extractApiError(res), 'error');
                delBtn.disabled = false;
            }
        } catch {
            showToast('Erreur de suppression', 'error');
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
                    placeholder="Saisir une valeur…">
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
                    
                    showToast(`${field} mis à jour ✅`, 'success');
                } else {
                    showToast(extractApiError(res), 'error');
                    valueCell.innerHTML = originalHTML;
                }
            } catch {
                showToast('Erreur de sauvegarde', 'error');
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
        if (textEl) textEl.textContent = '⏳ Scan du site en cours...';

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
                showToast(data.message || 'Aucun contact trouvé sur le site', 'info');
            } else {
                showToast(data.error || 'Erreur lors du crawl', 'error');
            }
        } catch (err) {
            showToast('Erreur réseau lors du crawl', 'error');
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

    const _SRC_LABELS = {
        google_maps: 'Google Maps', website_crawl: 'Site web', mentions_legales: 'Mentions légales',
        upload: 'Import CSV', manual_edit: 'Modification manuelle', recherche_entreprises: 'API gouvernement',
        sirene: 'Registre SIRENE', inpi: 'INPI',
    };
    const srcLabel = (s) => _SRC_LABELS[s] || s || '?';

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
                    <div style="font-size:var(--font-xs); color:var(--text-muted); margin-bottom:2px">✅ Actuel — ${escapeHtml(srcLabel(alert.current_source))}</div>
                    <div style="color:var(--text-primary); font-weight:500; font-size:var(--font-sm); word-break:break-all">${escapeHtml(alert.current_value)}</div>
                </div>
                <div style="flex:1; min-width:140px; padding:var(--space-xs) var(--space-sm); background:${isCritical ? 'rgba(239,68,68,0.08)' : 'rgba(255,193,7,0.08)'}; border-radius:var(--radius-sm); border:1px solid ${isCritical ? 'rgba(239,68,68,0.2)' : 'rgba(255,193,7,0.2)'}">
                    <div style="font-size:var(--font-xs); color:var(--text-muted); margin-bottom:2px">📦 Alternative — ${escapeHtml(srcLabel(alert.alt_source))}</div>
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
                    ✅ Utiliser l'alternative
                </button>
                <button class="btn-alert-dismiss" data-type="${alert.type}" data-field="${alert.field}" data-siren="${siren}"
                    data-rejected-value="${escapeHtml(alert.alt_value || '')}" data-rejected-source="${alert.alt_source || ''}"
                    style="background:var(--surface-elevated); color:var(--text-secondary); border:1px solid var(--border); padding:5px 14px; border-radius:var(--radius-sm); cursor:pointer; font-size:var(--font-xs);">
                    ❌ Ignorer
                </button>
            </div>
        ` : `
            <div style="display:flex; gap:var(--space-sm); justify-content:flex-end; margin-top:var(--space-sm)">
                <button class="btn-alert-dismiss" data-type="${alert.type}" data-field="${alert.field}" data-siren="${siren}"
                    data-rejected-value="" data-rejected-source=""
                    style="background:var(--surface-elevated); color:var(--text-secondary); border:1px solid var(--border); padding:5px 14px; border-radius:var(--radius-sm); cursor:pointer; font-size:var(--font-xs);">
                    ❌ Ignorer
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
                🔔 ${visible.length} alerte${visible.length > 1 ? 's' : ''} de données
            </div>
            ${rows}
        </div>
    `;
}

/**
 * Translate a contact source code (from DB) to a human-readable French label.
 * e.g. 'google_maps' → '🗺️ Google Maps'
 */
function sourceLabel(src) {
    if (!src) return null;
    const map = {
        google_maps:           '🗺️ Google Maps',
        website_crawl:         '🌐 Site web',
        mentions_legales:      '📜 Mentions légales',
        recherche_entreprises: '🏛️ Registre National',
        google_cse:            '🔍 Google Search',
        synthesized:           '🔗 Synthèse',
        inpi:                  '📋 INPI',
        sirene:                '🏛️ Registre SIRENE',
        manual_edit:           '✏️ Saisi manuellement',
        upload:                '📤 Import fichier',
        directory_search:      '📖 Annuaire',
        pages_jaunes:          '📒 Pages Jaunes',
    };
    return map[src] || src;
}

/** Show an alternate value from a different source (muted sub-row) */
function altValueRow(alt) {
    if (!alt || !alt.value) return '';
    const src = sourceLabel(alt.source) || alt.source;
    return `<div class="detail-row" style="padding-left:var(--space-lg); opacity:0.6; font-size:var(--font-sm)">
        <span class="detail-label">↳ aussi</span>
        <span class="detail-value">${escapeHtml(alt.value)} <span class="provenance-badge" title="Source : ${src}">ℹ️</span></span>
    </div>`;
}

function conflictRow(alt, field, siren, currentValue, currentSource) {
    if (!alt || !alt.value) return '';
    const altSrc = sourceLabel(alt.source) || alt.source;
    const curSrc = sourceLabel(currentSource) || currentSource || '?';
    const dismissedKey = `dismissed_conflict_${siren}_${field}`;
    if (localStorage.getItem(dismissedKey)) return '';
    const reason = `${curSrc} et ${altSrc} ont trouvé des valeurs différentes`;
    return `<div class="detail-row conflict-row" id="conflict-${field}" style="
        font-size:var(--font-sm);
        background: rgba(255,193,7,0.06); border-left: 3px solid var(--warning);
        padding: var(--space-sm) var(--space-md); border-radius: var(--radius-sm);
        margin: var(--space-xs) 0;
    ">
        <div style="display:flex; align-items:center; gap:var(--space-sm); margin-bottom:var(--space-xs)">
            <span style="color:var(--warning); font-weight:600">⚡ Conflit détecté</span>
            <span style="color:var(--text-muted); font-size:var(--font-xs)">— ${reason}</span>
        </div>
        <div style="display:flex; gap:var(--space-md); margin-bottom:var(--space-sm); flex-wrap:wrap">
            <div style="flex:1; min-width:160px; padding:var(--space-xs) var(--space-sm); background:rgba(16,185,129,0.08); border-radius:var(--radius-sm); border:1px solid rgba(16,185,129,0.2)">
                <div style="font-size:var(--font-xs); color:var(--text-muted); margin-bottom:2px">✅ Actuel — ${curSrc}</div>
                <div style="color:var(--text-primary); font-weight:500">${escapeHtml(currentValue || '—')}</div>
            </div>
            <div style="flex:1; min-width:160px; padding:var(--space-xs) var(--space-sm); background:rgba(255,193,7,0.08); border-radius:var(--radius-sm); border:1px solid rgba(255,193,7,0.2)">
                <div style="font-size:var(--font-xs); color:var(--text-muted); margin-bottom:2px">📦 Alternative — ${altSrc}</div>
                <div style="color:var(--text-primary); font-weight:500">${escapeHtml(alt.value)}</div>
            </div>
        </div>
        <div style="display:flex; gap:var(--space-sm); justify-content:flex-end">
            <button class="btn-merge-use" data-field="${field}" data-value="${alt.value.replace(/"/g, '&quot;')}" data-siren="${siren}"
                data-rejected-value="${escapeHtml(currentValue || '')}" data-rejected-source="${currentSource || ''}" data-chosen-source="${alt.source || ''}"
                style="background:var(--success); color:#fff; border:none; padding:5px 14px; border-radius:var(--radius-sm); cursor:pointer; font-size:var(--font-xs); font-weight:600;">
                ✅ Utiliser l'alternative
            </button>
            <button class="btn-merge-dismiss" data-field="${field}" data-siren="${siren}"
                data-rejected-value="${alt.value.replace(/"/g, '&quot;')}" data-rejected-source="${alt.source || ''}"
                style="background:var(--surface-elevated); color:var(--text-secondary); border:1px solid var(--border); padding:5px 14px; border-radius:var(--radius-sm); cursor:pointer; font-size:var(--font-xs);">
                ❌ Ignorer
            </button>
        </div>
    </div>`;
}

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
                Aucune activité enregistrée — lancez un enrichissement ou ajoutez une note.
            </div>
        `;
        return;
    }

    container.innerHTML = `
        <div style="display:flex; flex-direction:column; gap:var(--space-sm)">
            ${history.map(h => {
                if (h.type === 'note') {
                    return `
                        <div style="display:flex; gap:var(--space-md); padding:var(--space-sm); background:var(--surface-raised); border-radius:var(--radius-sm); border-left:3px solid var(--accent)">
                            <div style="font-size:1.2rem; flex-shrink:0">📝</div>
                            <div style="flex:1; min-width:0">
                                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:2px">
                                    <span style="font-weight:700; color:var(--text-primary); font-size:var(--font-xs)">Note par ${escapeHtml(h.username)}</span>
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
                                    <span style="font-weight:700; color:var(--text-primary); font-size:var(--font-xs)">${escapeHtml(h.action)} par ${escapeHtml(h.username)}</span>
                                    <span style="font-size:10px; color:var(--text-disabled)">${_formatTimelineDate(h.timestamp)}</span>
                                </div>
                                ${h.detail ? `<div style="font-size:var(--font-sm); color:var(--text-secondary)">${escapeHtml(h.detail)}</div>` : ''}
                            </div>
                        </div>
                    `;
                } else {
                    const ACTION_MAP = {
                        'maps_lookup': { icon: '🗺️', label: 'Recherche Google Maps' },
                        'website_crawl': { icon: '🕸️', label: 'Analyse de site web' },
                        'officers_found': { icon: '👥', label: 'Dirigeants identifiés' },
                        'financial_data': { icon: '💶', label: 'Données financières' },
                        'siren_verified': { icon: '✅', label: 'Correspondance SIREN' },
                        'siren_mismatch': { icon: '⚠️', label: 'Alerte SIREN' },
                        'manual_edit': { icon: '✏️', label: 'Édition manuelle' },
                        'conflict_resolved': { icon: '✅', label: 'Conflit résolu' },
                        'conflict_dismissed': { icon: '❌', label: 'Conflit ignoré' },
                        'link': { icon: '🔗', label: 'Liaison automatique' },
                        'merge': { icon: '🔀', label: 'Fusion de données' },
                    };
                    const act = ACTION_MAP[h.action] || { icon: '⚙️', label: h.action };
                    const isSuccess = h.result === 'success';
                    const isAlert = h.action === 'siren_mismatch' || h.result === 'fail' || h.result === 'error';
                    const color = isSuccess ? 'var(--success)' : (isAlert ? 'var(--error)' : 'var(--text-secondary)');
                    const resultLabel = isSuccess ? 'Succès' : (h.result === 'fail' || h.result === 'error' ? 'Échec' : h.result);
                    
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
                                        ${h.source_url ? `<span style="opacity:0.6">•</span><a href="${h.source_url.startsWith('http') ? h.source_url : 'https://'+h.source_url}" target="_blank" rel="noopener" style="color:var(--accent); text-decoration:none">🔗 Voir la source</a>` : ''}
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
        const d = new Date(dateStr);
        return d.toLocaleDateString('fr-FR', { day: '2-digit', month: '2-digit', year: 'numeric' })
            + ' à '
            + d.toLocaleTimeString('fr-FR', { hour: '2-digit', minute: '2-digit' });
    } catch {
        return dateStr;
    }
}
