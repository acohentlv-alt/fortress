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
         extractApiError, getCachedUser } from '../api.js';
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

function formatCurrency(val) {
    if (val === null || val === undefined) return null;
    return Number(val).toLocaleString('fr-FR', { style: 'currency', currency: 'EUR', maximumFractionDigits: 0 });
}

// ── Unenriched field helper ──────────────────────────────────────
function unenrichedField(module) {
    return `<span class="unenriched-field">
        <span class="unenriched-badge">🔒 Non enrichi</span>
        <button class="btn-enrich-cta" data-enrich-module="${module}">✨ Enrichir</button>
    </span>`;
}

// ── Enrichment Panel — Crawl only (Maps already ran in batch) ────
function enrichmentPanelHTML() {
    return `
        <button class="btn btn-primary enrich-submit" id="enrich-submit-btn" style="display:inline-flex; align-items:center; gap:var(--space-sm); font-weight:600; padding:var(--space-sm) var(--space-xl); border-radius:var(--radius-lg)">
            <span class="enrich-spinner" style="display:none; width:16px; height:16px; border:2px solid rgba(255,255,255,0.3); border-top-color:#fff; border-radius:50%; animation:spin 1s linear infinite"></span>
            <span class="enrich-submit-text">🚀 Enrichir SIREN</span>
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
                        <span style="font-weight:600; color:var(--text-primary)" title="${escapeHtml(username)}">${initials}</span>
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

// ── Context-aware breadcrumb ─────────────────────────────────────
function _buildBreadcrumb(co, tags) {
    const items = [{ label: 'Dashboard', href: '#/' }];

    // Determine origin from tags
    let parentLabel = 'Recherche';
    let parentHref = '#/search';

    if (tags && tags.length > 0) {
        const firstTag = tags[0].query_name || '';
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

export async function renderCompany(container, siren) {
    const data = await getCompany(siren);

    if (!data || data.error) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">❌</div>
                <div class="empty-state-text">Entreprise introuvable</div>
                <a href="#/" class="btn btn-primary">Retour au Dashboard</a>
            </div>
        `;
        return;
    }

    const co = data.company;
    const mc = data.merged_contact || {};
    const officers = data.officers || [];
    const tags = data.query_tags || [];



    container.innerHTML = `
        ${_buildBreadcrumb(co, tags)}

        <!-- Top Header Panel -->
        <div class="company-detail-header" style="display:flex; justify-content:space-between; align-items:flex-start; margin-bottom:var(--space-2xl)">
            <div class="company-detail-name-block">
                <div class="company-detail-name" style="font-size:2rem; font-weight:800; letter-spacing:-0.03em; margin-bottom:var(--space-xs)">${escapeHtml(co.denomination)}</div>
                <div class="company-detail-siren" style="font-size:var(--font-sm); color:var(--text-secondary); display:flex; align-items:center;">
                    ${formatSiren(co.siren)}
                    <span style="margin: 0 var(--space-sm)">·</span>
                    ${statutBadge(co.statut)}
                    <span style="margin: 0 var(--space-sm)">·</span>
                    ${co.forme_juridique ? formeJuridiqueBadge(co.forme_juridique) : ''}
                </div>
            </div>
            <div class="company-detail-actions">
                ${enrichmentPanelHTML()}
            </div>
        </div>

        <!-- HERO SECTION: Bento Grid 60/40 -->
        <div class="crm-bento-grid" style="gap:var(--space-xl); margin-bottom:var(--space-2xl)">
            
            <!-- Left Column: Communications Hub -->
            <div class="bento-col-left" style="display:flex; flex-direction:column; gap:var(--space-xl)">
                <!-- Contact Card -->
                <div class="detail-section" style="margin-bottom:0">
                    <h3 class="detail-section-title">📞 Contact</h3>
                    ${detailRow('Téléphone', mc.phone
                        ? `<a href="tel:${mc.phone}" style="color:var(--success); font-weight:600">${escapeHtml(mc.phone)}</a>`
                        : unenrichedField('contact_web'), sourceLabel(mc.phone_source), 'phone', mc.phone || '')}
                    ${detailRow('Email', mc.email
                        ? `<a href="mailto:${mc.email}">${escapeHtml(mc.email)}</a>${mc.email_type ? ` <span class="badge badge-muted">${mc.email_type}</span>` : ''}`
                        : unenrichedField('contact_web'), sourceLabel(mc.email_source), 'email', mc.email || '')}
                    ${detailRow('Site web', mc.website
                        ? `<a href="${mc.website.startsWith('http') ? mc.website : 'https://' + mc.website}" target="_blank">${escapeHtml(mc.website)}</a>`
                        : unenrichedField('contact_web'), sourceLabel(mc.website_source), 'website', mc.website || '')}
                    ${mc.address ? detailRow('Adresse Maps', `<span style="color:var(--text-primary)">${escapeHtml(mc.address)}</span>`, '🗺️ Google Maps') : ''}
                    ${detailRow('LinkedIn', formatSocial(mc.social_linkedin, 'Profil LinkedIn'), sourceLabel(mc.social_linkedin_source), 'social_linkedin', mc.social_linkedin || '')}
                    ${detailRow('Facebook', formatSocial(mc.social_facebook, 'Page Facebook'), sourceLabel(mc.social_facebook_source), 'social_facebook', mc.social_facebook || '')}
                    ${detailRow('Twitter', formatSocial(mc.social_twitter, 'Profil Twitter'), sourceLabel(mc.social_twitter_source), 'social_twitter', mc.social_twitter || '')}
                </div>

                <!-- Dirigeants Card -->
                <div class="detail-section">
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
            </div>

            <!-- Right Column: CRM Notes -->
            <div class="bento-col-right" style="display:flex; flex-direction:column; height:100%">
                <!-- Notes Card (sticky constraint) -->
                <div class="detail-section" style="display:flex; flex-direction:column; height:100%; margin-bottom:0">
                    <h3 class="detail-section-title">📝 Notes CRM</h3>
                    <div id="notes-list" style="flex:1; overflow-y:auto; margin-bottom:var(--space-md); padding-right:var(--space-xs)">
                        ${renderNotesDirect(data.notes || [], 2)}
                    </div>
                    <div style="display:flex; gap:var(--space-sm); margin-top:auto">
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

        <!-- BOTTOM SECTION: Legacy Identity & Meta Data -->
        <div class="company-detail">
            <!-- Left Column: Identity Card -->
            <div class="company-detail-identity">
                <!-- Quick Stats Chips -->
                <div style="display:flex; flex-wrap:wrap; gap:var(--space-sm);">
                    ${co.naf_code ? `<span class="badge badge-muted" title="${escapeHtml(co.naf_libelle || '')}">📋 ${escapeHtml(co.naf_code)}</span>` : ''}
                    ${effectifLabel(co.tranche_effectif) ? `<span class="badge badge-muted">👥 ${effectifLabel(co.tranche_effectif)}</span>` : ''}
                    ${co.departement ? `<span class="badge badge-muted">📍 ${escapeHtml(co.departement)}</span>` : ''}
                    ${mc.rating ? `<span class="badge badge-accent">⭐ ${mc.rating}</span>` : ''}
                </div>

                <!-- Google Maps Link (prominent) -->
                ${mc.maps_url ? `
                    <div style="margin-top: var(--space-2xl); text-align:center">
                        <a href="${mc.maps_url}" target="_blank" rel="noopener"
                           style="display:inline-flex; align-items:center; gap:var(--space-sm);
                                  padding:var(--space-md) var(--space-xl);
                                  background:var(--surface-raised); border:1px solid var(--border);
                                  border-radius:var(--radius-lg); color:var(--accent);
                                  font-weight:600; text-decoration:none; transition:all 0.2s">
                            🗺️ Voir sur Google Maps ↗
                        </a>
                    </div>
                ` : ''}

                <!-- Tags -->
                ${tags.length > 0 ? `
                    <div style="margin-top: var(--space-2xl)">
                        <span style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em">
                            Trouvé dans
                        </span>
                        <div style="display:flex; gap:var(--space-sm); flex-wrap:wrap; margin-top:var(--space-sm)">
                            ${tags.map(t => `<span class="badge badge-accent">${escapeHtml(t.query_name)}</span>`).join('')}
                        </div>
                    </div>
                ` : ''}

                <!-- Sources -->
                ${mc.sources && mc.sources.length > 0 ? `
                    <div style="margin-top: var(--space-xl)">
                        <span style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em">
                            Sources de données
                        </span>
                        <div style="display:flex; gap:var(--space-sm); flex-wrap:wrap; margin-top:var(--space-sm)">
                            ${mc.sources.map(s => `<span class="badge badge-muted">${escapeHtml(s)}</span>`).join('')}
                        </div>
                    </div>
                ` : ''}
            </div>

            <!-- Right Column: Data Sections -->


                <!-- 3. Identité juridique -->
                <div class="detail-section">
                    <h3 class="detail-section-title">🏛️ Identité juridique</h3>
                    ${detailRow('Dénomination', `<span style="font-weight:700">${escapeHtml(co.denomination)}</span>`, 'Registre SIRENE', 'denomination', co.denomination || '')}
                    ${detailRow('SIREN', formatSiren(co.siren), 'Registre SIRENE')}
                    ${detailRow('SIRET siège', formatSiret(co.siret_siege), 'Registre SIRENE')}
                    ${detailRow('Forme juridique', co.forme_juridique || '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE')}
                    ${detailRow('Statut', statutBadge(co.statut), 'Registre SIRENE')}
                    ${detailRow('Date création', formatDate(co.date_creation), 'Registre SIRENE')}
                </div>

                <!-- 4. Localisation -->
                <div class="detail-section">
                    <h3 class="detail-section-title">📍 Localisation</h3>
                    ${detailRow('Adresse', co.adresse || '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE', 'adresse', co.adresse || '')}
                    ${detailRow('Code postal', co.code_postal || '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE', 'code_postal', co.code_postal || '')}
                    ${detailRow('Ville', co.ville || '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE', 'ville', co.ville || '')}
                    ${detailRow('Département', co.departement ? `${escapeHtml(co.departement)}${co.region ? ` · ${escapeHtml(co.region)}` : ''}` : '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE')}
                </div>

                <!-- 5. Activité & Effectif -->
                <div class="detail-section">
                    <h3 class="detail-section-title">📊 Activité</h3>
                    ${detailRow('Code NAF', co.naf_code ? `<strong>${escapeHtml(co.naf_code)}</strong>` : '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE')}
                    ${detailRow('Libellé NAF', co.naf_libelle ? escapeHtml(co.naf_libelle) : '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE')}
                    ${detailRow('Effectif', effectifLabel(co.tranche_effectif) || '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE')}
                </div>

                <!-- 6. Données financières -->
                <div class="detail-section">
                    <h3 class="detail-section-title">💰 Données financières</h3>
                    ${detailRow("Chiffre d'affaires",
        co.chiffre_affaires
            ? formatCurrency(co.chiffre_affaires)
            : '<span style="color:var(--text-disabled); font-style:italic">Non disponible <span class="badge badge-muted" style="font-size:var(--font-xs)">Prochainement</span></span>')}
                    ${detailRow('Résultat net',
        co.resultat_net
            ? formatCurrency(co.resultat_net)
            : '<span style="color:var(--text-disabled); font-style:italic">Non disponible <span class="badge badge-muted" style="font-size:var(--font-xs)">Prochainement</span></span>')}
                </div>

                <!-- 7. Données supplémentaires (extra_data JSONB) -->
                ${co.extra_data && Object.keys(co.extra_data).length > 0 ? `
                <div class="detail-section">
                    <h3 class="detail-section-title">📋 Données supplémentaires</h3>
                    <div style="background:var(--bg-tertiary, var(--bg-secondary)); border-radius:var(--radius-md); padding:var(--space-sm) 0; border:1px solid var(--border-subtle)">
                        ${Object.entries(co.extra_data).map(([k, v]) => `
                            <div class="detail-row" style="padding:var(--space-xs) var(--space-md)">
                                <span class="detail-label" style="color:var(--text-muted)">${escapeHtml(k)}</span>
                                <span class="detail-value">${escapeHtml(String(v))}</span>
                            </div>
                        `).join('')}
                    </div>
                </div>
                ` : ''}



                <!-- 9. Enrichment History Timeline -->
                <div class="detail-section">
                    <h3 class="detail-section-title">📜 Historique d'enrichissement</h3>
                    <div id="enrich-history-container">
                        <div class="loading" style="padding:var(--space-lg) 0"><div class="spinner"></div></div>
                    </div>
                </div>
            </div>
        </div>
    `;

    // ── Wire up Smart Enrichment Panel ───────────────────────────
    _initEnrichmentPanel(siren, container);

    // ── Load Enrichment History ──────────────────────────────────
    _loadEnrichHistory(siren, data.contacts || []);

    // ── Inline editing on detail rows ───────────────────────────
    _initInlineEditing(container, siren);

    // ── Notes system ────────────────────────────────────────────
    _initNotes(siren);
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
                    showToast(`${field} mis à jour ✅`, 'success');
                    // Refresh the page to show updated data
                    await renderCompany(container, siren);
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
    const panel = document.getElementById('enrich-panel');
    const submitBtn = document.getElementById('enrich-submit-btn');
    if (!panel || !submitBtn) return;

    // Crawl-only enrichment (Maps already ran in batch)
    const modules = ['contact_web'];

    // Submit handler — 200 vs 202 split
    submitBtn.addEventListener('click', async () => {
        // Loading state
        submitBtn.classList.add('loading');
        submitBtn.disabled = true;

        try {
            const result = await enrichCompany(siren, modules);

            if (result && result._status === 202) {
                showToast(result.message || 'Enrichissement lancé — ~25s...', 'success');
            } else if (result && result._ok) {
                showToast(result.message || 'Données récupérées', 'success');
                await renderCompany(container, siren);
                return;
            } else {
                showToast(extractApiError(result), 'error');
            }
        } catch (err) {
            showToast('Erreur lors de l\'enrichissement', 'error');
        } finally {
            submitBtn.classList.remove('loading');
            submitBtn.disabled = false;
        }
    });

    // ── Wire up CTA buttons from empty states ────────────────────
    document.querySelectorAll('.btn-enrich-cta').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.preventDefault();
            e.stopPropagation();
            const targetModule = btn.dataset.enrichModule;

            // Scroll to the enrichment panel
            panel.scrollIntoView({ behavior: 'smooth', block: 'center' });

            // Highlight the panel briefly
            panel.classList.add('enrich-panel-highlight');
            setTimeout(() => panel.classList.remove('enrich-panel-highlight'), 2000);

            // Scroll-and-highlight only — no checkbox toggling
            // (the panel has a single hardcoded module, no checkboxes)
        });
    });
}

/**
 * Translate a contact source code (from DB) to a human-readable French label.
 * e.g. 'google_maps' → '🗺️ Google Maps'
 */
function sourceLabel(src) {
    if (!src) return null;
    const map = {
        google_maps:   '🗺️ Google Maps',
        website_crawl: '🌐 Site web',
        synthesized:   '🔗 Synthèse',
        inpi:          '📋 INPI',
        sirene:        '🏛️ Registre SIRENE',
    };
    return map[src] || src;
}

function formatSocial(url, label) {
    if (!url) return '<span style="color:var(--text-disabled)">—</span>';
    if (url.startsWith('http') || url.startsWith('www.')) {
        const href = url.startsWith('http') ? url : `https://${url}`;
        return `<a href="${href}" target="_blank">${label} ↗</a>`;
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

// ── Enrichment History ───────────────────────────────────────────
async function _loadEnrichHistory(siren, contacts) {
    const container = document.getElementById('enrich-history-container');
    if (!container) return;

    let timeline = [];

    // Try API first
    try {
        const apiData = await getCompanyEnrichHistory(siren);
        if (apiData && apiData.history && Array.isArray(apiData.history) && apiData.history.length > 0) {
            timeline = apiData.history;
        }
    } catch { /* fallback below */ }

    // Fallback: derive timeline from contacts data
    if (timeline.length === 0 && contacts.length > 0) {
        const FIELD_LABELS = {
            phone: '📞 Téléphone ajouté',
            email: '✉️ Email trouvé',
            website: '🌐 Site web trouvé',
            rating: '⭐ Avis Google récupéré',
            social_linkedin: '🔗 LinkedIn trouvé',
            social_facebook: '📘 Facebook trouvé',
        };
        for (const c of contacts) {
            const date = c.collected_at;
            const agent = c.source || 'Inconnu';
            for (const [field, label] of Object.entries(FIELD_LABELS)) {
                if (c[field]) {
                    timeline.push({
                        date,
                        action: label,
                        agent,
                        field,
                        value: String(c[field]),
                    });
                }
            }
        }
        // Sort newest first
        timeline.sort((a, b) => new Date(b.date || 0) - new Date(a.date || 0));
    }

    // Render
    if (timeline.length === 0) {
        container.innerHTML = `
            <div style="color:var(--text-muted); font-style:italic; padding:var(--space-sm) 0; font-size:var(--font-sm)">
                Aucun enrichissement enregistré — lancez un enrichissement ci-dessus ↑
            </div>
        `;
        return;
    }

    container.innerHTML = `
        <div class="enrich-timeline">
            ${timeline.map(ev => `
                <div class="enrich-timeline-item">
                    <div class="enrich-timeline-dot"></div>
                    <div class="enrich-timeline-date">${_formatTimelineDate(ev.date)}</div>
                    <div class="enrich-timeline-action">
                        ${escapeHtml(ev.action)}
                        ${ev.value ? `<span style="color:var(--text-muted); font-size:var(--font-xs)"> — ${escapeHtml(ev.value)}</span>` : ''}
                    </div>
                    <div class="enrich-timeline-agent">
                        via <span class="badge badge-muted">${escapeHtml(ev.agent)}</span>
                    </div>
                </div>
            `).join('')}
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
