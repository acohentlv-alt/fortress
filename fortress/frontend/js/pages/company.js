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
        <div class="enrich-panel" id="enrich-panel">
            <div class="enrich-panel-title">⚡ Enrichissement</div>
            <div class="enrich-pipeline-preview">
                <div class="enrich-step">
                    <span class="enrich-step-icon">🌐</span>
                    <div>
                        <div class="enrich-step-label">Site Web</div>
                        <div class="enrich-step-desc">Email, LinkedIn, Facebook, réseaux sociaux</div>
                        <div class="enrich-step-time">~20 secondes</div>
                    </div>
                </div>
            </div>
            <button class="enrich-submit" id="enrich-submit-btn">
                <span class="enrich-spinner"></span>
                <span class="enrich-submit-text">🚀 Lancer l'enrichissement</span>
            </button>
        </div>
    `;
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

        <div class="company-detail">
            <!-- Left Column: Identity Card -->
            <div class="company-detail-identity">
                <div class="company-detail-name">${escapeHtml(co.denomination)}</div>
                <div class="company-detail-siren">
                    ${formatSiren(co.siren)}
                    <span style="margin: 0 var(--space-sm)">·</span>
                    ${statutBadge(co.statut)}
                    ${co.forme_juridique ? formeJuridiqueBadge(co.forme_juridique) : ''}
                </div>

                <!-- Quick Stats Chips -->
                <div style="display:flex; flex-wrap:wrap; gap:var(--space-sm); margin-top:var(--space-xl)">
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

                <!-- Smart Enrichment Panel — MOVED TO TOP of identity -->
                ${enrichmentPanelHTML()}

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
            <div class="company-detail-data">

                <!-- TOP ROW: Contact + Dirigeants side by side -->
                <div class="detail-top-row">
                    <!-- Contact Card -->
                    <div class="detail-section">
                        <h3 class="detail-section-title">📞 Contact</h3>
                        ${detailRow('Téléphone', mc.phone
        ? `<a href="tel:${mc.phone}" style="color:var(--success); font-weight:600">${mc.phone}</a>`
        : unenrichedField('contact_web'), sourceLabel(mc.phone_source), 'phone')}
                        ${detailRow('Email', mc.email
                ? `<a href="mailto:${mc.email}">${escapeHtml(mc.email)}</a>${mc.email_type ? ` <span class="badge badge-muted">${mc.email_type}</span>` : ''}`
                : unenrichedField('contact_web'), sourceLabel(mc.email_source), 'email')}
                        ${detailRow('Site web', mc.website
                    ? `<a href="${mc.website.startsWith('http') ? mc.website : 'https://' + mc.website}" target="_blank">${escapeHtml(mc.website)}</a>`
                    : unenrichedField('contact_web'), sourceLabel(mc.website_source), 'website')}
                        ${mc.address ? detailRow('Adresse Maps', `<span style="color:var(--text-primary)">${escapeHtml(mc.address)}</span>`, '🗺️ Google Maps') : ''}
                        ${detailRow('LinkedIn', mc.social_linkedin
                            ? `<a href="${mc.social_linkedin}" target="_blank">Profil LinkedIn ↗</a>`
                            : '<span style="color:var(--text-disabled)">—</span>', sourceLabel(mc.social_linkedin_source), 'social_linkedin')}
                        ${detailRow('Facebook', mc.social_facebook
                            ? `<a href="${mc.social_facebook}" target="_blank">Page Facebook ↗</a>`
                            : '<span style="color:var(--text-disabled)">—</span>', sourceLabel(mc.social_facebook_source), 'social_facebook')}
                        ${detailRow('Twitter', mc.social_twitter
                            ? `<a href="${mc.social_twitter}" target="_blank">Profil Twitter ↗</a>`
                            : '<span style="color:var(--text-disabled)">—</span>', sourceLabel(mc.social_twitter_source), 'social_twitter')}
                        ${mc.rating ? `
                            <div class="detail-row" style="margin-top:var(--space-md)">
                                <span class="detail-label">Avis Google <span class="provenance-badge" title="Source : ${sourceLabel(mc.rating_source)}">ℹ️</span></span>
                                <span class="detail-value">
                                    <span style="font-weight:700">${mc.rating}</span>
                                    <span style="color:var(--warning)">${'★'.repeat(Math.round(mc.rating))}${'☆'.repeat(5 - Math.round(mc.rating))}</span>
                                    <span style="color:var(--text-secondary); font-size:var(--font-sm)">(${mc.review_count || 0} avis)</span>
                                </span>
                            </div>
                        ` : ''}
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

                <!-- 3. Identité juridique -->
                <div class="detail-section">
                    <h3 class="detail-section-title collapsible expanded" data-section="identite">
                        <span>🏛️ Identité juridique</span>
                        <span class="detail-section-chevron">▶</span>
                    </h3>
                    <div class="detail-section-body" data-section-body="identite">
                    ${detailRow('Dénomination', escapeHtml(co.denomination), 'Registre SIRENE', 'denomination')}
                    ${detailRow('SIREN', formatSiren(co.siren), 'Registre SIRENE')}
                    ${detailRow('SIRET siège', formatSiret(co.siret_siege), 'Registre SIRENE')}
                    ${detailRow('Forme juridique', co.forme_juridique || '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE')}
                    ${detailRow('Statut', statutBadge(co.statut), 'Registre SIRENE')}
                    ${detailRow('Date création', formatDate(co.date_creation), 'Registre SIRENE')}
                    </div>
                </div>

                <!-- 4. Localisation -->
                <div class="detail-section">
                    <h3 class="detail-section-title collapsible" data-section="localisation">
                        <span>📍 Localisation</span>
                        <span class="detail-section-chevron">▶</span>
                    </h3>
                    <div class="detail-section-body collapsed" data-section-body="localisation">
                    ${detailRow('Adresse', co.adresse || '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE', 'adresse')}
                    ${detailRow('Code postal', co.code_postal || '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE', 'code_postal')}
                    ${detailRow('Ville', co.ville || '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE', 'ville')}
                    ${detailRow('Département', co.departement ? `${escapeHtml(co.departement)}${co.region ? ` · ${escapeHtml(co.region)}` : ''}` : '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE')}
                    </div>
                </div>

                <!-- 5. Activité & Effectif -->
                <div class="detail-section">
                    <h3 class="detail-section-title collapsible" data-section="activite">
                        <span>📊 Activité</span>
                        <span class="detail-section-chevron">▶</span>
                    </h3>
                    <div class="detail-section-body collapsed" data-section-body="activite">
                    ${detailRow('Code NAF', co.naf_code ? `<strong>${escapeHtml(co.naf_code)}</strong>` : '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE')}
                    ${detailRow('Libellé NAF', co.naf_libelle ? escapeHtml(co.naf_libelle) : '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE')}
                    ${detailRow('Effectif', effectifLabel(co.tranche_effectif) || '<span style="color:var(--text-disabled)">—</span>', 'Registre SIRENE')}
                    </div>
                </div>

                <!-- 6. Données financières -->
                <div class="detail-section">
                    <h3 class="detail-section-title collapsible" data-section="financier">
                        <span>💰 Données financières</span>
                        <span class="detail-section-chevron">▶</span>
                    </h3>
                    <div class="detail-section-body collapsed" data-section-body="financier">
                    ${detailRow("Chiffre d'affaires",
        co.chiffre_affaires
            ? formatCurrency(co.chiffre_affaires)
            : '<span style="color:var(--text-disabled); font-style:italic">Non disponible <span class="badge badge-muted" style="font-size:var(--font-xs)">Prochainement</span></span>')}
                    ${detailRow('Résultat net',
        co.resultat_net
            ? formatCurrency(co.resultat_net)
            : '<span style="color:var(--text-disabled); font-style:italic">Non disponible <span class="badge badge-muted" style="font-size:var(--font-xs)">Prochainement</span></span>')}
                    </div>
                </div>

                <!-- 7. Données supplémentaires (extra_data JSONB) -->
                ${co.extra_data && Object.keys(co.extra_data).length > 0 ? `
                <div class="detail-section">
                    <h3 class="detail-section-title collapsible" data-section="extra">
                        <span>📋 Données supplémentaires</span>
                        <span class="detail-section-chevron">▶</span>
                    </h3>
                    <div class="detail-section-body collapsed" data-section-body="extra">
                    <div style="background:var(--bg-tertiary, var(--bg-secondary)); border-radius:var(--radius-md); padding:var(--space-sm) 0; border:1px solid var(--border-subtle)">
                        ${Object.entries(co.extra_data).map(([k, v]) => `
                            <div class="detail-row" style="padding:var(--space-xs) var(--space-md)">
                                <span class="detail-label" style="color:var(--text-muted)">${escapeHtml(k)}</span>
                                <span class="detail-value">${escapeHtml(String(v))}</span>
                            </div>
                        `).join('')}
                    </div>
                    </div>
                </div>
                ` : ''}

                <!-- 8. Notes (Comments) — CRM step 1 -->
                <div class="detail-section">
                    <h3 class="detail-section-title" style="display:flex; align-items:center; justify-content:space-between; cursor:pointer" id="notes-toggle">
                        <span>📝 Notes</span>
                        <span class="notes-toggle-chevron" id="notes-chevron" style="font-size:var(--font-xs); transition:transform 0.2s">▼</span>
                    </h3>
                    <div id="notes-section" style="display:none">
                        <div id="notes-list">
                            <div class="loading" style="padding:var(--space-lg) 0"><div class="spinner"></div></div>
                        </div>
                        <div style="margin-top:var(--space-md); display:flex; gap:var(--space-sm)">
                            <textarea id="note-input" placeholder="Ajouter une note…"
                                style="flex:1; min-height:60px; padding:var(--space-sm) var(--space-md);
                                background:var(--bg-input); border:1px solid var(--border-default);
                                border-radius:var(--radius-sm); color:var(--text-primary);
                                font-family:var(--font-family); font-size:var(--font-sm);
                                resize:vertical; outline:none"></textarea>
                            <button id="note-submit-btn" class="btn btn-primary" style="align-self:flex-end; white-space:nowrap">
                                Ajouter
                            </button>
                        </div>
                    </div>
                </div>

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
    _initInlineEditing(container, siren, { co, mc });

    // ── Notes system ────────────────────────────────────────────
    _initNotes(siren);

    // ── Collapsible sections ────────────────────────────────────
    _initCollapsibleSections(container);
}

// ── Collapsible Sections ─────────────────────────────────────────
function _initCollapsibleSections(container) {
    container.querySelectorAll('.detail-section-title.collapsible').forEach(title => {
        title.addEventListener('click', () => {
            const sectionKey = title.dataset.section;
            const body = container.querySelector(`[data-section-body="${sectionKey}"]`);
            if (!body) return;

            const isCollapsed = body.classList.contains('collapsed');
            if (isCollapsed) {
                body.classList.remove('collapsed');
                title.classList.add('expanded');
            } else {
                body.classList.add('collapsed');
                title.classList.remove('expanded');
            }
        });
    });
}

// ── Notes System ─────────────────────────────────────────────────
function _initNotes(siren) {
    const toggle = document.getElementById('notes-toggle');
    const section = document.getElementById('notes-section');
    const chevron = document.getElementById('notes-chevron');
    if (!toggle || !section) return;

    let loaded = false;

    toggle.addEventListener('click', () => {
        const visible = section.style.display !== 'none';
        section.style.display = visible ? 'none' : 'block';
        chevron.style.transform = visible ? '' : 'rotate(180deg)';
        if (!loaded) {
            loaded = true;
            _loadNotes(siren);
        }
    });

    // Submit button
    const submitBtn = document.getElementById('note-submit-btn');
    const input = document.getElementById('note-input');
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
    const currentUser = getCachedUser();

    if (notes.length === 0) {
        container.innerHTML = `
            <div style="color:var(--text-muted); font-style:italic; padding:var(--space-sm) 0; font-size:var(--font-sm)">
                Aucune note — ajoutez la première ci-dessous
            </div>
        `;
        return;
    }

    container.innerHTML = notes.map(n => {
        const canDelete = currentUser && (currentUser.id === n.user_id || currentUser.role === 'admin');
        return `
            <div class="note-item" style="padding:var(--space-sm) 0; border-bottom:1px solid var(--border-subtle)">
                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:var(--space-xs)">
                    <span style="font-size:var(--font-xs); font-weight:600; color:var(--text-secondary)">
                        ${n.username === currentUser?.username ? '👑' : '👤'} ${escapeHtml(n.username)}
                        <span style="color:var(--text-muted); font-weight:400; margin-left:var(--space-sm)">
                            ${_formatNoteDate(n.created_at)}
                        </span>
                    </span>
                    ${canDelete ? `<button class="btn-ghost btn-icon note-delete-btn" data-note-id="${n.id}" title="Supprimer" style="font-size:var(--font-xs); color:var(--text-muted)">🗑️</button>` : ''}
                </div>
                <div style="font-size:var(--font-sm); color:var(--text-primary); white-space:pre-wrap; line-height:1.5">${escapeHtml(n.text)}</div>
            </div>
        `;
    }).join('');

    // Wire delete buttons
    container.querySelectorAll('.note-delete-btn').forEach(btn => {
        btn.addEventListener('click', async (e) => {
            e.stopPropagation();
            const noteId = btn.dataset.noteId;
            btn.disabled = true;
            try {
                const res = await deleteCompanyNote(noteId);
                if (res._ok !== false) {
                    showToast('Note supprimée', 'success');
                    _loadNotes(siren);
                } else {
                    showToast(extractApiError(res), 'error');
                }
            } catch {
                showToast('Erreur de suppression', 'error');
            }
        });
    });
}

function _formatNoteDate(dateStr) {
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

// ── Inline Edit Logic ────────────────────────────────────────────
function _initInlineEditing(container, siren, { co, mc }) {
    // Raw values for pre-filling inputs (field → current value)
    const rawValues = {
        phone: mc.phone || '',
        email: mc.email || '',
        website: mc.website || '',
        social_linkedin: mc.social_linkedin || '',
        social_facebook: mc.social_facebook || '',
        social_twitter: mc.social_twitter || '',
        denomination: co.denomination || '',
        adresse: co.adresse || '',
        code_postal: co.code_postal || '',
        ville: co.ville || '',
    };

    container.addEventListener('click', (e) => {
        const btn = e.target.closest('.btn-inline-edit');
        if (!btn) return;
        e.stopPropagation();

        const field = btn.dataset.field;
        const row = btn.closest('.detail-row');
        const valueCell = row.querySelector('.detail-value');
        if (!valueCell || valueCell.querySelector('.inline-edit-input')) return; // Already editing

        const currentVal = rawValues[field] || '';

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
        input.select();

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

function detailRow(label, value, source = null, editField = null) {
    const tooltip = source
        ? `<span class="provenance-badge" title="Source : ${source}">ℹ️</span>`
        : '';
    const editBtn = editField
        ? `<button class="btn-inline-edit" data-field="${editField}" title="Modifier" onclick="event.stopPropagation()">✏️</button>`
        : '';
    return `
        <div class="detail-row" ${editField ? `data-edit-field="${editField}"` : ''}>
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
