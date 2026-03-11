/**
 * Company Detail Page — Pappers-style full view
 * Displays the enriched data model:
 *   Identity, Forme Juridique, Code NAF, Headcount, Dirigeants,
 *   Contacts, Revenue (graceful null → "Non public")
 *
 * Features:
 *   - Smart Enrichment Panel with goal-oriented checkboxes
 *   - Actionable empty states for unenriched fields
 */

import { getCompany, enrichCompany, getCompanyEnrichHistory, extractApiError } from '../api.js';
import {
    breadcrumb, formatSiren, formatSiret, formatDate,
    statutBadge, formeJuridiqueBadge, escapeHtml, renderGauge, showToast,
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

// ── Smart Enrichment Panel HTML ──────────────────────────────────
function enrichmentPanelHTML() {
    return `
        <div class="enrich-panel" id="enrich-panel">
            <div class="enrich-panel-title">⚡ Smart Enrichment</div>
            <div class="enrich-options">
                <label class="enrich-option">
                    <input type="checkbox" name="enrich-module" value="contact_web">
                    <span class="enrich-option-icon">🌐</span>
                    <div class="enrich-option-info">
                        <div class="enrich-option-label">Website & Contacts</div>
                        <div class="enrich-option-desc">Email, site web, réseaux sociaux, avis Google</div>
                    </div>
                </label>
                <label class="enrich-option">
                    <input type="checkbox" name="enrich-module" value="contact_phone">
                    <span class="enrich-option-icon">📞</span>
                    <div class="enrich-option-info">
                        <div class="enrich-option-label">Phone Numbers</div>
                        <div class="enrich-option-desc">Numéro de téléphone principal via PagesJaunes</div>
                    </div>
                </label>
                <label class="enrich-option">
                    <input type="checkbox" name="enrich-module" value="financials">
                    <span class="enrich-option-icon">💰</span>
                    <div class="enrich-option-info">
                        <div class="enrich-option-label">Financials</div>
                        <div class="enrich-option-desc">SIRET, CA, résultat net, effectif via Pappers</div>
                    </div>
                </label>
            </div>
            <button class="enrich-submit" id="enrich-submit-btn" disabled>
                <span class="enrich-spinner"></span>
                <span class="enrich-submit-text">🚀 Lancer l'enrichissement</span>
            </button>
        </div>
    `;
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

    // Compute completude score (out of available fields)
    const checkFields = [
        mc.phone, mc.email, mc.website, mc.address,
        mc.rating, mc.maps_url,
        co.siret_siege, co.naf_code, co.forme_juridique,
        co.tranche_effectif, officers.length > 0,
    ];
    const filledCount = checkFields.filter(Boolean).length;
    const completudePct = Math.round((filledCount / checkFields.length) * 100);

    container.innerHTML = `
        ${breadcrumb([
        { label: 'Dashboard', href: '#/' },
        { label: 'Recherche', href: '#/search' },
        { label: co.denomination },
    ])}

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

                <!-- Completude Gauge -->
                <div style="margin-top: var(--space-2xl)">
                    <div style="display:flex; justify-content:center; margin-bottom:var(--space-lg)">
                        ${renderGauge(completudePct, '📊 Complétude')}
                    </div>
                    <div style="display:flex; justify-content:center; gap:var(--space-xl); flex-wrap:wrap">
                        ${renderGauge(mc.phone ? 100 : 0, '📞 Tél.')}
                        ${renderGauge(mc.email ? 100 : 0, '✉️ Email')}
                        ${renderGauge(mc.website ? 100 : 0, '🌐 Web')}
                        ${renderGauge(co.siret_siege ? 100 : 0, '🏢 SIRET')}
                    </div>
                </div>

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

                <!-- Smart Enrichment Panel -->
                ${enrichmentPanelHTML()}
            </div>

            <!-- Right Column: Data Sections -->
            <div class="company-detail-data">
                <!-- Identité juridique -->
                <div class="detail-section">
                    <h3 class="detail-section-title">🏛️ Identité juridique</h3>
                    ${detailRow('SIREN', formatSiren(co.siren))}
                    ${detailRow('SIRET siège', formatSiret(co.siret_siege))}
                    ${detailRow('Forme juridique', co.forme_juridique || '<span style="color:var(--text-disabled)">—</span>')}
                    ${detailRow('Statut', statutBadge(co.statut))}
                    ${detailRow('Date création', formatDate(co.date_creation))}
                </div>

                <!-- Activité & Effectif -->
                <div class="detail-section">
                    <h3 class="detail-section-title">📊 Activité</h3>
                    ${detailRow('Code NAF', co.naf_code ? `<strong>${escapeHtml(co.naf_code)}</strong>` : '<span style="color:var(--text-disabled)">—</span>')}
                    ${detailRow('Libellé NAF', co.naf_libelle ? escapeHtml(co.naf_libelle) : '<span style="color:var(--text-disabled)">—</span>')}
                    ${detailRow('Effectif', effectifLabel(co.tranche_effectif) || '<span style="color:var(--text-disabled)">—</span>')}
                </div>

                <!-- Données financières — actionable empty states -->
                <div class="detail-section">
                    <h3 class="detail-section-title">💰 Données financières</h3>
                    ${detailRow("Chiffre d'affaires",
        co.chiffre_affaires
            ? formatCurrency(co.chiffre_affaires)
            : unenrichedField('financials')
    )}
                    ${detailRow('Résultat net',
        co.resultat_net
            ? formatCurrency(co.resultat_net)
            : unenrichedField('financials')
    )}
                </div>

                <!-- Localisation -->
                <div class="detail-section">
                    <h3 class="detail-section-title">📍 Localisation</h3>
                    ${detailRow('Adresse', co.adresse || '<span style="color:var(--text-disabled)">—</span>')}
                    ${detailRow('Code postal', co.code_postal || '<span style="color:var(--text-disabled)">—</span>')}
                    ${detailRow('Ville', co.ville || '<span style="color:var(--text-disabled)">—</span>')}
                    ${detailRow('Département', co.departement ? `${escapeHtml(co.departement)}${co.region ? ` · ${escapeHtml(co.region)}` : ''}` : '<span style="color:var(--text-disabled)">—</span>')}
                </div>

                <!-- Contact — actionable empty states -->
                <div class="detail-section">
                    <h3 class="detail-section-title">📞 Contact</h3>
                    ${detailRow('Téléphone', mc.phone
        ? `<a href="tel:${mc.phone}" style="color:var(--success); font-weight:600">${mc.phone}</a>`
        : unenrichedField('contact_phone'))}
                    ${detailRow('Email', mc.email
            ? `<a href="mailto:${mc.email}">${escapeHtml(mc.email)}</a>${mc.email_type ? ` <span class="badge badge-muted">${mc.email_type}</span>` : ''}`
            : unenrichedField('contact_web'))}
                    ${detailRow('Site web', mc.website
                ? `<a href="${mc.website.startsWith('http') ? mc.website : 'https://' + mc.website}" target="_blank">${escapeHtml(mc.website)}</a>`
                : unenrichedField('contact_web'))}
                    ${mc.address ? detailRow('Adresse Maps', `<span style="color:var(--text-primary)">${escapeHtml(mc.address)}</span>`) : ''}
                    ${mc.maps_url ? detailRow('Google Maps', `<a href="${mc.maps_url}" target="_blank" rel="noopener" style="color:var(--accent); font-weight:600">🗺️ Voir sur Google Maps ↗</a>`) : ''}
                    ${mc.social_linkedin ? detailRow('LinkedIn', `<a href="${mc.social_linkedin}" target="_blank">Profil LinkedIn ↗</a>`) : ''}
                    ${mc.social_facebook ? detailRow('Facebook', `<a href="${mc.social_facebook}" target="_blank">Page Facebook ↗</a>`) : ''}
                </div>

                <!-- Avis Google -->
                ${mc.rating ? `
                    <div class="detail-section">
                        <h3 class="detail-section-title">⭐ Avis Google</h3>
                        <div style="display:flex; align-items:center; gap:var(--space-lg)">
                            <span style="font-size:var(--font-2xl); font-weight:700">${mc.rating}</span>
                            <span style="font-size:1.2rem; color:var(--warning)">${'★'.repeat(Math.round(mc.rating))}${'☆'.repeat(5 - Math.round(mc.rating))}</span>
                            <span style="color:var(--text-secondary); font-size:var(--font-sm)">(${mc.review_count || 0} avis)</span>
                        </div>
                    </div>
                ` : ''}

                <!-- Dirigeants -->
                <div class="detail-section">
                    <h3 class="detail-section-title">👤 Dirigeants</h3>
                    ${officers.length > 0 ? officers.map(o => `
                        <div class="detail-row">
                            <span class="detail-label">${escapeHtml(o.role || 'Dirigeant')}</span>
                            <span class="detail-value" style="font-weight:600">${escapeHtml(o.prenom ? `${o.prenom} ${o.nom}` : o.nom)}</span>
                        </div>
                    `).join('') : `
                        <div style="color:var(--text-disabled); font-style:italic; padding:var(--space-sm) 0">
                            Aucun dirigeant référencé
                        </div>
                    `}
                </div>

                <!-- Enrichment History Timeline -->
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
    _initEnrichmentPanel(siren);

    // ── Load Enrichment History ──────────────────────────────────
    _loadEnrichHistory(siren, data.contacts || []);
}

// ── Enrichment Panel Logic ───────────────────────────────────────
function _initEnrichmentPanel(siren) {
    const panel = document.getElementById('enrich-panel');
    const submitBtn = document.getElementById('enrich-submit-btn');
    if (!panel || !submitBtn) return;

    const checkboxes = panel.querySelectorAll('input[name="enrich-module"]');

    // Enable/disable submit based on selections
    function updateSubmitState() {
        const anyChecked = Array.from(checkboxes).some(cb => cb.checked);
        submitBtn.disabled = !anyChecked;
    }
    checkboxes.forEach(cb => cb.addEventListener('change', updateSubmitState));

    // Submit handler — 200 vs 202 split
    submitBtn.addEventListener('click', async () => {
        const modules = Array.from(checkboxes)
            .filter(cb => cb.checked)
            .map(cb => cb.value);

        if (modules.length === 0) return;

        // Loading state
        submitBtn.classList.add('loading');
        submitBtn.disabled = true;

        try {
            const result = await enrichCompany(siren, modules);

            if (result && result._status === 202) {
                // Async job queued — scraping will happen in background
                showToast(result.message || 'Mise en file d\'attente...', 'success');
            } else if (result && result._ok) {
                // 200 — data was cached/deduplicated, available immediately
                showToast(result.message || 'Données récupérées', 'success');
                // Re-render the page to show updated data instantly
                await renderCompany(container, siren);
                return; // skip finally — page is re-rendered
            } else {
                // Error (422, 404, 500, etc.)
                showToast(extractApiError(result), 'error');
            }
        } catch (err) {
            showToast('Erreur lors de l\'enrichissement', 'error');
        } finally {
            submitBtn.classList.remove('loading');
            updateSubmitState();
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

            // Auto-check the relevant checkbox
            if (targetModule) {
                const cb = panel.querySelector(`input[value="${targetModule}"]`);
                if (cb && !cb.checked) {
                    cb.checked = true;
                    updateSubmitState();
                }
            }
        });
    });
}

function detailRow(label, value) {
    return `
        <div class="detail-row">
            <span class="detail-label">${label}</span>
            <span class="detail-value">${value}</span>
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
        if (apiData && Array.isArray(apiData) && apiData.length > 0) {
            timeline = apiData;
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

