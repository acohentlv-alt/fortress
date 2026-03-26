/**
 * Add Entity Modal — shared component used by dashboard and contacts pages.
 *
 * Opens a form to manually create a new MAPS entity with full contact card.
 * Optionally links to an existing SIRENE company by SIREN number.
 *
 * Usage:
 *   showAddEntityModal({ onSuccess: () => refreshPage() });
 */

import { searchSirene, createEntity } from '../api.js';
import { showToast, escapeHtml } from '../components.js';
import { DEPARTMENTS } from '../constants.js';

const MODAL_ID = 'add-entity-modal-overlay';

export function showAddEntityModal({ onSuccess } = {}) {
    // Remove any existing modal
    const existing = document.getElementById(MODAL_ID);
    if (existing) existing.remove();

    const overlay = document.createElement('div');
    overlay.id = MODAL_ID;
    overlay.style.cssText = `
        position: fixed; inset: 0; background: rgba(0,0,0,0.65);
        display: flex; align-items: flex-start; justify-content: center;
        z-index: 1000; overflow-y: auto; padding: 40px 16px;
    `;

    const deptOptions = DEPARTMENTS.map(([code, name]) =>
        `<option value="${code}">${code} — ${escapeHtml(name)}</option>`
    ).join('');

    overlay.innerHTML = `
        <div style="
            background: var(--bg-elevated);
            border: 1px solid var(--border-default);
            border-radius: var(--radius-lg);
            box-shadow: 0 24px 64px rgba(0,0,0,0.5);
            width: 100%;
            max-width: 760px;
            padding: var(--space-2xl);
            position: relative;
        ">
            <!-- Header -->
            <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:var(--space-xl)">
                <h2 style="margin:0; font-size:var(--font-xl); font-weight:700; color:var(--text-primary)">
                    ➕ Ajouter une entreprise
                </h2>
                <button id="add-entity-close" style="
                    background:none; border:none; cursor:pointer;
                    color:var(--text-muted); font-size:1.5rem; line-height:1;
                    padding: 4px 8px; border-radius:var(--radius-sm);
                " title="Fermer">✕</button>
            </div>

            <!-- SIREN Lookup -->
            <div style="
                background: var(--bg-subtle);
                border: 1px solid var(--border-subtle);
                border-radius: var(--radius);
                padding: var(--space-lg);
                margin-bottom: var(--space-xl);
            ">
                <label style="display:block; font-size:var(--font-sm); font-weight:600; color:var(--text-secondary); margin-bottom:var(--space-sm)">
                    SIREN (optionnel — 9 chiffres)
                </label>
                <div style="display:flex; gap:var(--space-sm); align-items:center; flex-wrap:wrap">
                    <input type="text" id="ae-siren-input"
                        placeholder="Ex: 823456789"
                        maxlength="9"
                        style="
                            flex:1; min-width:180px;
                            background:var(--bg-input); border:1px solid var(--border-default);
                            border-radius:var(--radius-sm); color:var(--text-primary);
                            font-family:var(--font-family); font-size:var(--font-base);
                            padding:var(--space-sm) var(--space-md); outline:none;
                        "
                    >
                    <button id="ae-siren-lookup" class="btn btn-secondary" style="white-space:nowrap">
                        Chercher
                    </button>
                </div>
                <div id="ae-siren-banner" style="margin-top:var(--space-sm); display:none; font-size:var(--font-sm); padding:var(--space-sm) var(--space-md); border-radius:var(--radius-sm)"></div>
            </div>

            <!-- Form Fields -->
            <form id="ae-form" autocomplete="off">

                <!-- Two-column layout for main fields -->
                <div class="ae-modal-form-grid" style="gap:var(--space-lg) var(--space-xl); margin-bottom:var(--space-xl)">

                    <!-- Left column -->
                    <div>
                        <label class="ae-label">Nom commercial <span style="color:var(--danger)">*</span></label>
                        <input type="text" id="ae-denomination" class="ae-input" placeholder="Nom de l'entreprise">
                    </div>
                    <div>
                        <label class="ae-label">Enseigne</label>
                        <input type="text" id="ae-enseigne" class="ae-input" placeholder="Nom commercial">
                    </div>
                    <div>
                        <label class="ae-label">Téléphone</label>
                        <input type="tel" id="ae-phone" class="ae-input" placeholder="+33 4 68 12 34 56">
                    </div>
                    <div>
                        <label class="ae-label">Email</label>
                        <input type="email" id="ae-email" class="ae-input" placeholder="contact@exemple.fr">
                    </div>
                    <div>
                        <label class="ae-label">Site web</label>
                        <input type="text" id="ae-website" class="ae-input" placeholder="exemple.fr">
                    </div>
                    <div>
                        <label class="ae-label">Adresse</label>
                        <input type="text" id="ae-adresse" class="ae-input" placeholder="12 Rue de la Paix">
                    </div>
                    <div>
                        <label class="ae-label">Code postal</label>
                        <input type="text" id="ae-code-postal" class="ae-input" placeholder="66000">
                    </div>
                    <div>
                        <label class="ae-label">Ville</label>
                        <input type="text" id="ae-ville" class="ae-input" placeholder="PERPIGNAN">
                    </div>
                    <div style="grid-column:1 / -1">
                        <label class="ae-label">Département</label>
                        <select id="ae-departement" class="ae-input">
                            <option value="">— Sélectionner —</option>
                            ${deptOptions}
                        </select>
                    </div>
                </div>

                <!-- Social Links -->
                <div style="margin-bottom:var(--space-xl)">
                    <div style="font-size:var(--font-sm); font-weight:700; color:var(--text-secondary); text-transform:uppercase; letter-spacing:0.05em; margin-bottom:var(--space-md)">
                        Réseaux sociaux
                    </div>
                    <div style="display:grid; grid-template-columns:1fr 1fr; gap:var(--space-md) var(--space-xl)">
                        <div>
                            <label class="ae-label">LinkedIn</label>
                            <input type="url" id="ae-linkedin" class="ae-input" placeholder="https://linkedin.com/company/...">
                        </div>
                        <div>
                            <label class="ae-label">Facebook</label>
                            <input type="url" id="ae-facebook" class="ae-input" placeholder="https://facebook.com/...">
                        </div>
                        <div>
                            <label class="ae-label">Instagram</label>
                            <input type="url" id="ae-instagram" class="ae-input" placeholder="https://instagram.com/...">
                        </div>
                        <div>
                            <label class="ae-label">TikTok</label>
                            <input type="url" id="ae-tiktok" class="ae-input" placeholder="https://tiktok.com/@...">
                        </div>
                        <div>
                            <label class="ae-label">Twitter / X</label>
                            <input type="url" id="ae-twitter" class="ae-input" placeholder="https://x.com/...">
                        </div>
                        <div>
                            <label class="ae-label">WhatsApp</label>
                            <input type="url" id="ae-whatsapp" class="ae-input" placeholder="https://wa.me/...">
                        </div>
                        <div style="grid-column:1 / -1">
                            <label class="ae-label">YouTube</label>
                            <input type="url" id="ae-youtube" class="ae-input" placeholder="https://youtube.com/...">
                        </div>
                    </div>
                </div>

                <!-- Officers -->
                <div style="margin-bottom:var(--space-xl)">
                    <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:var(--space-md)">
                        <div style="font-size:var(--font-sm); font-weight:700; color:var(--text-secondary); text-transform:uppercase; letter-spacing:0.05em">
                            Dirigeant(s)
                        </div>
                        <button type="button" id="ae-add-officer" class="btn btn-secondary" style="font-size:var(--font-xs); padding:4px 10px">
                            ➕ Ajouter un dirigeant
                        </button>
                    </div>
                    <div id="ae-officers-list">
                        <!-- Officer rows injected here -->
                    </div>
                </div>

                <!-- Notes -->
                <div style="margin-bottom:var(--space-xl)">
                    <label class="ae-label">Notes</label>
                    <textarea id="ae-notes" class="ae-input" rows="3"
                        placeholder="Rencontré au salon, prospect chaud..."
                        style="resize:vertical; min-height:70px"
                    ></textarea>
                </div>

                <!-- Validation error -->
                <div id="ae-error" style="display:none; color:var(--danger); font-size:var(--font-sm); margin-bottom:var(--space-md); padding:var(--space-sm) var(--space-md); background:rgba(220,38,38,0.1); border-radius:var(--radius-sm)"></div>

                <!-- Submit -->
                <div style="display:flex; justify-content:flex-end; gap:var(--space-md)">
                    <button type="button" id="ae-cancel" class="btn btn-secondary">Annuler</button>
                    <button type="submit" id="ae-submit" class="btn btn-primary">Créer l'entreprise</button>
                </div>
            </form>
        </div>
    `;

    // Inject shared input styles
    const style = document.createElement('style');
    style.textContent = `
        .ae-label { display:block; font-size:var(--font-xs); font-weight:600; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.04em; margin-bottom:4px; }
        .ae-input { width:100%; box-sizing:border-box; background:var(--bg-input); border:1px solid var(--border-default); border-radius:var(--radius-sm); color:var(--text-primary); font-family:var(--font-family); font-size:var(--font-sm); padding:var(--space-sm) var(--space-md); outline:none; transition:border-color 0.15s; }
        .ae-input:focus { border-color:var(--accent); box-shadow:0 0 0 3px var(--accent-subtle); }
        .ae-input.ae-prefilled { background:var(--bg-subtle); color:var(--text-secondary); }
        .ae-officer-row { display:grid; grid-template-columns:1fr 1fr 1fr auto; gap:var(--space-sm); align-items:center; margin-bottom:var(--space-sm); }
    `;
    document.head.appendChild(style);

    document.body.appendChild(overlay);

    // State
    let linkedSirenData = null; // SIRENE record found by lookup
    let officerCount = 0;

    // ── Close handlers ────────────────────────────────────────────
    const close = () => {
        overlay.remove();
        style.remove();
    };

    document.getElementById('add-entity-close').addEventListener('click', close);
    document.getElementById('ae-cancel').addEventListener('click', close);
    overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });
    const onKey = (e) => { if (e.key === 'Escape') { close(); document.removeEventListener('keydown', onKey); } };
    document.addEventListener('keydown', onKey);

    // ── SIREN Lookup ──────────────────────────────────────────────
    const sirenInput = document.getElementById('ae-siren-input');
    const sirenBanner = document.getElementById('ae-siren-banner');

    function showBanner(html, color) {
        sirenBanner.innerHTML = html;
        sirenBanner.style.cssText = `
            margin-top:var(--space-sm); display:block; font-size:var(--font-sm);
            padding:var(--space-sm) var(--space-md); border-radius:var(--radius-sm);
            background:${color === 'green' ? 'rgba(34,197,94,0.12)' : 'rgba(234,179,8,0.12)'};
            color:${color === 'green' ? 'var(--success)' : 'var(--warning)'};
            border:1px solid ${color === 'green' ? 'rgba(34,197,94,0.3)' : 'rgba(234,179,8,0.3)'};
        `;
    }

    function prefillFromSirene(record) {
        const fields = {
            'ae-denomination': record.denomination,
            'ae-enseigne': record.enseigne,
            'ae-adresse': record.adresse,
            'ae-code-postal': record.code_postal,
            'ae-ville': record.ville,
        };
        for (const [id, val] of Object.entries(fields)) {
            const el = document.getElementById(id);
            if (el && val) {
                el.value = val;
                el.classList.add('ae-prefilled');
                el.readOnly = true;
            }
        }
        // Department
        const deptEl = document.getElementById('ae-departement');
        if (deptEl && record.departement) {
            deptEl.value = record.departement;
            deptEl.classList.add('ae-prefilled');
            deptEl.disabled = true;
        }
    }

    function clearPrefill() {
        ['ae-denomination','ae-enseigne','ae-adresse','ae-code-postal','ae-ville'].forEach(id => {
            const el = document.getElementById(id);
            if (el) { el.readOnly = false; el.classList.remove('ae-prefilled'); }
        });
        const deptEl = document.getElementById('ae-departement');
        if (deptEl) { deptEl.disabled = false; deptEl.classList.remove('ae-prefilled'); }
    }

    async function doSirenLookup() {
        const raw = sirenInput.value.trim().replace(/\s/g, '');

        // Reject MAPS IDs
        if (raw.toUpperCase().startsWith('MAPS')) {
            showBanner('⚠️ Utilisez un SIREN à 9 chiffres ou laissez vide', 'yellow');
            return;
        }

        if (raw.length !== 9 || !/^\d{9}$/.test(raw)) {
            showBanner('⚠️ Le SIREN doit contenir exactement 9 chiffres', 'yellow');
            return;
        }

        const lookupBtn = document.getElementById('ae-siren-lookup');
        lookupBtn.disabled = true;
        lookupBtn.textContent = '⏳';

        try {
            const result = await searchSirene(raw, { limit: 1, statut: null });
            const records = result?.results || result?.companies || [];
            // Find exact match
            const match = records.find(r => (r.siren || '').replace(/\s/g, '') === raw);

            if (match) {
                linkedSirenData = match;
                clearPrefill();
                prefillFromSirene(match);
                showBanner(`✅ SIRENE trouvé : ${escapeHtml(match.denomination || raw)}`, 'green');
            } else {
                linkedSirenData = null;
                clearPrefill();
                showBanner('⚠️ SIREN non trouvé dans la base — un identifiant MAPS sera attribué', 'yellow');
            }
        } catch {
            showBanner('⚠️ Erreur lors de la recherche SIRENE', 'yellow');
        } finally {
            lookupBtn.disabled = false;
            lookupBtn.textContent = 'Chercher';
        }
    }

    document.getElementById('ae-siren-lookup').addEventListener('click', doSirenLookup);
    sirenInput.addEventListener('keydown', (e) => { if (e.key === 'Enter') { e.preventDefault(); doSirenLookup(); } });

    // ── Officers ──────────────────────────────────────────────────
    function addOfficerRow() {
        officerCount++;
        const idx = officerCount;
        const row = document.createElement('div');
        row.className = 'ae-officer-row';
        row.dataset.officerIdx = idx;
        row.innerHTML = `
            <input type="text" class="ae-input ae-officer-nom" placeholder="Nom" data-idx="${idx}">
            <input type="text" class="ae-input ae-officer-prenom" placeholder="Prénom" data-idx="${idx}">
            <input type="text" class="ae-input ae-officer-role" placeholder="Rôle (ex: Gérant)" data-idx="${idx}">
            <button type="button" class="btn btn-secondary ae-officer-remove" data-idx="${idx}" style="padding:4px 10px; font-size:var(--font-xs); white-space:nowrap">✕</button>
        `;
        document.getElementById('ae-officers-list').appendChild(row);
        row.querySelector('.ae-officer-remove').addEventListener('click', () => row.remove());
    }

    document.getElementById('ae-add-officer').addEventListener('click', addOfficerRow);

    // ── Form Submission ───────────────────────────────────────────
    document.getElementById('ae-form').addEventListener('submit', async (e) => {
        e.preventDefault();

        const errorEl = document.getElementById('ae-error');
        errorEl.style.display = 'none';

        const sirenVal = sirenInput.value.trim().replace(/\s/g, '');
        const denomination = document.getElementById('ae-denomination').value.trim();

        // Validation
        if (!denomination && !sirenVal) {
            errorEl.textContent = "Nom d'entreprise requis si aucun SIREN n'est fourni";
            errorEl.style.display = 'block';
            return;
        }
        if (sirenVal && sirenVal.toUpperCase().startsWith('MAPS')) {
            errorEl.textContent = "Utilisez un SIREN à 9 chiffres ou laissez vide";
            errorEl.style.display = 'block';
            return;
        }

        // Collect officers
        const officers = [];
        document.querySelectorAll('.ae-officer-row').forEach(row => {
            const nom = row.querySelector('.ae-officer-nom')?.value.trim();
            const prenom = row.querySelector('.ae-officer-prenom')?.value.trim();
            const role = row.querySelector('.ae-officer-role')?.value.trim();
            if (nom || prenom) {
                officers.push({ nom: nom || '', prenom: prenom || '', role: role || '' });
            }
        });

        const payload = {
            denomination: denomination || undefined,
            enseigne: document.getElementById('ae-enseigne').value.trim() || undefined,
            phone: document.getElementById('ae-phone').value.trim() || undefined,
            email: document.getElementById('ae-email').value.trim() || undefined,
            website: document.getElementById('ae-website').value.trim() || undefined,
            adresse: document.getElementById('ae-adresse').value.trim() || undefined,
            code_postal: document.getElementById('ae-code-postal').value.trim() || undefined,
            ville: document.getElementById('ae-ville').value.trim() || undefined,
            departement: document.getElementById('ae-departement').value || undefined,
            social_linkedin: document.getElementById('ae-linkedin').value.trim() || undefined,
            social_facebook: document.getElementById('ae-facebook').value.trim() || undefined,
            social_instagram: document.getElementById('ae-instagram').value.trim() || undefined,
            social_tiktok: document.getElementById('ae-tiktok').value.trim() || undefined,
            social_twitter: document.getElementById('ae-twitter').value.trim() || undefined,
            social_whatsapp: document.getElementById('ae-whatsapp').value.trim() || undefined,
            social_youtube: document.getElementById('ae-youtube').value.trim() || undefined,
            notes: document.getElementById('ae-notes').value.trim() || undefined,
            officers: officers.length > 0 ? officers : undefined,
        };

        if (sirenVal && /^\d{9}$/.test(sirenVal)) {
            payload.siren = sirenVal;
        }

        // Remove undefined keys
        Object.keys(payload).forEach(k => { if (payload[k] === undefined) delete payload[k]; });

        const submitBtn = document.getElementById('ae-submit');
        submitBtn.disabled = true;
        submitBtn.textContent = '⏳ Création...';

        try {
            const result = await createEntity(payload);

            if (!result || result._ok === false) {
                const msg = result?.detail || result?.error || 'Erreur lors de la création';
                errorEl.textContent = msg;
                errorEl.style.display = 'block';
                return;
            }

            showToast(`Entreprise ajoutée : ${result.denomination || result.siren}`, 'success');
            close();
            if (onSuccess) onSuccess();

        } catch (err) {
            errorEl.textContent = 'Erreur de connexion au serveur';
            errorEl.style.display = 'block';
        } finally {
            submitBtn.disabled = false;
            submitBtn.textContent = "Créer l'entreprise";
        }
    });
}
