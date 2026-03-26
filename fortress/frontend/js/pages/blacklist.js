/**
 * Blacklist Management Page
 *
 * Shows all blacklisted SIRENs with company names.
 * Users can add new SIRENs to the blacklist or remove existing ones.
 */

import { extractApiError, getCachedUser } from '../api.js';
import { escapeHtml, showToast, showConfirmModal } from '../components.js';

const BASE = '/api/blacklist';

async function _fetchBlacklist(search = '') {
    const url = search ? `${BASE}?search=${encodeURIComponent(search)}` : BASE;
    const resp = await fetch(url, { credentials: 'same-origin' });
    if (!resp.ok) throw new Error(await resp.text());
    return resp.json();
}

async function _addToBlacklist(siren, reason) {
    const resp = await fetch(BASE, {
        method: 'POST',
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ siren, reason }),
    });
    if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        throw new Error(err.error || 'Erreur lors de l\'ajout.');
    }
    return resp.json();
}

async function _removeFromBlacklist(siren) {
    const resp = await fetch(`${BASE}/${encodeURIComponent(siren)}`, {
        method: 'DELETE',
        credentials: 'same-origin',
    });
    if (!resp.ok) throw new Error('Erreur lors de la suppression.');
    return resp.json();
}

function _formatDate(dateStr) {
    if (!dateStr) return '';
    const d = new Date(dateStr);
    return d.toLocaleDateString('fr-FR', { day: '2-digit', month: '2-digit', year: 'numeric' });
}

const INPUT_STYLE = `
    background: var(--bg-input);
    border: 1px solid var(--border-default);
    border-radius: var(--radius-sm);
    color: var(--text-primary);
    font-family: var(--font-family);
    font-size: var(--font-sm);
    padding: var(--space-sm) var(--space-md);
    outline: none;
    transition: border-color var(--transition-fast), box-shadow var(--transition-fast);
`;

function _renderTable(rows, container, onRemove) {
    if (!rows.length) {
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">🚫</div>
                <div class="empty-state-text">La liste noire est vide</div>
                <p style="color:var(--text-muted); font-size:var(--font-sm)">
                    Ajoutez des SIRENs ci-dessus pour les exclure des prochaines recherches
                </p>
            </div>
        `;
        return;
    }

    container.innerHTML = `
        <div style="overflow-x:auto">
            <table style="width:100%; border-collapse:collapse; font-size:var(--font-sm)">
                <thead>
                    <tr>
                        <th class="contacts-th">SIREN</th>
                        <th class="contacts-th">Entreprise</th>
                        <th class="contacts-th">Raison</th>
                        <th class="contacts-th">Ajouté par</th>
                        <th class="contacts-th">Date</th>
                        <th class="contacts-th"></th>
                    </tr>
                </thead>
                <tbody>
                    ${rows.map(r => `
                        <tr class="contacts-row">
                            <td class="contacts-td" style="font-family:var(--font-mono); color:var(--accent); font-weight:600; white-space:nowrap">${escapeHtml(r.siren)}</td>
                            <td class="contacts-td" style="font-weight:500; max-width:220px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap">${escapeHtml(r.denomination || '—')}</td>
                            <td class="contacts-td" style="max-width:200px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:var(--text-secondary)">${escapeHtml(r.reason || '—')}</td>
                            <td class="contacts-td" style="color:var(--text-muted)">${escapeHtml(r.added_by || '—')}</td>
                            <td class="contacts-td" style="color:var(--text-muted); white-space:nowrap">${_formatDate(r.added_at)}</td>
                            <td class="contacts-td">
                                <button class="action-btn action-btn-reject btn-remove-blacklist"
                                        data-siren="${escapeHtml(r.siren)}"
                                        title="Retirer de la liste noire">
                                    ✕
                                </button>
                            </td>
                        </tr>
                    `).join('')}
                </tbody>
            </table>
        </div>
    `;

    container.querySelectorAll('.btn-remove-blacklist').forEach(btn => {
        btn.addEventListener('click', () => onRemove(btn.dataset.siren));
    });
}

export async function renderBlacklist(el) {
    el.innerHTML = `
        <h1 class="page-title">🚫 Liste noire</h1>
        <p class="page-subtitle">Les entreprises listées ici sont automatiquement ignorées lors des prochaines recherches.</p>

        <div class="card" style="margin-bottom:var(--space-xl)">
            <h3 style="font-size:var(--font-sm); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">Ajouter à la liste noire</h3>
            <div style="display:flex; gap:var(--space-md); align-items:flex-end; flex-wrap:wrap">
                <div style="flex:0 0 180px">
                    <label style="display:block; font-size:var(--font-xs); font-weight:600; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.06em; margin-bottom:var(--space-xs)">SIREN</label>
                    <input type="text" id="bl-siren"
                        placeholder="ex : 123456789" maxlength="9"
                        style="${INPUT_STYLE} width:100%"
                        onfocus="this.style.borderColor='var(--accent)'; this.style.boxShadow='0 0 0 3px var(--accent-subtle)'"
                        onblur="this.style.borderColor='var(--border-default)'; this.style.boxShadow='none'">
                </div>
                <div style="flex:1; min-width:200px">
                    <label style="display:block; font-size:var(--font-xs); font-weight:600; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.06em; margin-bottom:var(--space-xs)">Raison</label>
                    <input type="text" id="bl-reason"
                        placeholder="ex : Concurrent, Déjà client..."
                        style="${INPUT_STYLE} width:100%"
                        onfocus="this.style.borderColor='var(--accent)'; this.style.boxShadow='0 0 0 3px var(--accent-subtle)'"
                        onblur="this.style.borderColor='var(--border-default)'; this.style.boxShadow='none'">
                </div>
                <div>
                    <button id="btn-add-blacklist" class="btn btn-primary">Ajouter</button>
                </div>
            </div>
        </div>

        <div class="card">
            <div style="display:flex; align-items:center; gap:var(--space-lg); margin-bottom:var(--space-lg); flex-wrap:wrap">
                <h3 style="font-size:var(--font-sm); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin:0; flex:1">SIRENs blacklistés</h3>
                <input type="text" id="bl-search"
                    placeholder="Rechercher par SIREN ou nom..."
                    style="${INPUT_STYLE} width:280px"
                    onfocus="this.style.borderColor='var(--accent)'; this.style.boxShadow='0 0 0 3px var(--accent-subtle)'"
                    onblur="this.style.borderColor='var(--border-default)'; this.style.boxShadow='none'">
            </div>
            <div id="bl-table-container">
                <div class="loading"><div class="spinner"></div></div>
            </div>
        </div>
    `;

    const tableContainer = el.querySelector('#bl-table-container');
    const searchInput = el.querySelector('#bl-search');
    const sirenInput = el.querySelector('#bl-siren');
    const reasonInput = el.querySelector('#bl-reason');
    const addBtn = el.querySelector('#btn-add-blacklist');

    let currentRows = [];

    async function load(search = '') {
        try {
            currentRows = await _fetchBlacklist(search);
            _renderTable(currentRows, tableContainer, handleRemove);
        } catch (err) {
            tableContainer.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">⚠️</div>
                    <div class="empty-state-text">Erreur de chargement</div>
                    <p style="color:var(--text-muted); font-size:var(--font-sm)">${escapeHtml(String(err.message))}</p>
                </div>
            `;
        }
    }

    async function handleRemove(siren) {
        showConfirmModal({
            title: '🗑️ Retirer de la liste noire',
            body: `<p>Retirer <strong>${escapeHtml(siren)}</strong> de la liste noire ?</p>
                   <p style="color:var(--text-muted); font-size:var(--font-xs)">Cette entreprise sera à nouveau visible dans les prochaines recherches.</p>`,
            confirmLabel: 'Retirer',
            danger: true,
            onConfirm: async () => {
                try {
                    await _removeFromBlacklist(siren);
                    showToast(`${siren} retiré de la liste noire.`, 'success');
                    await load(searchInput.value.trim());
                } catch (err) {
                    showToast(err.message, 'error');
                }
            },
        });
    }

    addBtn.addEventListener('click', async () => {
        const siren = sirenInput.value.trim();
        const reason = reasonInput.value.trim();
        if (!siren) {
            showToast('Veuillez saisir un SIREN.', 'error');
            return;
        }
        addBtn.disabled = true;
        try {
            await _addToBlacklist(siren, reason);
            showToast(`${siren} ajouté à la liste noire.`, 'success');
            sirenInput.value = '';
            reasonInput.value = '';
            await load(searchInput.value.trim());
        } catch (err) {
            showToast(err.message, 'error');
        } finally {
            addBtn.disabled = false;
        }
    });

    let _searchTimeout;
    searchInput.addEventListener('input', () => {
        clearTimeout(_searchTimeout);
        _searchTimeout = setTimeout(() => load(searchInput.value.trim()), 400);
    });

    await load();
}
