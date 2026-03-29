/**
 * Admin User Management Page
 *
 * Allows the admin to list, create, edit and deactivate user accounts.
 */

import {
    getAdminUsers,
    createAdminUser,
    updateAdminUser,
    deactivateAdminUser,
    getWorkspaces,
    extractApiError,
    getCachedUser,
} from '../api.js';
import { escapeHtml, showToast, showConfirmModal, formatDateTime } from '../components.js';
import { isStale } from '../app.js';

const INPUT_STYLE = `
    background: var(--bg-input);
    border: 1px solid var(--border-default);
    color: var(--text-primary);
    padding: var(--space-sm) var(--space-md);
    border-radius: var(--radius-sm);
    font-size: var(--font-sm);
    width: 100%;
    outline: none;
    font-family: var(--font-family);
    transition: border-color var(--transition-fast), box-shadow var(--transition-fast);
`;

const LABEL_STYLE = `
    display: block;
    font-size: var(--font-xs);
    font-weight: 600;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.06em;
    margin-bottom: var(--space-xs);
`;

function _roleFr(role) {
    if (role === 'admin') return 'Administrateur';
    if (role === 'head') return 'Responsable';
    return 'Utilisateur';
}

function _roleBadge(role) {
    if (role === 'admin') {
        return `<span class="badge" style="background:rgba(139,92,246,0.15); color:#a78bfa; border:1px solid rgba(139,92,246,0.3)">Administrateur</span>`;
    }
    if (role === 'head') {
        return `<span class="badge" style="background:rgba(16,185,129,0.15); color:#34d399; border:1px solid rgba(16,185,129,0.3)">Responsable</span>`;
    }
    return `<span class="badge" style="background:var(--bg-subtle); color:var(--text-muted); border:1px solid var(--border-default)">Utilisateur</span>`;
}

function _focusStyle(el) {
    el.addEventListener('focus', () => {
        el.style.borderColor = 'var(--accent)';
        el.style.boxShadow = '0 0 0 3px var(--accent-subtle)';
    });
    el.addEventListener('blur', () => {
        el.style.borderColor = 'var(--border-default)';
        el.style.boxShadow = 'none';
    });
}

export async function renderAdmin(container, gen) {
    container.innerHTML = `
        <h1 class="page-title">Gestion des utilisateurs</h1>
        <p class="page-subtitle">Gérer les comptes, rôles et accès des utilisateurs.</p>

        <div id="admin-form-area"></div>

        <div class="card">
            <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:var(--space-lg); flex-wrap:wrap; gap:var(--space-md)">
                <h3 style="font-size:var(--font-sm); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin:0">
                    Comptes utilisateurs
                </h3>
                <button id="btn-add-user" class="btn btn-primary" style="display:flex; align-items:center; gap:var(--space-xs)">
                    <span>+</span> <span>Ajouter un utilisateur</span>
                </button>
            </div>
            <div id="admin-table-container">
                <div class="loading"><div class="spinner"></div></div>
            </div>
        </div>
    `;

    const formArea = container.querySelector('#admin-form-area');
    const tableContainer = container.querySelector('#admin-table-container');
    const addBtn = container.querySelector('#btn-add-user');

    let _workspaces = [];
    let _editingUserId = null;

    // Load workspaces once
    try {
        const wsData = await getWorkspaces();
        if (isStale(gen)) return;
        _workspaces = (wsData && wsData.workspaces) ? wsData.workspaces : [];
    } catch {
        _workspaces = [];
    }

    async function loadUsers() {
        tableContainer.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
        try {
            const data = await getAdminUsers();
            if (isStale(gen)) return;
            if (!data || !data._ok) {
                tableContainer.innerHTML = `<div class="empty-state"><div class="empty-state-text">Erreur : ${escapeHtml(extractApiError(data))}</div></div>`;
                return;
            }
            renderTable(data.users || []);
        } catch (err) {
            if (isStale(gen)) return;
            tableContainer.innerHTML = `<div class="empty-state"><div class="empty-state-icon">⚠️</div><div class="empty-state-text">Erreur de chargement</div></div>`;
        }
    }

    function renderTable(users) {
        const me = getCachedUser();
        if (!users.length) {
            tableContainer.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">👤</div>
                    <div class="empty-state-text">Aucun utilisateur</div>
                </div>
            `;
            return;
        }

        tableContainer.innerHTML = `
            <div style="overflow-x:auto">
                <table style="width:100%; border-collapse:collapse; font-size:var(--font-sm)">
                    <thead>
                        <tr>
                            <th class="contacts-th">Utilisateur</th>
                            <th class="contacts-th">Nom affiché</th>
                            <th class="contacts-th">Rôle</th>
                            <th class="contacts-th">Espace de travail</th>
                            <th class="contacts-th">Dernière connexion</th>
                            <th class="contacts-th"></th>
                        </tr>
                    </thead>
                    <tbody>
                        ${users.map(u => `
                            <tr class="contacts-row" data-user-id="${u.id}">
                                <td class="contacts-td" style="font-family:var(--font-mono); color:var(--accent); font-weight:600; white-space:nowrap">
                                    ${escapeHtml(u.username)}
                                </td>
                                <td class="contacts-td" style="color:var(--text-secondary)">
                                    ${escapeHtml(u.display_name || '—')}
                                </td>
                                <td class="contacts-td">
                                    ${_roleBadge(u.role)}
                                </td>
                                <td class="contacts-td" style="color:var(--text-secondary)">
                                    ${escapeHtml(u.workspace_name || '—')}
                                </td>
                                <td class="contacts-td" style="color:var(--text-muted); white-space:nowrap">
                                    ${u.last_login ? formatDateTime(u.last_login) : 'Jamais'}
                                </td>
                                <td class="contacts-td">
                                    <div style="display:flex; gap:var(--space-sm); align-items:center">
                                        <button class="action-btn btn-edit-user"
                                                data-user-id="${u.id}"
                                                title="Modifier cet utilisateur"
                                                style="font-size:14px">
                                            ✏️
                                        </button>
                                        ${me && me.username !== u.username ? `
                                        <button class="action-btn action-btn-reject btn-deactivate-user"
                                                data-user-id="${u.id}"
                                                data-username="${escapeHtml(u.username)}"
                                                title="Désactiver ce compte"
                                                style="font-size:14px">
                                            🚫
                                        </button>
                                        ` : ''}
                                    </div>
                                </td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        `;

        // Store user data for edit forms
        const userMap = {};
        users.forEach(u => { userMap[u.id] = u; });

        tableContainer.querySelectorAll('.btn-edit-user').forEach(btn => {
            btn.addEventListener('click', () => {
                const uid = parseInt(btn.dataset.userId, 10);
                showForm(userMap[uid]);
            });
        });

        tableContainer.querySelectorAll('.btn-deactivate-user').forEach(btn => {
            btn.addEventListener('click', () => {
                const uid = parseInt(btn.dataset.userId, 10);
                const uname = btn.dataset.username;
                handleDeactivate(uid, uname);
            });
        });
    }

    function _wsOptions(selectedId) {
        let opts = `<option value="">— Aucun —</option>`;
        _workspaces.forEach(ws => {
            const sel = ws.id === selectedId ? 'selected' : '';
            opts += `<option value="${ws.id}" ${sel}>${escapeHtml(ws.name)}</option>`;
        });
        return opts;
    }

    function showForm(user = null) {
        _editingUserId = user ? user.id : null;
        const isEdit = !!user;
        const title = isEdit ? `Modifier l'utilisateur — ${escapeHtml(user.username)}` : 'Ajouter un utilisateur';

        formArea.innerHTML = `
            <div class="card" style="margin-bottom:var(--space-xl); border:1px solid var(--accent); border-opacity:0.4">
                <h3 style="font-size:var(--font-sm); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                    ${escapeHtml(title)}
                </h3>
                <div style="display:grid; grid-template-columns:repeat(auto-fill, minmax(200px, 1fr)); gap:var(--space-lg); margin-bottom:var(--space-lg)">
                    <div>
                        <label style="${LABEL_STYLE}">Identifiant ${!isEdit ? '*' : ''}</label>
                        <input type="text" id="f-username" placeholder="identifiant"
                               value="${isEdit ? escapeHtml(user.username) : ''}"
                               style="${INPUT_STYLE}">
                    </div>
                    <div>
                        <label style="${LABEL_STYLE}">Nom affiché</label>
                        <input type="text" id="f-display-name" placeholder="Prénom Nom"
                               value="${isEdit ? escapeHtml(user.display_name || '') : ''}"
                               style="${INPUT_STYLE}">
                    </div>
                    <div>
                        <label style="${LABEL_STYLE}">Mot de passe ${!isEdit ? '*' : ''}</label>
                        <input type="password" id="f-password"
                               placeholder="${isEdit ? 'Laisser vide pour ne pas modifier' : 'Mot de passe'}"
                               style="${INPUT_STYLE}">
                    </div>
                    <div>
                        <label style="${LABEL_STYLE}">Rôle</label>
                        <select id="f-role" style="${INPUT_STYLE}">
                            <option value="user" ${(!isEdit || user.role === 'user') ? 'selected' : ''}>Utilisateur</option>
                            <option value="head" ${(isEdit && user.role === 'head') ? 'selected' : ''}>Responsable</option>
                            <option value="admin" ${(isEdit && user.role === 'admin') ? 'selected' : ''}>Administrateur</option>
                        </select>
                    </div>
                    <div>
                        <label style="${LABEL_STYLE}">Espace de travail</label>
                        <select id="f-workspace" style="${INPUT_STYLE}">
                            ${_wsOptions(isEdit ? user.workspace_id : null)}
                        </select>
                    </div>
                </div>
                <div style="display:flex; gap:var(--space-md)">
                    <button id="btn-save-user" class="btn btn-primary">Enregistrer</button>
                    <button id="btn-cancel-form" class="btn">Annuler</button>
                </div>
            </div>
        `;

        // Apply focus styles
        ['f-username', 'f-display-name', 'f-password', 'f-role', 'f-workspace'].forEach(id => {
            const el = formArea.querySelector(`#${id}`);
            if (el) _focusStyle(el);
        });

        formArea.querySelector('#btn-cancel-form').addEventListener('click', () => {
            formArea.innerHTML = '';
            _editingUserId = null;
        });

        formArea.querySelector('#btn-save-user').addEventListener('click', () => handleSave(isEdit));
    }

    async function handleSave(isEdit) {
        const saveBtn = formArea.querySelector('#btn-save-user');
        if (saveBtn) saveBtn.disabled = true;

        const username = (formArea.querySelector('#f-username')?.value || '').trim();
        const displayName = (formArea.querySelector('#f-display-name')?.value || '').trim();
        const password = (formArea.querySelector('#f-password')?.value || '');
        const role = formArea.querySelector('#f-role')?.value || 'user';
        const workspaceRaw = formArea.querySelector('#f-workspace')?.value || '';
        const workspaceId = workspaceRaw ? parseInt(workspaceRaw, 10) : null;

        try {
            let result;
            if (isEdit) {
                // Only send fields that were actually provided
                const payload = {};
                if (username) payload.username = username;
                if (displayName !== undefined) payload.display_name = displayName;
                if (password) payload.password = password;
                payload.role = role;
                payload.workspace_id = workspaceId;

                result = await updateAdminUser(_editingUserId, payload);
            } else {
                if (!username) {
                    showToast("L'identifiant est obligatoire.", 'error');
                    if (saveBtn) saveBtn.disabled = false;
                    return;
                }
                if (!password) {
                    showToast('Le mot de passe est obligatoire.', 'error');
                    if (saveBtn) saveBtn.disabled = false;
                    return;
                }
                result = await createAdminUser({
                    username,
                    display_name: displayName || username,
                    password,
                    role,
                    workspace_id: workspaceId,
                });
            }

            if (isStale(gen)) return;

            if (!result || !result._ok) {
                showToast(extractApiError(result), 'error');
                if (saveBtn) saveBtn.disabled = false;
                return;
            }

            showToast(isEdit ? 'Utilisateur mis à jour.' : 'Utilisateur créé.', 'success');
            formArea.innerHTML = '';
            _editingUserId = null;
            await loadUsers();
        } catch (err) {
            if (isStale(gen)) return;
            showToast(err.message || 'Erreur inattendue.', 'error');
            if (saveBtn) saveBtn.disabled = false;
        }
    }

    async function handleDeactivate(userId, username) {
        showConfirmModal({
            title: 'Désactiver le compte',
            body: `<p>Désactiver le compte de <strong>${escapeHtml(username)}</strong> ?</p>
                   <p style="color:var(--text-muted); font-size:var(--font-xs)">L'utilisateur ne pourra plus se connecter. Cette action est réversible depuis la base de données.</p>`,
            confirmLabel: 'Désactiver',
            danger: true,
            onConfirm: async () => {
                const result = await deactivateAdminUser(userId);
                if (isStale(gen)) return;
                if (!result || !result._ok) {
                    showToast(extractApiError(result), 'error');
                    return;
                }
                showToast(`Compte de ${username} désactivé.`, 'success');
                await loadUsers();
            },
        });
    }

    addBtn.addEventListener('click', () => showForm(null));

    await loadUsers();
}
