/**
 * Admin User Management Page
 *
 * Allows the admin to list, create, edit and deactivate user accounts,
 * and manage workspaces.
 */

import {
    getAdminUsers,
    createAdminUser,
    updateAdminUser,
    deactivateAdminUser,
    getWorkspaces,
    createWorkspace,
    updateWorkspace,
    deleteWorkspace,
    getActivityLog,
    getSystemLog,
    clearSystemLog,
    extractApiError,
    getCachedUser,
    getOppositions,
    submitOpposition,
    confirmOppositionDeletion,
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
    const currentUser = getCachedUser();
    if (!currentUser || currentUser.role !== 'admin') {
        container.innerHTML = `
            <div class="empty-state" style="margin-top:var(--space-2xl)">
                <div class="empty-state-icon">🔒</div>
                <div class="empty-state-text">Accès réservé aux administrateurs</div>
                <a href="#/" class="btn btn-primary" style="margin-top:var(--space-lg)">Retour au Dashboard</a>
            </div>
        `;
        return;
    }

    container.innerHTML = `
        <style>
            .admin-tabs { display: flex; gap: 0; border-bottom: 1px solid var(--border-default); margin-bottom: var(--space-lg); }
            .admin-tab { padding: 10px 20px; background: none; border: none; color: var(--text-muted); cursor: pointer; font-size: var(--font-sm); font-weight: 600; border-bottom: 2px solid transparent; transition: all 0.2s; }
            .admin-tab:hover { color: var(--text-primary); }
            .admin-tab.active { color: var(--accent); border-bottom-color: var(--accent); }
        </style>

        <h1 class="page-title">Administration</h1>
        <p class="page-subtitle">Gestion des utilisateurs, des espaces de travail et conformité RGPD.</p>

        <div class="admin-tabs">
            <button class="admin-tab active" data-tab="workspaces">Espaces de travail</button>
            <button class="admin-tab" data-tab="users">Utilisateurs</button>
            <button class="admin-tab" data-tab="logs">Journaux</button>
            <button class="admin-tab" data-tab="rgpd">RGPD</button>
        </div>

        <!-- TAB: Espaces de travail -->
        <div id="tab-workspaces" class="admin-tab-content" style="display:block">
            <div id="ws-form-area"></div>

            <div class="card" style="margin-bottom:var(--space-xl)">
                <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:var(--space-lg)">
                    <h3 style="font-size:var(--font-sm); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin:0">
                        Espaces de travail
                    </h3>
                    <button id="btn-add-ws" class="btn btn-primary" style="display:flex; align-items:center; gap:var(--space-xs)">
                        <span>+</span> <span>Ajouter un espace</span>
                    </button>
                </div>
                <div id="ws-table-container">
                    <div class="loading"><div class="spinner"></div></div>
                </div>
            </div>
        </div>

        <!-- TAB: Utilisateurs -->
        <div id="tab-users" class="admin-tab-content" style="display:none">
            <div id="admin-form-area"></div>

            <div class="card" style="margin-bottom:var(--space-xl)">
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
        </div>

        <!-- TAB: Journaux -->
        <div id="tab-logs" class="admin-tab-content" style="display:none">
            <!-- Activity Log section -->
            <div class="card" style="margin-bottom:var(--space-xl)">
                <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:var(--space-lg); flex-wrap:wrap; gap:var(--space-md)">
                    <h3 style="font-size:var(--font-sm); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin:0">
                        📋 Journal d'activité
                    </h3>
                    <div style="display:flex; gap:var(--space-sm); flex-wrap:wrap">
                        <button class="view-toggle-btn log-period active" data-period="day">Aujourd'hui</button>
                        <button class="view-toggle-btn log-period" data-period="week">Cette semaine</button>
                        <button class="view-toggle-btn log-period" data-period="month">Ce mois</button>
                        <button class="view-toggle-btn log-period" data-period="all">Tout</button>
                    </div>
                </div>
                <div id="log-container">
                    <div class="loading"><div class="spinner"></div></div>
                </div>
                <div id="log-pagination" style="margin-top:var(--space-lg)"></div>
            </div>

            <!-- System Error Log section -->
            <div class="card">
                <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:var(--space-lg); flex-wrap:wrap; gap:var(--space-md)">
                    <h3 style="font-size:var(--font-sm); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin:0">
                        🔧 Journal système
                    </h3>
                    <div style="display:flex; gap:var(--space-sm); flex-wrap:wrap; align-items:center">
                        <button class="view-toggle-btn syslog-level active" data-level="all">Tout</button>
                        <button class="view-toggle-btn syslog-level" data-level="error">Erreurs</button>
                        <button class="view-toggle-btn syslog-level" data-level="warning">Avertissements</button>
                        <span style="color:var(--border-default)">|</span>
                        <button class="view-toggle-btn syslog-period active" data-period="week">Semaine</button>
                        <button class="view-toggle-btn syslog-period" data-period="month">Mois</button>
                        <button class="view-toggle-btn syslog-period" data-period="all">Tout</button>
                        <button id="btn-clear-syslog" class="btn btn-secondary" style="font-size:var(--font-xs); padding:var(--space-xs) var(--space-sm); color:var(--text-muted)" title="Nettoyer les entrées de plus de 7 jours">🗑️ Nettoyer</button>
                    </div>
                </div>
                <div id="syslog-container">
                    <div class="loading"><div class="spinner"></div></div>
                </div>
                <div id="syslog-pagination" style="margin-top:var(--space-lg)"></div>
            </div>
        </div>

        <!-- TAB: RGPD -->
        <div id="tab-rgpd" class="admin-tab-content" style="display:none">

            <!-- Section A: Droit d'opposition -->
            <div class="card" style="margin-bottom:var(--space-lg)">
                <div class="card-header">
                    <h3>🛡️ Droit d'opposition — Suppression de données personnelles</h3>
                </div>
                <div class="card-body">
                    <p style="color:var(--text-muted); margin-bottom:var(--space-md); font-size:var(--font-sm)">
                        Lorsqu'une personne exerce son droit d'opposition (Art. 21 RGPD), saisissez ses coordonnées ci-dessous.
                        Le système supprimera ses données personnelles (dirigeants, emails, téléphones) de la base.
                    </p>
                    <form id="rgpd-opposition-form" style="display:grid; grid-template-columns:1fr 1fr; gap:var(--space-sm)">
                        <div>
                            <label style="font-size:var(--font-xs); color:var(--text-muted)">Nom</label>
                            <input type="text" id="rgpd-nom" class="form-input" placeholder="Dupont" style="${INPUT_STYLE}">
                        </div>
                        <div>
                            <label style="font-size:var(--font-xs); color:var(--text-muted)">Prénom</label>
                            <input type="text" id="rgpd-prenom" class="form-input" placeholder="Jean" style="${INPUT_STYLE}">
                        </div>
                        <div>
                            <label style="font-size:var(--font-xs); color:var(--text-muted)">Email *</label>
                            <input type="email" id="rgpd-email" class="form-input" placeholder="jean.dupont@example.com" style="${INPUT_STYLE}">
                        </div>
                        <div>
                            <label style="font-size:var(--font-xs); color:var(--text-muted)">Téléphone</label>
                            <input type="text" id="rgpd-phone" class="form-input" placeholder="0612345678" style="${INPUT_STYLE}">
                        </div>
                        <div style="grid-column:1/-1">
                            <label style="font-size:var(--font-xs); color:var(--text-muted)">Motif *</label>
                            <input type="text" id="rgpd-motif" class="form-input" placeholder="Demande par email du 30/03/2026" required style="${INPUT_STYLE}">
                        </div>
                        <div style="grid-column:1/-1">
                            <button type="submit" class="btn btn-primary">Traiter la demande</button>
                        </div>
                    </form>
                    <div id="rgpd-result" style="display:none; margin-top:var(--space-md)"></div>

                    <!-- Name candidates for confirmation -->
                    <div id="rgpd-candidates" style="display:none; margin-top:var(--space-md)">
                        <h4 style="margin-bottom:var(--space-sm)">Correspondances par nom (vérification manuelle)</h4>
                        <div id="rgpd-candidates-list"></div>
                    </div>
                </div>
            </div>

            <!-- Opposition history -->
            <div class="card" style="margin-bottom:var(--space-lg)">
                <div class="card-header">
                    <h3>📋 Historique des demandes d'opposition</h3>
                </div>
                <div class="card-body">
                    <div id="rgpd-history-container">
                        <div class="loading"><div class="spinner"></div></div>
                    </div>
                </div>
            </div>

            <!-- Section B: Registre des traitements -->
            <div class="card">
                <div class="card-header">
                    <h3>📑 Registre des traitements (Article 30 RGPD)</h3>
                </div>
                <div class="card-body">

                    <div style="background:var(--bg-secondary); border-radius:var(--radius-md); padding:var(--space-md); margin-bottom:var(--space-md)">
                        <h4 style="margin-bottom:var(--space-sm)">1. Prospection commerciale B2B</h4>
                        <div style="display:grid; grid-template-columns:180px 1fr; gap:4px 12px; font-size:var(--font-sm)">
                            <span style="color:var(--text-muted); font-weight:600">Finalité</span>
                            <span>Collecte et enrichissement de données d'entreprises pour la prospection B2B</span>
                            <span style="color:var(--text-muted); font-weight:600">Base légale</span>
                            <span>Intérêt légitime (Art. 6.1.f RGPD)</span>
                            <span style="color:var(--text-muted); font-weight:600">Données</span>
                            <span>Raisons sociales, SIREN, adresses, noms de dirigeants, emails pro, téléphones, sites web</span>
                            <span style="color:var(--text-muted); font-weight:600">Sources</span>
                            <span>Registre SIRENE (INSEE), Google Maps, sites web d'entreprises</span>
                            <span style="color:var(--text-muted); font-weight:600">Personnes concernées</span>
                            <span>Dirigeants et contacts d'entreprises françaises</span>
                            <span style="color:var(--text-muted); font-weight:600">Destinataires</span>
                            <span>Administrateur, responsables d'espace de travail</span>
                            <span style="color:var(--text-muted); font-weight:600">Conservation</span>
                            <span>Tant que l'entreprise est active au registre SIRENE</span>
                            <span style="color:var(--text-muted); font-weight:600">Sécurité</span>
                            <span>Chiffrement en transit (HTTPS), base de données en EU, contrôle d'accès par rôle</span>
                        </div>
                    </div>

                    <div style="background:var(--bg-secondary); border-radius:var(--radius-md); padding:var(--space-md); margin-bottom:var(--space-md)">
                        <h4 style="margin-bottom:var(--space-sm)">2. Gestion des comptes utilisateurs</h4>
                        <div style="display:grid; grid-template-columns:180px 1fr; gap:4px 12px; font-size:var(--font-sm)">
                            <span style="color:var(--text-muted); font-weight:600">Finalité</span>
                            <span>Authentification et gestion des accès à la plateforme</span>
                            <span style="color:var(--text-muted); font-weight:600">Base légale</span>
                            <span>Exécution du contrat (Art. 6.1.b)</span>
                            <span style="color:var(--text-muted); font-weight:600">Données</span>
                            <span>Identifiant, mot de passe hashé (bcrypt), nom d'affichage, rôle</span>
                            <span style="color:var(--text-muted); font-weight:600">Personnes concernées</span>
                            <span>Utilisateurs de la plateforme</span>
                            <span style="color:var(--text-muted); font-weight:600">Destinataires</span>
                            <span>Administrateur</span>
                            <span style="color:var(--text-muted); font-weight:600">Conservation</span>
                            <span>Durée de vie du compte</span>
                            <span style="color:var(--text-muted); font-weight:600">Sécurité</span>
                            <span>Mots de passe hashés (bcrypt), sessions signées, cookies HttpOnly</span>
                        </div>
                    </div>

                    <div style="background:var(--bg-secondary); border-radius:var(--radius-md); padding:var(--space-md); margin-bottom:var(--space-md)">
                        <h4 style="margin-bottom:var(--space-sm)">3. Formulaire de contact</h4>
                        <div style="display:grid; grid-template-columns:180px 1fr; gap:4px 12px; font-size:var(--font-sm)">
                            <span style="color:var(--text-muted); font-weight:600">Finalité</span>
                            <span>Traitement des demandes de prospects</span>
                            <span style="color:var(--text-muted); font-weight:600">Base légale</span>
                            <span>Consentement (Art. 6.1.a)</span>
                            <span style="color:var(--text-muted); font-weight:600">Données</span>
                            <span>Nom, email, entreprise, message</span>
                            <span style="color:var(--text-muted); font-weight:600">Personnes concernées</span>
                            <span>Visiteurs du site</span>
                            <span style="color:var(--text-muted); font-weight:600">Destinataires</span>
                            <span>Administrateur</span>
                            <span style="color:var(--text-muted); font-weight:600">Conservation</span>
                            <span>12 mois</span>
                            <span style="color:var(--text-muted); font-weight:600">Sécurité</span>
                            <span>Limitation de débit (5 demandes/jour/email), notification par email</span>
                        </div>
                    </div>

                    <div style="background:var(--bg-secondary); border-radius:var(--radius-md); padding:var(--space-md); margin-bottom:var(--space-md)">
                        <h4 style="margin-bottom:var(--space-sm)">4. Journal d'activité</h4>
                        <div style="display:grid; grid-template-columns:180px 1fr; gap:4px 12px; font-size:var(--font-sm)">
                            <span style="color:var(--text-muted); font-weight:600">Finalité</span>
                            <span>Audit de sécurité et traçabilité des actions</span>
                            <span style="color:var(--text-muted); font-weight:600">Base légale</span>
                            <span>Intérêt légitime (Art. 6.1.f)</span>
                            <span style="color:var(--text-muted); font-weight:600">Données</span>
                            <span>Actions effectuées, horodatage, identifiant utilisateur</span>
                            <span style="color:var(--text-muted); font-weight:600">Personnes concernées</span>
                            <span>Utilisateurs de la plateforme</span>
                            <span style="color:var(--text-muted); font-weight:600">Destinataires</span>
                            <span>Administrateur, responsables d'espace de travail (périmètre limité)</span>
                            <span style="color:var(--text-muted); font-weight:600">Conservation</span>
                            <span>12 mois</span>
                            <span style="color:var(--text-muted); font-weight:600">Sécurité</span>
                            <span>Accès restreint par rôle</span>
                        </div>
                    </div>

                </div>
            </div>
        </div>
    `;

    // ── Tab switching ────────────────────────────────────────────────
    container.querySelectorAll('.admin-tab').forEach(btn => {
        btn.addEventListener('click', () => {
            container.querySelectorAll('.admin-tab').forEach(b => b.classList.remove('active'));
            container.querySelectorAll('.admin-tab-content').forEach(c => { c.style.display = 'none'; c.classList.remove('active'); });
            btn.classList.add('active');
            const target = document.getElementById('tab-' + btn.dataset.tab);
            if (target) { target.style.display = 'block'; target.classList.add('active'); }
        });
    });

    const wsFormArea = container.querySelector('#ws-form-area');
    const wsTableContainer = container.querySelector('#ws-table-container');
    const addWsBtn = container.querySelector('#btn-add-ws');

    const formArea = container.querySelector('#admin-form-area');
    const tableContainer = container.querySelector('#admin-table-container');
    const addBtn = container.querySelector('#btn-add-user');

    const logContainer = container.querySelector('#log-container');
    const logPagination = container.querySelector('#log-pagination');

    let _workspaces = [];
    let _editingUserId = null;
    let _logPeriod = 'day';
    let _logOffset = 0;
    const LOG_PAGE_SIZE = 30;

    // ── Workspace Management ────────────────────────────────────────

    async function loadWorkspaces() {
        wsTableContainer.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
        try {
            const data = await getWorkspaces();
            if (!data || !data._ok) {
                wsTableContainer.innerHTML = `<div class="empty-state"><div class="empty-state-text">Erreur : ${escapeHtml(extractApiError(data))}</div></div>`;
                return;
            }
            _workspaces = data.workspaces || [];
            renderWsTable(_workspaces);
        } catch (err) {
            wsTableContainer.innerHTML = `<div class="empty-state"><div class="empty-state-icon">⚠️</div><div class="empty-state-text">Impossible de charger les espaces de travail.</div></div>`;
        }
    }

    function renderWsTable(workspaces) {
        if (!workspaces.length) {
            wsTableContainer.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">🏢</div>
                    <div class="empty-state-text">Aucun espace de travail.</div>
                </div>
            `;
            return;
        }

        wsTableContainer.innerHTML = `
            <div style="overflow-x:auto">
                <table style="width:100%; border-collapse:collapse; font-size:var(--font-sm)">
                    <thead>
                        <tr>
                            <th class="contacts-th">Nom</th>
                            <th class="contacts-th">Responsable</th>
                            <th class="contacts-th">Utilisateurs</th>
                            <th class="contacts-th">Créé le</th>
                            <th class="contacts-th"></th>
                        </tr>
                    </thead>
                    <tbody>
                        ${workspaces.map(ws => `
                            <tr class="contacts-row" data-ws-id="${ws.id}">
                                <td class="contacts-td" style="font-weight:600; color:var(--text-primary)">
                                    ${escapeHtml(ws.name)}
                                </td>
                                <td class="contacts-td" style="color:var(--text-secondary)">
                                    ${ws.head_username ? escapeHtml(ws.head_username) : '<span style="color:var(--text-muted)">—</span>'}
                                </td>
                                <td class="contacts-td" style="color:var(--text-secondary)">
                                    ${ws.user_count}
                                </td>
                                <td class="contacts-td" style="color:var(--text-muted); white-space:nowrap">
                                    ${ws.created_at ? formatDateTime(ws.created_at) : '—'}
                                </td>
                                <td class="contacts-td">
                                    <div style="display:flex; gap:var(--space-sm); align-items:center">
                                        <button class="action-btn btn-edit-ws"
                                                data-ws-id="${ws.id}"
                                                title="Renommer cet espace"
                                                style="font-size:14px">
                                            ✏️
                                        </button>
                                        <button class="action-btn action-btn-reject btn-delete-ws"
                                                data-ws-id="${ws.id}"
                                                data-ws-name="${escapeHtml(ws.name)}"
                                                data-user-count="${ws.user_count}"
                                                title="Supprimer cet espace"
                                                style="font-size:14px">
                                            🗑️
                                        </button>
                                    </div>
                                </td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        `;

        const wsMap = {};
        workspaces.forEach(ws => { wsMap[ws.id] = ws; });

        wsTableContainer.querySelectorAll('.btn-edit-ws').forEach(btn => {
            btn.addEventListener('click', () => {
                const wsId = parseInt(btn.dataset.wsId, 10);
                showWsForm(wsMap[wsId]);
            });
        });

        wsTableContainer.querySelectorAll('.btn-delete-ws').forEach(btn => {
            btn.addEventListener('click', () => {
                const wsId = parseInt(btn.dataset.wsId, 10);
                const wsName = btn.dataset.wsName;
                const userCount = parseInt(btn.dataset.userCount, 10);
                handleDeleteWs(wsId, wsName, userCount);
            });
        });
    }

    function showWsForm(ws = null) {
        const isEdit = !!ws;
        const title = isEdit ? `Renommer l'espace — ${escapeHtml(ws.name)}` : 'Nouvel espace de travail';

        wsFormArea.innerHTML = `
            <div class="card" style="margin-bottom:var(--space-xl); border:1px solid var(--accent); border-opacity:0.4">
                <h3 style="font-size:var(--font-sm); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                    ${escapeHtml(title)}
                </h3>
                <div style="display:grid; grid-template-columns:repeat(auto-fill, minmax(200px, 1fr)); gap:var(--space-lg); margin-bottom:var(--space-lg)">
                    <div>
                        <label style="${LABEL_STYLE}">Nom *</label>
                        <input type="text" id="f-ws-name" placeholder="Nom de l'espace"
                               value="${isEdit ? escapeHtml(ws.name) : ''}"
                               style="${INPUT_STYLE}">
                    </div>
                </div>
                <div style="display:flex; gap:var(--space-md)">
                    <button id="btn-save-ws" class="btn btn-primary">${isEdit ? 'Enregistrer' : 'Créer l\'espace'}</button>
                    <button id="btn-cancel-ws-form" class="btn">Annuler</button>
                </div>
            </div>
        `;

        const nameInput = wsFormArea.querySelector('#f-ws-name');
        if (nameInput) {
            _focusStyle(nameInput);
            nameInput.focus();
        }

        wsFormArea.querySelector('#btn-cancel-ws-form').addEventListener('click', () => {
            wsFormArea.innerHTML = '';
        });

        wsFormArea.querySelector('#btn-save-ws').addEventListener('click', () => handleSaveWs(isEdit, ws ? ws.id : null));
    }

    async function handleSaveWs(isEdit, wsId) {
        const saveBtn = wsFormArea.querySelector('#btn-save-ws');
        if (saveBtn) saveBtn.disabled = true;

        const name = (wsFormArea.querySelector('#f-ws-name')?.value || '').trim();
        if (!name) {
            showToast('Le nom de l\'espace de travail est requis.', 'error');
            if (saveBtn) saveBtn.disabled = false;
            return;
        }

        try {
            let result;
            if (isEdit) {
                result = await updateWorkspace(wsId, { name });
            } else {
                result = await createWorkspace({ name });
            }

            if (!result || !result._ok) {
                showToast(extractApiError(result), 'error');
                if (saveBtn) saveBtn.disabled = false;
                return;
            }

            showToast(isEdit ? 'Espace renommé avec succès.' : 'Espace de travail créé avec succès.', 'success');
            wsFormArea.innerHTML = '';
            await loadWorkspaces();
            // Refresh user form dropdown if open
            const wsSelect = formArea.querySelector('#f-workspace');
            if (wsSelect) {
                const currentVal = wsSelect.value;
                wsSelect.innerHTML = _wsOptions(currentVal ? parseInt(currentVal, 10) : null);
            }
        } catch (err) {
            showToast(err.message || 'Erreur inconnue.', 'error');
            if (saveBtn) saveBtn.disabled = false;
        }
    }

    async function handleDeleteWs(wsId, wsName, userCount) {
        if (userCount > 0) {
            showToast(`Impossible de supprimer : ${userCount} utilisateur(s) encore assigné(s).`, 'error');
            return;
        }

        showConfirmModal({
            title: 'Supprimer l\'espace de travail',
            body: `<p>Voulez-vous vraiment supprimer l'espace <strong>${escapeHtml(wsName)}</strong> ?</p>
                   <p style="color:var(--text-muted); font-size:var(--font-xs)">Cette action est irréversible. L'espace doit être vide (aucun utilisateur, aucune donnée).</p>`,
            confirmLabel: 'Supprimer',
            danger: true,
            onConfirm: async () => {
                const result = await deleteWorkspace(wsId);
                if (!result || !result._ok) {
                    showToast(extractApiError(result), 'error');
                    return;
                }
                showToast(`Espace "${escapeHtml(wsName)}" supprimé.`, 'success');
                await loadWorkspaces();
            },
        });
    }

    addWsBtn.addEventListener('click', () => showWsForm(null));

    // ── User Management ─────────────────────────────────────────────

    async function loadUsers() {
        tableContainer.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
        try {
            const data = await getAdminUsers();
            if (!data || !data._ok) {
                tableContainer.innerHTML = `<div class="empty-state"><div class="empty-state-text">Erreur : ${escapeHtml(extractApiError(data))}</div></div>`;
                return;
            }
            renderTable(data.users || []);
        } catch (err) {
            tableContainer.innerHTML = `<div class="empty-state"><div class="empty-state-icon">⚠️</div><div class="empty-state-text">Impossible de charger les utilisateurs.</div></div>`;
        }
    }

    function renderTable(users) {
        const me = getCachedUser();
        if (!users.length) {
            tableContainer.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon">👤</div>
                    <div class="empty-state-text">Aucun utilisateur.</div>
                </div>
            `;
            return;
        }

        tableContainer.innerHTML = `
            <div style="overflow-x:auto">
                <table style="width:100%; border-collapse:collapse; font-size:var(--font-sm)">
                    <thead>
                        <tr>
                            <th class="contacts-th">Identifiant</th>
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
                        <input type="text" id="f-username" placeholder="Identifiant"
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
                               placeholder="${isEdit ? 'Laisser vide pour conserver' : 'Mot de passe'}"
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
                    showToast('L\'identifiant est requis.', 'error');
                    if (saveBtn) saveBtn.disabled = false;
                    return;
                }
                if (!password) {
                    showToast('Le mot de passe est requis.', 'error');
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

            if (!result || !result._ok) {
                showToast(extractApiError(result), 'error');
                if (saveBtn) saveBtn.disabled = false;
                return;
            }

            showToast(isEdit ? 'Utilisateur mis à jour.' : 'Utilisateur créé avec succès.', 'success');
            formArea.innerHTML = '';
            _editingUserId = null;
            await loadUsers();
        } catch (err) {
            showToast(err.message || 'Erreur inconnue.', 'error');
            if (saveBtn) saveBtn.disabled = false;
        }
    }

    async function handleDeactivate(userId, username) {
        showConfirmModal({
            title: 'Désactiver le compte',
            body: `<p>Voulez-vous vraiment désactiver le compte <strong>${escapeHtml(username)}</strong> ?</p>
                   <p style="color:var(--text-muted); font-size:var(--font-xs)">L'utilisateur ne pourra plus se connecter. Cette action est réversible.</p>`,
            confirmLabel: 'Désactiver',
            danger: true,
            onConfirm: async () => {
                const result = await deactivateAdminUser(userId);
                if (!result || !result._ok) {
                    showToast(extractApiError(result), 'error');
                    return;
                }
                showToast(`Compte "${username}" désactivé.`, 'success');
                await loadUsers();
            },
        });
    }

    addBtn.addEventListener('click', () => showForm(null));

    // ── Activity Log ────────────────────────────────────────────────

    const ACTION_ICONS = {
        batch_launched: '🚀', batch_completed: '✅', batch_failed: '❌',
        upload: '📤', delete_job: '🗑️', cancel_job: '⏹️', delete_tags: '🏷️',
        export: '📥', note_added: '📝', note_deleted: '🗑️',
        contact_request: '✉️', conflict_resolved: '✅', conflict_dismissed: '❌',
        merge: '🔀', link: '🔗', reject_link: '❌', unlink: '🔓', manual_edit: '✏️',
    };

    const ACTION_LABELS = {
        batch_launched: 'Recherche lancée', batch_completed: 'Batch terminé',
        batch_failed: 'Erreur Batch', upload: 'Fichier importé',
        delete_job: 'Batch supprimé', cancel_job: 'Batch annulé',
        delete_tags: 'Tags supprimés', export: 'Export effectué',
        note_added: 'Note ajoutée', note_deleted: 'Note supprimée',
        contact_request: 'Demande de contact', conflict_resolved: 'Conflit résolu',
        conflict_dismissed: 'Conflit ignoré', merge: 'Fusion manuelle',
        link: 'Lien confirmé', reject_link: 'Lien refusé',
        unlink: 'Lien dissocié', manual_edit: 'Modification manuelle',
    };

    function _timeAgo(dateStr) {
        if (!dateStr) return '';
        const d = new Date(dateStr);
        const now = new Date();
        const mins = Math.floor((now - d) / 60000);
        if (mins < 1) return "à l'instant";
        if (mins < 60) return `il y a ${mins} min`;
        const hours = Math.floor(mins / 60);
        if (hours < 24) return `il y a ${hours}h`;
        const days = Math.floor(hours / 24);
        if (days === 1) return 'hier';
        return `il y a ${days} jours`;
    }

    async function loadLog() {
        logContainer.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
        logPagination.innerHTML = '';
        try {
            const data = await getActivityLog({ period: _logPeriod, limit: LOG_PAGE_SIZE, offset: _logOffset });
            if (!data || !data._ok) {
                logContainer.innerHTML = `<div class="empty-state"><div class="empty-state-text">Erreur : ${escapeHtml(extractApiError(data))}</div></div>`;
                return;
            }
            const entries = data.entries || [];
            const total = data.total || 0;

            if (entries.length === 0) {
                logContainer.innerHTML = `<div class="empty-state"><div class="empty-state-icon">📋</div><div class="empty-state-text">Aucune activité pour cette période.</div></div>`;
                return;
            }

            logContainer.innerHTML = `
                <div style="display:flex; flex-direction:column; gap:var(--space-sm)">
                    ${entries.map(e => `
                        <div style="display:flex; gap:var(--space-md); align-items:flex-start; padding:var(--space-sm) var(--space-md); border-radius:var(--radius); ${e.action === 'batch_failed' ? 'border-left:3px solid var(--text-error)' : ''}">
                            <span style="font-size:16px; flex-shrink:0">${ACTION_ICONS[e.action] || '📌'}</span>
                            <div style="flex:1; min-width:0">
                                <div style="display:flex; justify-content:space-between; align-items:center; gap:var(--space-md)">
                                    <span style="font-weight:600; color:var(--text-primary); font-size:var(--font-sm)">
                                        👤 ${escapeHtml(e.username || 'système')}
                                    </span>
                                    <span style="font-size:var(--font-xs); color:var(--text-muted); white-space:nowrap">
                                        ${_timeAgo(e.created_at)}
                                    </span>
                                </div>
                                <div style="font-size:var(--font-sm); color:var(--text-secondary); margin-top:2px">
                                    <strong>${ACTION_LABELS[e.action] || e.action}</strong>
                                    ${e.details ? `<div style="margin-top:2px; padding:var(--space-xs) var(--space-sm); background:var(--bg-secondary); border-radius:4px; font-size:var(--font-xs); ${e.action === 'batch_failed' ? 'color:var(--text-error)' : ''}">${escapeHtml(e.details)}</div>` : ''}
                                </div>
                                ${e.target_id ? `<div style="font-size:var(--font-xs); color:var(--text-muted); margin-top:2px; font-family:var(--font-mono)">${e.target_type === 'company' ? `<a href="#/company/${encodeURIComponent(e.target_id)}" style="color:var(--accent); text-decoration:none">${escapeHtml(e.target_id)} ↗</a>` : escapeHtml(e.target_id)}</div>` : ''}
                            </div>
                        </div>
                    `).join('')}
                </div>
            `;

            // Pagination
            const currentPage = Math.floor(_logOffset / LOG_PAGE_SIZE) + 1;
            const totalPages = Math.ceil(total / LOG_PAGE_SIZE);
            if (totalPages > 1) {
                const hasPrev = _logOffset > 0;
                const hasNext = _logOffset + LOG_PAGE_SIZE < total;
                logPagination.innerHTML = `
                    <div style="display:flex; justify-content:center; align-items:center; gap:var(--space-lg)">
                        <button class="btn btn-secondary" id="log-prev" ${hasPrev ? '' : 'disabled'} style="${hasPrev ? '' : 'opacity:0.4; cursor:not-allowed'}">← Précédent</button>
                        <span style="font-size:var(--font-sm); color:var(--text-secondary); font-weight:600">${currentPage} / ${totalPages}</span>
                        <button class="btn btn-secondary" id="log-next" ${hasNext ? '' : 'disabled'} style="${hasNext ? '' : 'opacity:0.4; cursor:not-allowed'}">Suivant →</button>
                    </div>
                `;
                const prevBtn = logPagination.querySelector('#log-prev');
                const nextBtn = logPagination.querySelector('#log-next');
                if (prevBtn && hasPrev) prevBtn.addEventListener('click', () => { _logOffset -= LOG_PAGE_SIZE; loadLog(); });
                if (nextBtn && hasNext) nextBtn.addEventListener('click', () => { _logOffset += LOG_PAGE_SIZE; loadLog(); });
            }
        } catch (err) {
            logContainer.innerHTML = `<div class="empty-state"><div class="empty-state-icon">⚠️</div><div class="empty-state-text">Impossible de charger le journal.</div></div>`;
        }
    }

    // Wire period filter buttons
    container.querySelectorAll('.log-period').forEach(btn => {
        btn.addEventListener('click', () => {
            container.querySelectorAll('.log-period').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            _logPeriod = btn.dataset.period;
            _logOffset = 0;
            loadLog();
        });
    });

    // ── System Error Log ───────────────────────────────────────────

    const syslogContainer = container.querySelector('#syslog-container');
    const syslogPagination = container.querySelector('#syslog-pagination');
    let _syslogLevel = 'all';
    let _syslogPeriod = 'week';
    let _syslogOffset = 0;
    const SYSLOG_PAGE_SIZE = 30;

    const LEVEL_STYLES = {
        ERROR: 'background:rgba(239,68,68,0.1); border-left:3px solid var(--text-error); color:var(--text-error)',
        CRITICAL: 'background:rgba(239,68,68,0.15); border-left:3px solid #dc2626; color:#dc2626',
        WARNING: 'background:rgba(234,179,8,0.1); border-left:3px solid #eab308; color:#eab308',
    };

    async function loadSyslog() {
        syslogContainer.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
        syslogPagination.innerHTML = '';
        try {
            const data = await getSystemLog({ level: _syslogLevel, period: _syslogPeriod, limit: SYSLOG_PAGE_SIZE, offset: _syslogOffset });
            if (!data || !data._ok) {
                syslogContainer.innerHTML = `<div class="empty-state"><div class="empty-state-text">Erreur : ${escapeHtml(extractApiError(data))}</div></div>`;
                return;
            }
            const entries = data.entries || [];
            const total = data.total || 0;

            if (entries.length === 0) {
                syslogContainer.innerHTML = `<div class="empty-state"><div class="empty-state-icon">✅</div><div class="empty-state-text">Aucune erreur système pour cette période.</div></div>`;
                return;
            }

            syslogContainer.innerHTML = `
                <div style="display:flex; flex-direction:column; gap:var(--space-xs)">
                    ${entries.map((e, i) => {
                        const style = LEVEL_STYLES[e.level] || '';
                        const hasTraceback = e.traceback && e.traceback.trim();
                        return `
                            <div style="padding:var(--space-sm) var(--space-md); border-radius:var(--radius); ${style}">
                                <div style="display:flex; justify-content:space-between; align-items:center; gap:var(--space-md)">
                                    <div style="display:flex; align-items:center; gap:var(--space-sm); min-width:0">
                                        <span style="font-weight:700; font-size:var(--font-xs); padding:2px 6px; border-radius:3px; background:rgba(0,0,0,0.2); white-space:nowrap">${escapeHtml(e.level)}</span>
                                        <span style="font-size:var(--font-xs); opacity:0.7; white-space:nowrap">${escapeHtml(e.source || 'api')}</span>
                                        ${e.path ? `<span style="font-size:var(--font-xs); font-family:var(--font-mono); opacity:0.6; white-space:nowrap">${escapeHtml(e.path)}</span>` : ''}
                                    </div>
                                    <span style="font-size:var(--font-xs); opacity:0.6; white-space:nowrap">${_timeAgo(e.created_at)}</span>
                                </div>
                                <div style="font-size:var(--font-sm); margin-top:4px; word-break:break-word">${escapeHtml(e.message)}</div>
                                ${hasTraceback ? `
                                    <details style="margin-top:4px">
                                        <summary style="font-size:var(--font-xs); cursor:pointer; opacity:0.7">Traceback complet</summary>
                                        <pre style="font-size:11px; margin-top:4px; padding:var(--space-sm); background:rgba(0,0,0,0.3); border-radius:4px; overflow-x:auto; white-space:pre-wrap; word-break:break-all">${escapeHtml(e.traceback)}</pre>
                                    </details>
                                ` : ''}
                            </div>
                        `;
                    }).join('')}
                </div>
            `;

            // Pagination
            const currentPage = Math.floor(_syslogOffset / SYSLOG_PAGE_SIZE) + 1;
            const totalPages = Math.ceil(total / SYSLOG_PAGE_SIZE);
            if (totalPages > 1) {
                const hasPrev = _syslogOffset > 0;
                const hasNext = _syslogOffset + SYSLOG_PAGE_SIZE < total;
                syslogPagination.innerHTML = `
                    <div style="display:flex; justify-content:center; align-items:center; gap:var(--space-lg)">
                        <button class="btn btn-secondary" id="syslog-prev" ${hasPrev ? '' : 'disabled'} style="${hasPrev ? '' : 'opacity:0.4; cursor:not-allowed'}">← Précédent</button>
                        <span style="font-size:var(--font-sm); color:var(--text-secondary); font-weight:600">${currentPage} / ${totalPages}</span>
                        <button class="btn btn-secondary" id="syslog-next" ${hasNext ? '' : 'disabled'} style="${hasNext ? '' : 'opacity:0.4; cursor:not-allowed'}">Suivant →</button>
                    </div>
                `;
                const prevBtn = syslogPagination.querySelector('#syslog-prev');
                const nextBtn = syslogPagination.querySelector('#syslog-next');
                if (prevBtn && hasPrev) prevBtn.addEventListener('click', () => { _syslogOffset -= SYSLOG_PAGE_SIZE; loadSyslog(); });
                if (nextBtn && hasNext) nextBtn.addEventListener('click', () => { _syslogOffset += SYSLOG_PAGE_SIZE; loadSyslog(); });
            }
        } catch (err) {
            syslogContainer.innerHTML = `<div class="empty-state"><div class="empty-state-icon">⚠️</div><div class="empty-state-text">Impossible de charger le journal système.</div></div>`;
        }
    }

    // Wire syslog filter buttons
    container.querySelectorAll('.syslog-level').forEach(btn => {
        btn.addEventListener('click', () => {
            container.querySelectorAll('.syslog-level').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            _syslogLevel = btn.dataset.level;
            _syslogOffset = 0;
            loadSyslog();
        });
    });

    container.querySelectorAll('.syslog-period').forEach(btn => {
        btn.addEventListener('click', () => {
            container.querySelectorAll('.syslog-period').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            _syslogPeriod = btn.dataset.period;
            _syslogOffset = 0;
            loadSyslog();
        });
    });

    // Clear button
    container.querySelector('#btn-clear-syslog')?.addEventListener('click', async () => {
        const result = await clearSystemLog();
        if (!result || !result._ok) {
            showToast(extractApiError(result), 'error');
            return;
        }
        showToast('Entrées anciennes nettoyées.', 'success');
        loadSyslog();
    });

    // ── RGPD Tab Logic ──────────────────────────────────────────────

    async function loadOppositionHistory() {
        const historyContainer = document.getElementById('rgpd-history-container');
        if (!historyContainer) return;
        try {
            const data = await getOppositions();
            if (!data || !Array.isArray(data) || data.length === 0) {
                historyContainer.innerHTML = `<p style="color:var(--text-muted)">Aucune demande d'opposition enregistrée.</p>`;
                return;
            }
            historyContainer.innerHTML = `
                <div style="overflow-x:auto">
                    <table class="data-table" style="width:100%; border-collapse:collapse; font-size:var(--font-sm)">
                        <thead>
                            <tr>
                                <th class="contacts-th">Date</th>
                                <th class="contacts-th">Nom</th>
                                <th class="contacts-th">Email</th>
                                <th class="contacts-th">Téléphone</th>
                                <th class="contacts-th">Motif</th>
                                <th class="contacts-th">Résultat</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${data.map(row => `
                                <tr class="contacts-row">
                                    <td class="contacts-td" style="color:var(--text-muted); white-space:nowrap">${row.created_at ? formatDateTime(row.created_at) : '—'}</td>
                                    <td class="contacts-td" style="color:var(--text-secondary)">${escapeHtml((row.nom || '') + (row.prenom ? ' ' + row.prenom : '') || '—')}</td>
                                    <td class="contacts-td" style="color:var(--text-secondary)">${escapeHtml(row.email || '—')}</td>
                                    <td class="contacts-td" style="color:var(--text-secondary)">${escapeHtml(row.telephone || '—')}</td>
                                    <td class="contacts-td" style="color:var(--text-secondary)">${escapeHtml(row.motif || '—')}</td>
                                    <td class="contacts-td" style="color:var(--text-muted); font-size:var(--font-xs)">${escapeHtml(row.summary || row.result || '—')}</td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            `;
        } catch (err) {
            historyContainer.innerHTML = `<p style="color:var(--danger)">Erreur de chargement.</p>`;
        }
    }

    function showNameCandidates(candidates, oppositionId) {
        const candidatesDiv = document.getElementById('rgpd-candidates');
        const candidatesList = document.getElementById('rgpd-candidates-list');
        if (!candidatesDiv || !candidatesList) return;

        candidatesDiv.style.display = 'block';
        candidatesList.innerHTML = `
            <div style="overflow-x:auto; margin-bottom:var(--space-sm)">
                <table style="width:100%; border-collapse:collapse; font-size:var(--font-sm)">
                    <thead>
                        <tr>
                            <th class="contacts-th" style="width:32px"></th>
                            <th class="contacts-th">Nom</th>
                            <th class="contacts-th">Prénom</th>
                            <th class="contacts-th">Entreprise</th>
                            <th class="contacts-th">Source</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${candidates.map((c, idx) => `
                            <tr class="contacts-row">
                                <td class="contacts-td">
                                    <input type="checkbox" class="rgpd-candidate-check" data-officer-id="${c.id || ''}" data-idx="${idx}" style="cursor:pointer">
                                </td>
                                <td class="contacts-td" style="color:var(--text-primary); font-weight:600">${escapeHtml(c.nom || '—')}</td>
                                <td class="contacts-td" style="color:var(--text-secondary)">${escapeHtml(c.prenom || '—')}</td>
                                <td class="contacts-td" style="color:var(--text-secondary)">${escapeHtml(c.company_name || c.siren || '—')}</td>
                                <td class="contacts-td" style="color:var(--text-muted); font-size:var(--font-xs)">${escapeHtml(c.source || '—')}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
            <button id="btn-confirm-candidates" class="btn btn-primary" style="font-size:var(--font-sm)">
                Confirmer la suppression des entrées sélectionnées
            </button>
        `;

        candidatesList.querySelector('#btn-confirm-candidates')?.addEventListener('click', async () => {
            const checked = candidatesList.querySelectorAll('.rgpd-candidate-check:checked');
            if (checked.length === 0) {
                showToast('Sélectionnez au moins une entrée à supprimer.', 'error');
                return;
            }
            const officerIds = Array.from(checked).map(cb => cb.dataset.officerId).filter(Boolean);
            try {
                const result = await confirmOppositionDeletion({ opposition_id: oppositionId, officer_ids: officerIds });
                if (result && result.summary) {
                    showToast(result.summary, 'success');
                } else {
                    showToast('Suppression confirmée.', 'success');
                }
                candidatesDiv.style.display = 'none';
                loadOppositionHistory();
            } catch (err) {
                showToast('Erreur lors de la confirmation : ' + err.message, 'error');
            }
        });
    }

    const oppForm = document.getElementById('rgpd-opposition-form');
    if (oppForm) {
        oppForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            const data = {
                nom: document.getElementById('rgpd-nom').value.trim(),
                prenom: document.getElementById('rgpd-prenom').value.trim(),
                email: document.getElementById('rgpd-email').value.trim(),
                telephone: document.getElementById('rgpd-phone').value.trim(),
                motif: document.getElementById('rgpd-motif').value.trim(),
            };
            if (!data.email && !data.telephone) {
                showToast('Email ou téléphone requis', 'error');
                return;
            }
            if (!data.motif) {
                showToast('Le motif est requis', 'error');
                return;
            }
            try {
                const result = await submitOpposition(data);
                const resultDiv = document.getElementById('rgpd-result');
                resultDiv.style.display = 'block';
                resultDiv.innerHTML = `<div style="padding:var(--space-sm) var(--space-md); background:rgba(16,185,129,0.1); border:1px solid rgba(16,185,129,0.3); border-radius:var(--radius-sm); color:#34d399; font-size:var(--font-sm)">${escapeHtml(result.summary || 'Demande traitée.')}</div>`;

                if (result.name_candidates && result.name_candidates.length > 0) {
                    showNameCandidates(result.name_candidates, result.opposition_id);
                }

                oppForm.reset();
                loadOppositionHistory();
            } catch (err) {
                showToast('Erreur: ' + err.message, 'error');
            }
        });
    }

    // Load everything
    await loadWorkspaces();
    if (isStale(gen)) return;
    await loadUsers();
    loadLog();
    loadSyslog();
    loadOppositionHistory();
}
