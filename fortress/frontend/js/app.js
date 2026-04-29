/**
 * Fortress SPA — Main Application & Router
 */

import { renderDashboard } from './pages/dashboard.js';
import { renderDepartment } from './pages/department.js';
import { renderJob } from './pages/job.js';
import { renderCompany } from './pages/company.js';
import { renderSearch } from './pages/search.js';
import { renderMonitor } from './pages/monitor.js';
import { renderNewBatch } from './pages/new-batch.js';
import { renderOpenQuery } from './pages/open-query.js';
import { renderUpload } from './pages/upload.js';
import { renderContacts } from './pages/contacts.js';
import { renderActivity } from './pages/activity.js';
import { renderBlacklist } from './pages/blacklist.js';
import { renderAdmin } from './pages/admin.js';
import { renderLogin } from './pages/login.js';
import { renderIntro } from './pages/intro.js';
import { renderLegal } from './pages/legal.js';
import { getDashboardStats, getCurrentUser, logoutUser, getCachedUser } from './api.js';
import { initI18n, changeLanguage, getLang, t, translateDOM, onLanguageChange } from './i18n.js';

// ── Console Error Capture (for bug reports) ─────────────────────
const _consoleErrors = [];
const _origConsoleError = console.error;
console.error = function (...args) {
    _consoleErrors.push({
        time: new Date().toISOString(),
        message: args.map(a => {
            try { return typeof a === 'object' ? JSON.stringify(a) : String(a); }
            catch { return String(a); }
        }).join(' ')
    });
    if (_consoleErrors.length > 10) _consoleErrors.shift();
    _origConsoleError.apply(console, args);
};

// ── Navigation Generation Counter ───────────────────────────────
// Each navigate() call increments _navGeneration. Page handlers
// receive this value and call isStale(gen) after every `await`
// before writing to the DOM — if another navigate() fired while
// they were awaiting, they bail out instead of overwriting the
// new page's content.
let _navGeneration = 0;

export function isStale(gen) {
    return gen !== _navGeneration;
}

// ── Page Cleanup System ──────────────────────────────────────────
// Pages register cleanup functions (e.g. clearInterval) that must
// run before navigating away. Without this, polling intervals from
// the monitor page keep firing and overwrite other pages.
const _cleanupFns = [];

export function registerCleanup(fn) {
    _cleanupFns.push(fn);
}

function _runCleanup() {
    // Remove any lingering modal overlays
    document.querySelectorAll('#confirm-modal-overlay, .modal-overlay, #bulk-action-bar').forEach(el => el.remove());
    while (_cleanupFns.length > 0) {
        try { _cleanupFns.pop()(); } catch { /* swallow */ }
    }
}

// ── Router ───────────────────────────────────────────────────────
const routes = [
    { pattern: /^#?\/?$/, handler: renderDashboard, nav: 'dashboard' },
    { pattern: /^#\/dashboard$/, handler: renderDashboard, nav: 'dashboard' },
    { pattern: /^#\/department\/(.+)$/, handler: renderDepartment, nav: 'dashboard' },
    { pattern: /^#\/job\/(.+)$/, handler: renderJob, nav: 'none' },
    { pattern: /^#\/company\/(.+)$/, handler: renderCompany, nav: 'none' },
    { pattern: /^#\/search/, handler: renderSearch, nav: 'search' },
    { pattern: /^#\/new-batch/, handler: renderNewBatch, nav: 'new-batch' },
    { pattern: /^#\/open-query$/, handler: renderOpenQuery, nav: 'query' },
    { pattern: /^#\/monitor\/(.+)$/, handler: renderMonitor, nav: 'monitor' },
    { pattern: /^#\/monitor$/, handler: renderMonitor, nav: 'monitor' },
    { pattern: /^#\/upload$/, handler: renderUpload, nav: 'upload' },
    { pattern: /^#\/contacts$/, handler: renderContacts, nav: 'contacts' },
    { pattern: /^#\/activity$/, handler: renderActivity, nav: 'activity' },
    { pattern: /^#\/blacklist$/, handler: renderBlacklist, nav: 'blacklist' },
    { pattern: /^#\/admin$/, handler: renderAdmin, nav: 'admin' },
    { pattern: /^#\/login$/, handler: renderLogin, nav: 'none' },
    { pattern: /^#\/intro$/, handler: renderIntro, nav: 'none' },
    { pattern: /^#\/legal$/, handler: renderLegal, nav: 'none' },
];

function getPageContent() {
    return document.getElementById('page-content');
}

function showLoading() {
    getPageContent().innerHTML = '<div class="loading"><div class="spinner"></div></div>';
}

// ── UI helpers ───────────────────────────────────────────────────

function _showSidebar(show) {
    const sidebar = document.getElementById('sidebar');
    const mainContent = document.querySelector('.main-content');
    const header = document.querySelector('.header');
    if (show) {
        sidebar?.classList.remove('hidden');
        mainContent?.classList.remove('no-sidebar');
        header?.classList.remove('hidden');
    } else {
        sidebar?.classList.add('hidden');
        mainContent?.classList.add('no-sidebar');
        header?.classList.add('hidden');
    }
}

function _updateUserDisplay(user) {
    const el = document.getElementById('user-display');
    if (!el) return;
    if (!user) {
        el.classList.add('hidden');
        return;
    }
    const icon = user.role === 'admin' ? '👑' : '👤';
    const label = user.display_name || user.username;
    
    // We need initials for the collapsed state avatar circle
    const initials = label.charAt(0).toUpperCase();
    
    // Structure it so CSS can hide text and keep the icon/initials in collapsed mode
    el.innerHTML = `
        <span class="user-display-icon" data-initials="${initials}">${icon}</span>
        <span class="user-display-text">${label}</span>
    `;
    el.classList.remove('hidden');
}

function _revealApp() {
    const layout = document.querySelector('.app-layout');
    if (layout) layout.style.visibility = 'visible';
}

function _showIntroPage() {
    _showSidebar(false);
    if (window.location.hash !== '#/intro') {
        history.replaceState(null, '', '#/intro');
    }
    renderIntro(getPageContent());
    _revealApp();
}

function _showLegalPage() {
    _showSidebar(false);
    renderLegal(getPageContent());
    _revealApp();
}

function _showLoginPage() {
    _showSidebar(false);
    _revealApp();
    renderLogin(getPageContent(), (user) => {
        // On successful login — restore app UI
        _showSidebar(true);
        _setupSidebarToggle();
        _updateUserDisplay(user);
        _setupLogout();
        _setupRunningJobs();
        _wireBugReportButton();
        // Show admin section for admin and head users
        const adminSection = document.getElementById('nav-section-admin');
        const adminNav = document.getElementById('nav-admin');
        if (user.role === 'admin' || user.role === 'head') {
            if (adminSection) adminSection.style.display = '';
            if (adminNav) adminNav.style.display = '';
        }
        // Navigate to dashboard
        window.location.hash = '#/';
        navigate();
    });
}

function _setupLogout() {
    const logoutBtn = document.getElementById('btn-logout');
    if (!logoutBtn) return;
    logoutBtn.classList.remove('hidden');
    // Remove old listener by cloning
    const fresh = logoutBtn.cloneNode(true);
    logoutBtn.parentNode.replaceChild(fresh, logoutBtn);
    fresh.addEventListener('click', async (e) => {
        e.preventDefault();
        await logoutUser();
        _updateUserDisplay(null);
        _showLoginPage();
    });
}

// ── Navigation ───────────────────────────────────────────────────

async function navigate() {
    const gen = ++_navGeneration;
    const hash = window.location.hash || '#/';

    // Intro + Login + Legal routes — skip auth check
    if (hash === '#/intro') {
        _showIntroPage();
        return;
    }
    if (hash === '#/legal') {
        _showLegalPage();
        return;
    }
    if (hash === '#/login') {
        _showLoginPage();
        return;
    }

    // Auth guard — verify session before rendering
    const user = getCachedUser();
    if (!user) {
        _showIntroPage();
        return;
    }

    const container = getPageContent();

    // Restore page-container constraints (may have been removed by landing page)
    const pageContainer = document.querySelector('.page-container');
    if (pageContainer) {
        pageContainer.style.padding = '';
        pageContainer.style.maxWidth = '';
    }
    const mainContent = document.querySelector('.main-content');
    if (mainContent) mainContent.style.background = '';
    document.body.style.overflow = '';

    // Find matching route
    for (const route of routes) {
        const match = hash.match(route.pattern);
        if (match) {
            // Update active nav
            document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
            const navEl = document.querySelector(`[data-page="${route.nav}"]`);
            if (navEl) navEl.classList.add('active');

            // Hide header search on Base SIRENE page (it has its own search bar)
            const headerSearch = document.querySelector('.header-search');
            if (headerSearch) headerSearch.style.display = route.nav === 'search' ? 'none' : '';

            // Clean up previous page (intervals, listeners, etc.)
            _runCleanup();

            // Show loading then render
            showLoading();
            try {
                await route.handler(container, ...match.slice(1), gen);
                // Page transition animation
                container.style.animation = 'none';
                container.offsetHeight; // force reflow
                container.style.animation = 'fadeSlideIn 0.3s ease';
            } catch (err) {
                console.error('Page render error:', err);
                container.innerHTML = `
                    <div class="empty-state">
                        <div class="empty-state-icon">❌</div>
                        <div class="empty-state-text">Erreur de chargement</div>
                        <p style="color: var(--text-muted)">${err.message}</p>
                    </div>
                `;
            }
            return;
        }
    }

    // 404
    container.innerHTML = `
        <div class="empty-state">
            <div class="empty-state-icon">🔍</div>
            <div class="empty-state-text">Page introuvable</div>
            <a href="#/" class="btn btn-primary">Retour au Dashboard</a>
        </div>
    `;
}

// ── Running Job Badge ────────────────────────────────────────────
let _runningJobsInterval = null;
let _runningJobIds = [];
let _runningJobsCount = 0;

async function checkRunningJobs() {
    const stats = await getDashboardStats();
    const badge = document.getElementById('header-running-badge');
    if (!badge) return;
    if (stats && stats.running_jobs > 0) {
        badge.style.display = 'inline-flex';
        badge.textContent = `⏳ ${stats.running_jobs} batch en cours`;
        _runningJobIds = Array.isArray(stats.running_job_ids) ? stats.running_job_ids : [];
        _runningJobsCount = stats.running_jobs;
    } else {
        badge.style.display = 'none';
        _runningJobIds = [];
        _runningJobsCount = 0;
    }
}

function _setupRunningJobs() {
    checkRunningJobs();
    if (_runningJobsInterval) clearInterval(_runningJobsInterval);
    _runningJobsInterval = setInterval(checkRunningJobs, 30000);

    // Wire click handler once
    const badge = document.getElementById('header-running-badge');
    if (badge && !badge.dataset.clickWired) {
        badge.addEventListener('click', () => {
            if (_runningJobIds.length === 1) {
                window.location.hash = `#/monitor/${encodeURIComponent(_runningJobIds[0])}`;
            } else if (_runningJobIds.length > 1) {
                window.location.hash = '#/';
            } else if (_runningJobsCount > 0) {
                // API/JS skew during deploy: count says yes but IDs missing → dashboard
                window.location.hash = '#/';
            }
            // count 0 = badge hidden; do nothing
        });
        badge.dataset.clickWired = '1';
    }
}

// ── Workspace completion notifications (WebSocket) ──────────────
let _wsWorkspace = null;

function _subscribeWorkspaceNotifications(user) {
    if (!user) return;

    const wsId = (user.role === 'admin' || !user.workspace_id) ? 'all' : String(user.workspace_id);
    const wsProtocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const wsUrl = `${wsProtocol}://${window.location.host}/ws/workspace/${wsId}`;

    function _connect() {
        if (_wsWorkspace) {
            try { _wsWorkspace.close(); } catch (_) {}
        }
        _wsWorkspace = new WebSocket(wsUrl);

        _wsWorkspace.onmessage = async (event) => {
            try {
                const msg = JSON.parse(event.data);
                if (msg.type === 'batch_complete') {
                    const { playCompletionSound, showCompletionBanner } = await import('./components.js');
                    playCompletionSound();
                    showCompletionBanner({
                        batchId: msg.batch_id,
                        batchName: msg.batch_name,
                        count: msg.count,
                    });
                }
            } catch (_) {}
        };

        _wsWorkspace.onclose = () => {
            // Auto-reconnect after 5s
            setTimeout(_connect, 5000);
        };

        _wsWorkspace.onerror = () => {
            // Close triggers onclose which schedules reconnect
            try { _wsWorkspace.close(); } catch (_) {}
        };
    }

    _connect();
}

// ── Bug Report Modal ────────────────────────────────────────────

function _wireBugReportButton() {
    const btn = document.getElementById('btn-bug-report');
    if (btn) btn.onclick = _openBugReportModal;
}

async function _openBugReportModal() {
    if (document.querySelector('.bug-report-overlay')) return;

    const user = getCachedUser();
    if (!user) return;

    // Capture screenshot BEFORE adding modal overlay to DOM
    let screenshotBlob = null;
    let screenshotDataUrl = null;
    try {
        if (typeof html2canvas !== 'undefined') {
            const canvas = await html2canvas(document.body, { scale: 0.5, useCORS: true, logging: false });
            screenshotDataUrl = canvas.toDataURL('image/png');
            screenshotBlob = await new Promise(resolve => canvas.toBlob(resolve, 'image/png'));
        }
    } catch (err) {
        // Fallback: proceed without screenshot
        screenshotBlob = null;
        screenshotDataUrl = null;
    }

    const overlay = document.createElement('div');
    overlay.className = 'bug-report-overlay';

    const previewHtml = screenshotDataUrl ? `
        <div class="bug-report-screenshot-preview">
            <img src="${screenshotDataUrl}" alt="Capture d'écran" />
            <label>
                <input type="checkbox" id="bug-include-screenshot" checked />
                Inclure la capture d'écran
            </label>
        </div>
    ` : '';

    overlay.innerHTML = `
        <div class="bug-report-modal">
            <h3>Signaler un bug</h3>
            <label for="bug-desc">Décrivez le problème *</label>
            <textarea id="bug-desc" placeholder="Que s'est-il passé ? Qu'attendiez-vous ?"></textarea>
            ${previewHtml}
            <div class="bug-report-actions">
                <button class="btn btn-ghost" id="bug-cancel">Annuler</button>
                <button class="btn btn-primary" id="bug-send">Envoyer</button>
            </div>
            <div class="bug-report-status" id="bug-status"></div>
        </div>
    `;

    document.body.appendChild(overlay);

    overlay.addEventListener('click', (e) => {
        if (e.target === overlay) overlay.remove();
    });
    document.getElementById('bug-cancel').addEventListener('click', () => overlay.remove());

    const escHandler = (e) => {
        if (e.key === 'Escape') { overlay.remove(); document.removeEventListener('keydown', escHandler); }
    };
    document.addEventListener('keydown', escHandler);

    document.getElementById('bug-send').addEventListener('click', async () => {
        const desc = document.getElementById('bug-desc').value.trim();
        const statusEl = document.getElementById('bug-status');
        const sendBtn = document.getElementById('bug-send');
        const includeScreenshot = document.getElementById('bug-include-screenshot');

        if (!desc) {
            statusEl.innerHTML = '<span style="color:var(--danger)">Veuillez décrire le problème.</span>';
            return;
        }

        sendBtn.disabled = true;
        sendBtn.textContent = 'Envoi...';
        statusEl.innerHTML = '';

        const context = {
            username: user.username,
            role: user.role,
            workspace_id: user.workspace_id || null,
            page_url: window.location.href,
            timestamp: new Date().toISOString(),
            user_agent: navigator.userAgent,
            screen: `${window.screen.width}x${window.screen.height}`,
            console_errors: _consoleErrors.slice(-5)
        };

        const formData = new FormData();
        formData.append('description', desc);
        formData.append('context', JSON.stringify(context));
        if (screenshotBlob && (!includeScreenshot || includeScreenshot.checked)) {
            formData.append('screenshot', new File([screenshotBlob], 'screenshot.png', { type: 'image/png' }));
        }

        try {
            const resp = await fetch('/api/bug-report', {
                method: 'POST',
                credentials: 'same-origin',
                body: formData
            });
            if (resp.ok) {
                statusEl.innerHTML = '<span style="color:var(--success)">Merci ! Votre rapport a été envoyé.</span>';
                setTimeout(() => overlay.remove(), 1500);
            } else {
                const data = await resp.json().catch(() => ({}));
                statusEl.innerHTML = `<span style="color:var(--danger)">${data.error || "Erreur lors de l'envoi."}</span>`;
                sendBtn.disabled = false;
                sendBtn.textContent = 'Envoyer';
            }
        } catch {
            statusEl.innerHTML = '<span style="color:var(--danger)">Erreur réseau. Réessayez.</span>';
            sendBtn.disabled = false;
            sendBtn.textContent = 'Envoyer';
        }
    });
}

// ── App Init ─────────────────────────────────────────────────────

async function initApp() {
    // Initialize i18n before anything renders
    await initI18n();

    // Wire language toggle — expose globally for onclick handler
    window.__toggleLang = () => {
        const newLang = getLang() === 'fr' ? 'en' : 'fr';
        console.log('[i18n] Switching to:', newLang);
        changeLanguage(newLang);
    };

    // Re-render current page when language changes
    onLanguageChange(() => {
        console.log('[i18n] Language changed, re-rendering...');
        navigate();
    });

    // Check session with backend — cookie is sent automatically
    const user = await getCurrentUser();

    if (!user) {
        // Not authenticated — check if navigating to login or legal, otherwise show intro
        if (window.location.hash === '#/login') {
            _showLoginPage();
        } else if (window.location.hash === '#/legal') {
            _showLegalPage();
        } else {
            _showIntroPage();
        }
        return;
    }

    // Authenticated — show full app
    _showSidebar(true);
    _setupSidebarToggle();
    _updateUserDisplay(user);
    _setupLogout();
    // Hide "Requête Libre" for ALL users (placeholder, not launched)
    const queryNav = document.getElementById('nav-query');
    if (queryNav) queryNav.style.display = 'none';
    // Show admin section for admin and head users
    const adminSection = document.getElementById('nav-section-admin');
    const adminNav = document.getElementById('nav-admin');
    if (user.role === 'admin' || user.role === 'head') {
        if (adminSection) adminSection.style.display = '';
        if (adminNav) adminNav.style.display = '';
    }
    navigate();
    _setupRunningJobs();
    _subscribeWorkspaceNotifications(user);
    _wireBugReportButton();
    _revealApp();
}

// ── Sidebar Collapse Toggle ─────────────────────────────────────

function _setupSidebarToggle() {
    const sidebar = document.getElementById('sidebar');
    const brandBtn = document.querySelector('.sidebar-brand');
    const mainContent = document.querySelector('.main-content');
    if (!sidebar || !brandBtn) return;

    function _applyState(collapsed) {
        if (collapsed) {
            sidebar.classList.add('collapsed');
            if (mainContent) mainContent.classList.add('sidebar-collapsed');
        } else {
            sidebar.classList.remove('collapsed');
            if (mainContent) mainContent.classList.remove('sidebar-collapsed');
        }
    }

    const storedState = localStorage.getItem('fortress_sidebar_collapsed');
    
    // Default: collapsed. Only expand if user explicitly chose it AND screen is wide
    if (storedState === '0' && window.innerWidth > 1400) {
        _applyState(false);
    } else {
        _applyState(true);
    }

    brandBtn.style.cursor = 'pointer';
    brandBtn.title = t('sidebar.pinMenu');

    // Click to pin/unpin
    let _suppressHover = false;

    brandBtn.addEventListener('click', (e) => {
        e.stopPropagation();

        sidebar.classList.remove('hover-expanded');

        const willCollapse = !sidebar.classList.contains('collapsed');
        _applyState(willCollapse);
        localStorage.setItem('fortress_sidebar_collapsed', willCollapse ? '1' : '0');

        // Suppress hover-expand for 300ms after click to prevent immediate re-expand
        if (willCollapse) {
            _suppressHover = true;
            setTimeout(() => { _suppressHover = false; }, 300);
        }
    });

    // JS Hover Architecture (Overlay tray)
    sidebar.addEventListener('mouseenter', () => {
        if (sidebar.classList.contains('collapsed') && !_suppressHover) {
            sidebar.classList.add('hover-expanded');
        }
    });

    sidebar.addEventListener('mouseleave', () => {
        sidebar.classList.remove('hover-expanded');
    });

    // Auto-fold when screen shrinks
    let resizeTimer;
    window.addEventListener('resize', () => {
        clearTimeout(resizeTimer);
        resizeTimer = setTimeout(() => {
            if (window.innerWidth <= 1400 && !sidebar.classList.contains('collapsed')) {
                _applyState(true);
                sidebar.classList.remove('hover-expanded');
            }
        }, 100);
    });
}

// ── Event Listeners ──────────────────────────────────────────────
window.addEventListener('hashchange', () => {
    const hash = window.location.hash || '#/';
    if (hash === '#/intro') {
        _showIntroPage();
        return;
    }
    if (hash === '#/legal') {
        _showLegalPage();
        return;
    }
    if (hash === '#/login') {
        _showLoginPage();
        return;
    }
    if (!getCachedUser()) {
        _showIntroPage();
        return;
    }
    navigate();
});
window.addEventListener('DOMContentLoaded', initApp);
