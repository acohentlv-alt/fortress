/**
 * Fortress SPA — Main Application & Router
 */

import { renderDashboard } from './pages/dashboard.js?v=19';
import { renderDepartment } from './pages/department.js?v=19';
import { renderJob } from './pages/job.js?v=19';
import { renderCompany } from './pages/company.js?v=19';
import { renderSearch } from './pages/search.js?v=19';
import { renderMonitor } from './pages/monitor.js?v=21';
import { renderNewBatch } from './pages/new-batch.js?v=19';
import { renderOpenQuery } from './pages/open-query.js?v=19';
import { renderUpload } from './pages/upload.js?v=19';
import { renderContacts } from './pages/contacts.js?v=19';
import { renderActivity } from './pages/activity.js?v=19';
import { renderBlacklist } from './pages/blacklist.js?v=19';
import { renderAdmin } from './pages/admin.js?v=19';
import { renderLogin } from './pages/login.js?v=19';
import { renderIntro } from './pages/intro.js?v=19';
import { getDashboardStats, getCurrentUser, logoutUser, getCachedUser } from './api.js';
import { initI18n, changeLanguage, getLang, t, translateDOM, onLanguageChange } from './i18n.js';

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
    { pattern: /^#\/new-batch$/, handler: renderNewBatch, nav: 'new-batch' },
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

function _showIntroPage() {
    _showSidebar(false);
    renderIntro(getPageContent());
}

function _showLoginPage() {
    _showSidebar(false);
    renderLogin(getPageContent(), (user) => {
        // On successful login — restore app UI
        _showSidebar(true);
        _setupSidebarToggle();
        _updateUserDisplay(user);
        _setupLogout();
        _setupRunningJobs();
        // Show admin section for admin users only
        const adminSection = document.getElementById('nav-section-admin');
        const adminNav = document.getElementById('nav-admin');
        if (user.role === 'admin') {
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

    // Intro + Login routes — skip auth check
    if (hash === '#/intro') {
        _showIntroPage();
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

async function checkRunningJobs() {
    const stats = await getDashboardStats();
    const badge = document.getElementById('header-running-badge');
    if (stats && stats.running_jobs > 0) {
        badge.style.display = 'inline-flex';
        badge.textContent = `⏳ ${stats.running_jobs} batch en cours`;
    } else {
        badge.style.display = 'none';
    }
}

function _setupRunningJobs() {
    checkRunningJobs();
    if (_runningJobsInterval) clearInterval(_runningJobsInterval);
    _runningJobsInterval = setInterval(checkRunningJobs, 30000);
}

// ── App Init ─────────────────────────────────────────────────────

async function initApp() {
    // Initialize i18n before anything renders
    await initI18n();

    // Wire language toggle — use event delegation on document for reliability
    document.addEventListener('click', (e) => {
        if (e.target && e.target.id === 'lang-toggle') {
            e.preventDefault();
            e.stopPropagation();
            const newLang = getLang() === 'fr' ? 'en' : 'fr';
            changeLanguage(newLang);
        }
    });

    // Re-render current page when language changes
    onLanguageChange(() => navigate());

    // Check session with backend — cookie is sent automatically
    const user = await getCurrentUser();

    if (!user) {
        // Not authenticated — check if navigating to login, otherwise show intro
        if (window.location.hash === '#/login') {
            _showLoginPage();
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
    // Show admin section for admin users only
    const adminSection = document.getElementById('nav-section-admin');
    const adminNav = document.getElementById('nav-admin');
    if (user.role === 'admin') {
        if (adminSection) adminSection.style.display = '';
        if (adminNav) adminNav.style.display = '';
    }
    navigate();
    _setupRunningJobs();
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
    brandBtn.title = 'Fixer/réduire le menu';

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
