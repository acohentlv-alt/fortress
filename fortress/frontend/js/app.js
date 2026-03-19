/**
 * Fortress SPA — Main Application & Router
 */

import { renderDashboard } from './pages/dashboard.js';
import { renderDepartment } from './pages/department.js';
import { renderJob } from './pages/job.js';
import { renderCompany } from './pages/company.js?v=3';
import { renderSearch } from './pages/search.js';
import { renderMonitor } from './pages/monitor.js';
import { renderNewBatch } from './pages/new-batch.js';
import { renderOpenQuery } from './pages/open-query.js';
import { renderUpload } from './pages/upload.js';
import { renderContacts } from './pages/contacts.js';
import { renderActivity } from './pages/activity.js';
import { renderLogin } from './pages/login.js';
import { renderIntro } from './pages/intro.js';
import { getDashboardStats, getCurrentUser, logoutUser, getCachedUser } from './api.js';

// ── Page Cleanup System ──────────────────────────────────────────
// Pages register cleanup functions (e.g. clearInterval) that must
// run before navigating away. Without this, polling intervals from
// the monitor page keep firing and overwrite other pages.
const _cleanupFns = [];

export function registerCleanup(fn) {
    _cleanupFns.push(fn);
}

function _runCleanup() {
    while (_cleanupFns.length > 0) {
        try { _cleanupFns.pop()(); } catch { /* swallow */ }
    }
}

// ── Router ───────────────────────────────────────────────────────
const routes = [
    { pattern: /^#?\/?$/, handler: renderDashboard, nav: 'dashboard' },
    { pattern: /^#\/dashboard$/, handler: renderDashboard, nav: 'dashboard' },
    { pattern: /^#\/department\/(.+)$/, handler: renderDepartment, nav: 'dashboard' },
    { pattern: /^#\/job\/(.+)$/, handler: renderJob, nav: 'dashboard' },
    { pattern: /^#\/company\/(.+)$/, handler: renderCompany, nav: 'search' },
    { pattern: /^#\/search/, handler: renderSearch, nav: 'search' },
    { pattern: /^#\/new-batch$/, handler: renderNewBatch, nav: 'new-batch' },
    { pattern: /^#\/open-query$/, handler: renderOpenQuery, nav: 'query' },
    { pattern: /^#\/monitor\/(.+)$/, handler: renderMonitor, nav: 'monitor' },
    { pattern: /^#\/monitor$/, handler: renderMonitor, nav: 'monitor' },
    { pattern: /^#\/upload$/, handler: renderUpload, nav: 'upload' },
    { pattern: /^#\/contacts$/, handler: renderContacts, nav: 'contacts' },
    { pattern: /^#\/activity$/, handler: renderActivity, nav: 'activity' },
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
        _updateUserDisplay(user);
        _setupLogout();
        _setupRunningJobs();
        initGlobalSearch();
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
                await route.handler(container, ...match.slice(1));
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

// ── Global Search ────────────────────────────────────────────────
function initGlobalSearch() {
    const input = document.getElementById('global-search');
    if (!input) return;
    let debounceTimer;

    function _doSearch(q) {
        if (!q) return;
        const hash = window.location.hash || '#/';
        const onDashboard = hash === '#/' || hash === '#/dashboard' || hash === '';
        if (onDashboard) {
            // On dashboard: switch to "Toutes les Données" tab and search within it
            const btn = document.getElementById('btn-all-data');
            if (btn) { btn.click(); }
            // Set the search term in the All Data search input after a tick
            setTimeout(() => {
                const allDataSearch = document.getElementById('alldata-search');
                if (allDataSearch) { allDataSearch.value = q; allDataSearch.dispatchEvent(new Event('input')); }
            }, 100);
        } else {
            window.location.hash = `#/search?q=${encodeURIComponent(q)}`;
        }
    }

    input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            const q = input.value.trim();
            if (q) { _doSearch(q); input.blur(); }
        }
    });

    input.addEventListener('input', () => {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => {
            const q = input.value.trim();
            if (q.length >= 3) { _doSearch(q); }
        }, 500);
    });
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
    // Check session with backend — cookie is sent automatically
    const user = await getCurrentUser();

    if (!user) {
        // Not authenticated — show intro page
        _showIntroPage();
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
    initGlobalSearch();
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
    
    // Default load state
    if (storedState === '0' && window.innerWidth > 1100) {
        _applyState(false);
    } else {
        _applyState(true);
    }

    brandBtn.style.cursor = 'pointer';
    brandBtn.title = 'Fixer/réduire le menu';

    // Click to pin/unpin
    brandBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        
        // Always kill hover state before applying pin state
        sidebar.classList.remove('hover-expanded');
        
        const willCollapse = !sidebar.classList.contains('collapsed');
        _applyState(willCollapse);
        localStorage.setItem('fortress_sidebar_collapsed', willCollapse ? '1' : '0');
    });

    // JS Hover Architecture (Overlay tray)
    sidebar.addEventListener('mouseenter', () => {
        if (sidebar.classList.contains('collapsed')) {
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
            if (window.innerWidth <= 1100 && !sidebar.classList.contains('collapsed')) {
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
