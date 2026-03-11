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
import { renderLogin } from './pages/login.js';
import { getDashboardStats, checkAuthRequired, isLoggedIn, logout } from './api.js';

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
];

function getPageContent() {
    return document.getElementById('page-content');
}

function showLoading() {
    getPageContent().innerHTML = '<div class="loading"><div class="spinner"></div></div>';
}

async function navigate() {
    const hash = window.location.hash || '#/';
    const container = getPageContent();

    // Find matching route
    for (const route of routes) {
        const match = hash.match(route.pattern);
        if (match) {
            // Update active nav
            document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
            const navEl = document.querySelector(`[data-page="${route.nav}"]`);
            if (navEl) navEl.classList.add('active');

            // Clean up previous page (intervals, listeners, etc.)
            _runCleanup();

            // Show loading then render
            showLoading();
            try {
                await route.handler(container, ...match.slice(1));
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
    let debounceTimer;

    input.addEventListener('keydown', (e) => {
        if (e.key === 'Enter') {
            const q = input.value.trim();
            if (q) {
                window.location.hash = `#/search?q=${encodeURIComponent(q)}`;
                input.blur();
            }
        }
    });

    input.addEventListener('input', () => {
        clearTimeout(debounceTimer);
        debounceTimer = setTimeout(() => {
            const q = input.value.trim();
            if (q.length >= 3) {
                window.location.hash = `#/search?q=${encodeURIComponent(q)}`;
            }
        }, 500);
    });
}

// ── Running Job Badge ────────────────────────────────────────────
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

// ── Auth Gate ─────────────────────────────────────────────────────
let _authRequired = false;

async function initApp() {
    const authInfo = await checkAuthRequired();
    _authRequired = authInfo.auth_required;

    if (_authRequired && !isLoggedIn()) {
        // Hide sidebar, show login
        const sidebar = document.getElementById('sidebar');
        if (sidebar) sidebar.style.display = 'none';
        renderLogin(getPageContent(), () => {
            // On successful login, restore UI and navigate
            if (sidebar) sidebar.style.display = '';
            navigate();
        });
        return;
    }

    initGlobalSearch();
    navigate();
    checkRunningJobs();
    // Poll for running jobs every 30s (reduced from 10s to limit DB load)
    setInterval(checkRunningJobs, 30000);

    // Show logout button if auth is active
    if (_authRequired) {
        const logoutBtn = document.getElementById('btn-logout');
        if (logoutBtn) {
            logoutBtn.style.display = 'block';
            logoutBtn.addEventListener('click', (e) => {
                e.preventDefault();
                logout();
                window.location.reload();
            });
        }
    }
}

// ── Init ─────────────────────────────────────────────────────────
window.addEventListener('hashchange', () => {
    if (_authRequired && !isLoggedIn()) return;
    navigate();
});
window.addEventListener('DOMContentLoaded', initApp);
