/**
 * Fortress API Client
 * Wraps all fetch calls to the backend API.
 *
 * Response convention:
 *   - Every response object gets `_status` (HTTP code) and `_ok` (boolean) injected.
 *   - Consumers can check `result._status === 202` for async jobs, `422` for validation, etc.
 *   - Use `extractApiError(result)` to get a human-readable error string.
 */

const API_BASE = '/api';

/**
 * In-memory user state (populated by getCurrentUser on app load).
 * No localStorage needed — the HttpOnly session cookie handles persistence.
 */
let _currentUser = null;

export function getCachedUser() { return _currentUser; }
export function setCachedUser(user) { _currentUser = user; }
export function clearCachedUser() { _currentUser = null; }

/**
 * Core request wrapper.
 * Returns the JSON body with `_status` and `_ok` fields injected.
 * The session cookie is sent automatically via credentials: 'same-origin'.
 */
async function request(path, options = {}) {
    const url = `${API_BASE}${path}`;
    const headers = { 'Accept': 'application/json', ...options.headers };
    try {
        const resp = await fetch(url, {
            ...options,
            headers,
            credentials: 'same-origin',
        });
        let data;
        try { data = await resp.json(); } catch { data = {}; }
        // Inject HTTP metadata into the response object
        if (data && typeof data === 'object' && !Array.isArray(data)) {
            data._status = resp.status;
            data._ok = resp.ok;
        }
        if (resp.status === 401) {
            // Session expired or invalid — redirect to login
            _currentUser = null;
            if (!window.location.hash.startsWith('#/login')) {
                window.location.hash = '#/login';
            }
            return data;
        }
        if (!resp.ok) {
            console.error(`API error: ${resp.status} ${resp.statusText} for ${url}`);
        }
        return data;
    } catch (err) {
        console.error(`API fetch failed: ${url}`, err);
        return { _status: 0, _ok: false, error: err.message };
    }
}

async function postJSON(path, body) {
    return request(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    });
}

async function patchJSON(path, body) {
    return request(path, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    });
}

/**
 * Extract a human-readable error string from an API response.
 * Handles: Pydantic 422 detail arrays, generic error/message fields, fallback.
 */
export function extractApiError(result) {
    if (!result) return 'Erreur de connexion au serveur';

    // 503 Service Unavailable — maintenance / DB offline
    if (result._status === 503) {
        return 'Serveur en maintenance ou base de données hors ligne (Erreur 503).';
    }

    // FastAPI / Pydantic 422 validation errors
    if (result.detail && Array.isArray(result.detail)) {
        return result.detail.map(e => {
            const loc = (e.loc || []).filter(l => l !== 'body').join(' → ') || 'champ';
            return `${loc}: ${e.msg}`;
        }).join('; ');
    }
    // Single string detail (e.g. 404 "Not found")
    if (result.detail && typeof result.detail === 'string') {
        return result.detail;
    }
    // Generic error / message keys
    if (result.error) return result.error;
    if (result.message) return result.message;

    return 'Erreur inconnue du serveur';
}

// ── Dashboard ────────────────────────────────────────────────────
export async function getDashboardStats() {
    return await request('/dashboard/stats');
}

export async function getDashboardStatsByJob() {
    return await request('/dashboard/stats/by-job');
}

export async function getDataBank() {
    return await request('/dashboard/data-bank');
}

/** Browse all enriched entities with contact data (paginated). */
export async function getAllData({ q = '', department = '', limit = 50, offset = 0 } = {}) {
    const params = new URLSearchParams({ limit: limit.toString(), offset: offset.toString() });
    if (q) params.set('q', q);
    if (department) params.set('department', department);
    return await request(`/dashboard/all-data?${params}`);
}

// ── Departments ──────────────────────────────────────────────────
export async function getDepartments() {
    return await request('/departments');
}

export async function getDepartmentJobs(dept) {
    return await request(`/departments/${dept}/jobs`);
}

// ── Jobs ─────────────────────────────────────────────────────────
export async function getJobs() {
    return await request('/jobs');
}

export async function getJob(queryId) {
    return await request(`/jobs/${encodeURIComponent(queryId)}`);
}

export async function deleteJob(queryId) {
    return await request(`/jobs/${encodeURIComponent(queryId)}`, { method: 'DELETE' });
}

export async function cancelJob(queryId) {
    return await request(`/jobs/${encodeURIComponent(queryId)}/cancel`, { method: 'POST' });
}

export async function retryJob(queryId) {
    return await request(`/jobs/${encodeURIComponent(queryId)}/retry`, { method: 'POST' });
}

export async function resumeBatch(queryId) {
    return await request(`/batch/${encodeURIComponent(queryId)}/resume`, { method: 'POST' });
}

export async function untagCompany(siren, queryName) {
    return await request(`/companies/${siren}/tags/${encodeURIComponent(queryName)}`, { method: 'DELETE' });
}

export async function getJobCompanies(queryId, { page = 1, pageSize = 20, search = '', sort = 'completude' } = {}) {
    const params = new URLSearchParams({
        page: page.toString(),
        page_size: pageSize.toString(),
        search,
        sort,
    });
    return await request(`/jobs/${encodeURIComponent(queryId)}/companies?${params}`);
}

export async function getJobQuality(queryId) {
    return await request(`/jobs/${encodeURIComponent(queryId)}/quality`);
}

// ── Companies ────────────────────────────────────────────────────
export async function searchCompanies(query, {
    limit = 50, offset = 0, sortBy = '', order = '', department = '', sector = '',
    minRating = '', minReviews = '',
} = {}) {
    const params = new URLSearchParams({
        q: query,
        limit: limit.toString(),
        offset: offset.toString(),
    });
    if (sortBy) params.set('sort_by', sortBy);
    if (order) params.set('order', order);
    if (department) params.set('department', department);
    if (sector) params.set('sector', sector);
    if (minRating) params.set('min_rating', minRating);
    if (minReviews) params.set('min_reviews', minReviews);
    return await request(`/companies/search?${params}`);
}

/** Search the raw SIRENE DB (14.7M companies) — no enrichment data. */
export async function searchSirene(query, {
    limit = 50, offset = 0, department = null, nafCode = null, statut = 'A',
} = {}) {
    const params = new URLSearchParams({
        q: query,
        limit: limit.toString(),
        offset: offset.toString(),
    });
    if (department) params.set('department', department);
    if (nafCode) params.set('naf_code', nafCode);
    if (statut) params.set('statut', statut);
    return await request(`/sirene/search?${params}`);
}

export async function getCompanyEnrichHistory(siren) {
    return await request(`/companies/${siren}/enrich-history`);
}

export async function getCompany(siren) {
    return await request(`/companies/${siren}`);
}

// ── Enrichment ───────────────────────────────────────────────────
export async function enrichCompany(siren, targetModules) {
    return await postJSON(`/companies/${siren}/enrich`, {
        target_modules: targetModules,
    });
}

/** PATCH individual fields on a company (inline edit). */
export async function updateCompany(siren, fields) {
    return await patchJSON(`/companies/${siren}`, fields);
}

// ── Batch Execution ──────────────────────────────────────────────
export async function runBatch({ sector, department, size, mode, city, naf_code, strategy, search_queries }) {
    const body = {
        sector,
        department,
        size: parseInt(size, 10),
        mode,
        city: city || null,
        strategy: strategy || 'sirene',
    };
    if (naf_code) body.naf_code = naf_code;
    if (search_queries && search_queries.length > 0) body.search_queries = search_queries;
    return await postJSON('/batch/run', body);
}

// ── Export ────────────────────────────────────────────────────────
export function getExportUrl(queryId, format = 'csv') {
    return `${API_BASE}/export/${encodeURIComponent(queryId)}/${format}`;
}

export function getMasterExportUrl(format = 'csv') {
    return `${API_BASE}/export/master/${format}`;
}

/** POST bulk export — sends list of SIRENs, returns CSV file. */
export async function bulkExportCSV(sirens) {
    const resp = await fetch(`${API_BASE}/export/bulk/csv`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sirens }),
    });
    return resp;
}

// ── Health Check ─────────────────────────────────────────────────
export async function checkHealth() {
    try {
        const resp = await fetch(`${API_BASE}/health`, {
            headers: { 'Accept': 'application/json' },
        });
        return { ok: resp.ok, status: resp.status };
    } catch {
        return { ok: false, status: 0 };
    }
}

// ── Client Upload (Smart Ingestion) ──────────────────────────────

/** Preview file mapping without ingesting. Returns column mapping summary. */
export async function previewUpload(file) {
    const formData = new FormData();
    formData.append('file', file);
    try {
        const resp = await fetch(`${API_BASE}/client/preview`, {
            method: 'POST',
            body: formData,
            credentials: 'same-origin',
        });
        let data;
        try { data = await resp.json(); } catch { data = {}; }
        if (data && typeof data === 'object') {
            data._status = resp.status;
            data._ok = resp.ok;
        }
        return data;
    } catch (err) {
        return { _status: 0, _ok: false, error: err.message };
    }
}

/** Upload and ingest file with smart column mapping. */
export async function uploadClientFile(file) {
    const formData = new FormData();
    formData.append('file', file);
    try {
        const resp = await fetch(`${API_BASE}/client/upload`, {
            method: 'POST',
            body: formData,
            credentials: 'same-origin',
        });
        let data;
        try { data = await resp.json(); } catch { data = {}; }
        if (data && typeof data === 'object') {
            data._status = resp.status;
            data._ok = resp.ok;
        }
        return data;
    } catch (err) {
        return { _status: 0, _ok: false, error: err.message };
    }
}

// Keep backward compat
export const uploadClientCSV = uploadClientFile;

export async function getClientStats() {
    return await request('/client/stats');
}

export async function clearClientSirens() {
    return await request('/client/clear', { method: 'DELETE' });
}

// ── Dashboard Delete Actions ─────────────────────────────────────
export async function getSectorStats() {
    return await request('/dashboard/stats/by-sector');
}

export async function getAnalysis() {
    return await request('/dashboard/analysis');
}

export async function deleteSectorTags(sector) {
    return await request(`/dashboard/sector/${encodeURIComponent(sector)}/tags`, { method: 'DELETE' });
}

export async function deleteDeptTags(dept) {
    return await request(`/dashboard/department/${encodeURIComponent(dept)}/tags`, { method: 'DELETE' });
}

export async function deleteJobGroup(queryName) {
    return await request(`/dashboard/job-group/${encodeURIComponent(queryName)}`, { method: 'DELETE' });
}

export async function deleteCompanyTag(siren) {
    return await request(`/companies/${siren}/tags/*`, { method: 'DELETE' });
}

// ── Auth ──────────────────────────────────────────────────────────
export async function checkAuthRequired() {
    try {
        const resp = await fetch(`${API_BASE}/auth/check`);
        return await resp.json();
    } catch {
        return { auth_required: false };
    }
}

export async function loginUser(username, password) {
    const maxRetries = 2;
    for (let attempt = 0; attempt <= maxRetries; attempt++) {
        try {
            const resp = await fetch(`${API_BASE}/auth/login`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                credentials: 'same-origin',
                body: JSON.stringify({ username, password }),
            });
            const data = await resp.json();
            if (resp.ok && data.status === 'ok') {
                _currentUser = data.user;
                return { ok: true, user: data.user };
            }
            // 503 = DB cold start — retry with backoff
            if (resp.status === 503 && attempt < maxRetries) {
                await new Promise(r => setTimeout(r, 1500 * (attempt + 1)));
                continue;
            }
            return { ok: false, error: data.error || 'Identifiants incorrects.' };
        } catch (err) {
            // Network error — retry
            if (attempt < maxRetries) {
                await new Promise(r => setTimeout(r, 1500 * (attempt + 1)));
                continue;
            }
            return { ok: false, error: err.message };
        }
    }
    return { ok: false, error: 'Serveur temporairement indisponible. Réessayez.' };
}

export async function logoutUser() {
    try {
        await fetch(`${API_BASE}/auth/logout`, {
            method: 'POST',
            credentials: 'same-origin',
        });
    } catch { /* ignore */ }
    _currentUser = null;
}

export async function getCurrentUser() {
    try {
        const resp = await fetch(`${API_BASE}/auth/me`, {
            credentials: 'same-origin',
        });
        if (resp.ok) {
            const data = await resp.json();
            _currentUser = data.user;
            return data.user;
        }
        _currentUser = null;
        return null;
    } catch {
        _currentUser = null;
        return null;
    }
}
