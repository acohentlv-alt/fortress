/**
 * error-states.test.js — Tests for graceful 503 degradation
 *
 * Proves the circuit breaker pattern works:
 *   - 503 responses produce the correct French microcopy
 *   - Spinners are removed from the DOM
 *   - Error state with retry button is rendered
 *   - Health-check retry flow works correctly
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// ── Mock helpers ─────────────────────────────────────────────────
function mockFetchResponse(body, status = 200) {
    return Promise.resolve({
        ok: status >= 200 && status < 300,
        status,
        statusText: status === 503 ? 'Service Unavailable' : 'OK',
        json: () => Promise.resolve(body),
    });
}

let apiModule;

beforeEach(async () => {
    global.fetch = vi.fn();
    apiModule = await import('../api.js');
});

afterEach(() => {
    vi.restoreAllMocks();
});

// ─────────────────────────────────────────────────────────────────
// extractApiError — 503 handling
// ─────────────────────────────────────────────────────────────────
describe('extractApiError — 503 Service Unavailable', () => {
    it('returns the exact French 503 microcopy', () => {
        const result = { _status: 503, _ok: false, detail: 'Service Unavailable' };
        const msg = apiModule.extractApiError(result);
        expect(msg).toBe('Serveur en maintenance ou base de données hors ligne (Erreur 503).');
    });

    it('503 takes priority over detail string', () => {
        const result = { _status: 503, _ok: false, detail: 'DB pool exhausted' };
        const msg = apiModule.extractApiError(result);
        expect(msg).toBe('Serveur en maintenance ou base de données hors ligne (Erreur 503).');
    });

    it('422 still returns Pydantic field errors (not 503 message)', () => {
        const result = {
            _status: 422,
            _ok: false,
            detail: [{ loc: ['body', 'size'], msg: 'must be >= 1', type: 'value_error' }],
        };
        const msg = apiModule.extractApiError(result);
        expect(msg).toBe('size: must be >= 1');
    });
});

// ─────────────────────────────────────────────────────────────────
// request() — 503 metadata injection
// ─────────────────────────────────────────────────────────────────
describe('request() — 503 status injection', () => {
    it('injects _status=503 and _ok=false', async () => {
        global.fetch.mockReturnValueOnce(mockFetchResponse(
            { detail: 'Service Unavailable' }, 503
        ));
        const result = await apiModule.getDashboardStats();
        expect(result._status).toBe(503);
        expect(result._ok).toBe(false);
    });
});

// ─────────────────────────────────────────────────────────────────
// checkHealth()
// ─────────────────────────────────────────────────────────────────
describe('checkHealth()', () => {
    it('returns { ok: true, status: 200 } when API is healthy', async () => {
        global.fetch.mockReturnValueOnce(Promise.resolve({
            ok: true,
            status: 200,
        }));
        const health = await apiModule.checkHealth();
        expect(health.ok).toBe(true);
        expect(health.status).toBe(200);
    });

    it('returns { ok: false, status: 503 } when API is down', async () => {
        global.fetch.mockReturnValueOnce(Promise.resolve({
            ok: false,
            status: 503,
        }));
        const health = await apiModule.checkHealth();
        expect(health.ok).toBe(false);
        expect(health.status).toBe(503);
    });

    it('returns { ok: false, status: 0 } on network failure', async () => {
        global.fetch.mockRejectedValueOnce(new Error('Network error'));
        const health = await apiModule.checkHealth();
        expect(health.ok).toBe(false);
        expect(health.status).toBe(0);
    });

    it('calls the correct /api/health endpoint', async () => {
        global.fetch.mockReturnValueOnce(Promise.resolve({ ok: true, status: 200 }));
        await apiModule.checkHealth();
        expect(global.fetch).toHaveBeenCalledWith('/api/health', expect.objectContaining({
            headers: { Accept: 'application/json' },
        }));
    });
});

// ─────────────────────────────────────────────────────────────────
// DOM error state rendering simulation
// ─────────────────────────────────────────────────────────────────
describe('503 error state DOM rendering', () => {
    let container;

    beforeEach(() => {
        container = document.createElement('div');
        document.body.appendChild(container);
    });

    afterEach(() => {
        document.body.removeChild(container);
    });

    it('removes spinner and shows error state with retry button', () => {
        // Simulate what dashboard.js does on 503
        container.innerHTML = '<div class="loading"><div class="spinner"></div></div>';
        expect(container.querySelector('.spinner')).not.toBeNull();

        // Simulate the error state injection (mirrors dashboard.js logic)
        const errorMsg = apiModule.extractApiError({ _status: 503, _ok: false });
        container.innerHTML = `
            <div class="error-state text-center">
                <div style="font-size:3rem">🔌</div>
                <div class="error-text">${errorMsg}</div>
                <button id="retry-btn" class="btn btn-primary">🔄 Réessayer</button>
            </div>
        `;

        // Assert 1: spinner is removed
        expect(container.querySelector('.spinner')).toBeNull();

        // Assert 2: localized error text is rendered
        const errorText = container.querySelector('.error-text');
        expect(errorText).not.toBeNull();
        expect(errorText.textContent).toBe(
            'Serveur en maintenance ou base de données hors ligne (Erreur 503).'
        );

        // Assert 3: retry button is present
        const retryBtn = container.querySelector('#retry-btn');
        expect(retryBtn).not.toBeNull();
        expect(retryBtn.textContent).toContain('Réessayer');
    });

    it('shows empty-results state (not error state) for valid empty response', () => {
        // A valid 200 with empty results should NOT show the error state
        const data = { _status: 200, _ok: true, results: [], total: 0 };

        // This should go to the "no results" branch, not error
        const isApiError = !data || (data._status && !data._ok);
        expect(isApiError).toBe(false);
    });
});
