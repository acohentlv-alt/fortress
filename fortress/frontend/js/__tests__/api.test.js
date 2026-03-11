/**
 * api.test.js — Tests for the API client wrapper and error extraction
 *
 * Mocks the global `fetch` to simulate backend responses.
 * Tests: _status/_ok injection, extractApiError with Pydantic 422,
 *        searchCompanies query param construction, error edge cases.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// We need to mock fetch BEFORE importing the module
let apiModule;

beforeEach(async () => {
    global.fetch = vi.fn();
    // Fresh import each time to pick up the mock
    apiModule = await import('../api.js');
});

afterEach(() => {
    vi.restoreAllMocks();
});

// ── Helper to create a mock Response ────────────────────────────
function mockFetchResponse(body, status = 200) {
    return Promise.resolve({
        ok: status >= 200 && status < 300,
        status,
        statusText: status === 200 ? 'OK' : status === 422 ? 'Unprocessable Entity' : 'Error',
        json: () => Promise.resolve(body),
    });
}

// ─────────────────────────────────────────────────────────────────
// extractApiError
// ─────────────────────────────────────────────────────────────────
describe('extractApiError', () => {
    it('returns fallback for null/undefined input', () => {
        expect(apiModule.extractApiError(null)).toBe('Erreur de connexion au serveur');
        expect(apiModule.extractApiError(undefined)).toBe('Erreur de connexion au serveur');
    });

    it('extracts Pydantic 422 detail array (single error)', () => {
        const response = {
            detail: [{ loc: ['body', 'size'], msg: 'size must be greater than 0', type: 'value_error' }],
        };
        const msg = apiModule.extractApiError(response);
        expect(msg).toBe('size: size must be greater than 0');
    });

    it('extracts Pydantic 422 detail array (multiple errors)', () => {
        const response = {
            detail: [
                { loc: ['body', 'sector'], msg: 'field required', type: 'missing' },
                { loc: ['body', 'department'], msg: 'field required', type: 'missing' },
            ],
        };
        const msg = apiModule.extractApiError(response);
        expect(msg).toBe('sector: field required; department: field required');
    });

    it('filters "body" from Pydantic loc path', () => {
        const response = {
            detail: [{ loc: ['body', 'size'], msg: 'too small', type: 'value_error' }],
        };
        const msg = apiModule.extractApiError(response);
        expect(msg).not.toContain('body');
        expect(msg).toBe('size: too small');
    });

    it('handles Pydantic error with empty loc', () => {
        const response = {
            detail: [{ loc: [], msg: 'general error', type: 'value_error' }],
        };
        const msg = apiModule.extractApiError(response);
        expect(msg).toBe('champ: general error');
    });

    it('extracts string detail (e.g. 404)', () => {
        const response = { detail: 'Not found' };
        expect(apiModule.extractApiError(response)).toBe('Not found');
    });

    it('extracts generic error field', () => {
        const response = { error: 'Internal server error' };
        expect(apiModule.extractApiError(response)).toBe('Internal server error');
    });

    it('extracts generic message field', () => {
        const response = { message: 'Rate limited' };
        expect(apiModule.extractApiError(response)).toBe('Rate limited');
    });

    it('returns fallback for empty object', () => {
        expect(apiModule.extractApiError({})).toBe('Erreur inconnue du serveur');
    });
});

// ─────────────────────────────────────────────────────────────────
// request() — _status / _ok injection
// ─────────────────────────────────────────────────────────────────
describe('request() — status metadata injection', () => {
    it('injects _status=200 and _ok=true on success', async () => {
        global.fetch.mockReturnValueOnce(mockFetchResponse({ data: 'ok' }, 200));
        const result = await apiModule.getDashboardStats();
        expect(result._status).toBe(200);
        expect(result._ok).toBe(true);
        expect(result.data).toBe('ok');
    });

    it('injects _status=422 and _ok=false on validation error', async () => {
        const errorBody = { detail: [{ loc: ['body', 'size'], msg: 'too small' }] };
        global.fetch.mockReturnValueOnce(mockFetchResponse(errorBody, 422));
        const result = await apiModule.getDashboardStats();
        expect(result._status).toBe(422);
        expect(result._ok).toBe(false);
        expect(result.detail).toEqual(errorBody.detail);
    });

    it('injects _status=202 and _ok=true for accepted', async () => {
        global.fetch.mockReturnValueOnce(mockFetchResponse({ message: 'Queued' }, 202));
        const result = await apiModule.getDashboardStats();
        expect(result._status).toBe(202);
        expect(result._ok).toBe(true);
    });

    it('returns error object on network failure', async () => {
        global.fetch.mockRejectedValueOnce(new Error('Network error'));
        const result = await apiModule.getDashboardStats();
        expect(result._status).toBe(0);
        expect(result._ok).toBe(false);
        expect(result.error).toBe('Network error');
    });
});

// ─────────────────────────────────────────────────────────────────
// searchCompanies — query parameter construction
// ─────────────────────────────────────────────────────────────────
describe('searchCompanies() — query params', () => {
    it('sends default offset=0 and limit=50', async () => {
        global.fetch.mockReturnValueOnce(mockFetchResponse({ results: [], total: 0 }));
        await apiModule.searchCompanies('test');

        const calledUrl = global.fetch.mock.calls[0][0];
        const url = new URL(calledUrl, 'http://localhost');
        expect(url.searchParams.get('q')).toBe('test');
        expect(url.searchParams.get('offset')).toBe('0');
        expect(url.searchParams.get('limit')).toBe('50');
    });

    it('passes custom offset and limit', async () => {
        global.fetch.mockReturnValueOnce(mockFetchResponse({ results: [], total: 100 }));
        await apiModule.searchCompanies('boulangerie', { offset: 50, limit: 25 });

        const calledUrl = global.fetch.mock.calls[0][0];
        const url = new URL(calledUrl, 'http://localhost');
        expect(url.searchParams.get('offset')).toBe('50');
        expect(url.searchParams.get('limit')).toBe('25');
    });

    it('includes department and sector filters when provided', async () => {
        global.fetch.mockReturnValueOnce(mockFetchResponse({ results: [] }));
        await apiModule.searchCompanies('test', { department: '75', sector: 'AGRICULTURE' });

        const calledUrl = global.fetch.mock.calls[0][0];
        const url = new URL(calledUrl, 'http://localhost');
        expect(url.searchParams.get('department')).toBe('75');
        expect(url.searchParams.get('sector')).toBe('AGRICULTURE');
    });

    it('does not include empty filter params', async () => {
        global.fetch.mockReturnValueOnce(mockFetchResponse({ results: [] }));
        await apiModule.searchCompanies('test', { department: '', sector: '' });

        const calledUrl = global.fetch.mock.calls[0][0];
        const url = new URL(calledUrl, 'http://localhost');
        expect(url.searchParams.has('department')).toBe(false);
        expect(url.searchParams.has('sector')).toBe(false);
    });
});

// ─────────────────────────────────────────────────────────────────
// runBatch — payload construction
// ─────────────────────────────────────────────────────────────────
describe('runBatch() — POST payload', () => {
    it('sends correct JSON body with parsed size integer', async () => {
        global.fetch.mockReturnValueOnce(mockFetchResponse({ status: 'launched' }));
        await apiModule.runBatch({
            sector: 'transport',
            department: '31',
            size: '50',
            mode: 'discovery',
            city: 'Toulouse',
        });

        const call = global.fetch.mock.calls[0];
        const body = JSON.parse(call[1].body);
        expect(body.sector).toBe('transport');
        expect(body.department).toBe('31');
        expect(body.size).toBe(50); // parsed from string '50'
        expect(body.mode).toBe('discovery');
        expect(body.city).toBe('Toulouse');
    });

    it('sends city as null when not provided', async () => {
        global.fetch.mockReturnValueOnce(mockFetchResponse({ status: 'launched' }));
        await apiModule.runBatch({
            sector: 'agriculture',
            department: '11',
            size: '20',
            mode: 'discovery',
        });

        const body = JSON.parse(global.fetch.mock.calls[0][1].body);
        expect(body.city).toBeNull();
    });
});
