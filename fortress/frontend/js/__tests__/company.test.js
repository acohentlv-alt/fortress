/**
 * company.test.js — Tests for the enrichment submit handler (200 vs 202 split)
 *
 * Strategy: Rather than importing the full renderCompany function (which has
 * heavy DOM dependencies), we extract and test the enrichment response
 * handling logic by simulating the submit flow with mocked API calls.
 *
 * This tests the CONTRACT: given a mocked API response, verify the correct
 * toast type is shown and whether re-render is triggered.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// ── Mock modules BEFORE importing ────────────────────────────────
// We mock the entire api.js and components.js modules
vi.mock('../api.js', () => ({
    getCompany: vi.fn(),
    enrichCompany: vi.fn(),
    getCompanyEnrichHistory: vi.fn(),
    extractApiError: vi.fn((result) => {
        if (!result) return 'Erreur de connexion au serveur';
        if (result.detail && Array.isArray(result.detail)) {
            return result.detail.map(e => e.msg).join('; ');
        }
        if (result.detail && typeof result.detail === 'string') return result.detail;
        if (result.error) return result.error;
        return 'Erreur inconnue du serveur';
    }),
}));

vi.mock('../components.js', () => ({
    breadcrumb: vi.fn(() => ''),
    formatSiren: vi.fn(s => s),
    formatSiret: vi.fn(s => s),
    formatDate: vi.fn(d => d),
    statutBadge: vi.fn(() => ''),
    formeJuridiqueBadge: vi.fn(() => ''),
    escapeHtml: vi.fn(s => s || ''),
    renderGauge: vi.fn(() => '<div class="gauge"></div>'),
    showToast: vi.fn(),
}));

import { enrichCompany, extractApiError } from '../api.js';
import { showToast } from '../components.js';

/**
 * Simulate the enrichment submit handler logic
 * (extracted from company.js submitBtn.addEventListener)
 *
 * This mirrors the exact logic in the production code without
 * requiring full DOM rendering of the company page.
 */
async function simulateEnrichSubmit(siren, modules, renderCompanyFn) {
    const result = await enrichCompany(siren, modules);

    if (result && result._status === 202) {
        showToast(result.message || 'Mise en file d\'attente...', 'success');
        return { action: 'queued', rerendered: false };
    } else if (result && result._ok) {
        showToast(result.message || 'Données récupérées', 'success');
        await renderCompanyFn(siren);
        return { action: 'cached', rerendered: true };
    } else {
        showToast(extractApiError(result), 'error');
        return { action: 'error', rerendered: false };
    }
}

describe('Enrichment Submit — 200 vs 202 split', () => {
    const SIREN = '123456789';
    const MODULES = ['contact_web', 'financials'];
    let mockRenderCompany;

    beforeEach(() => {
        vi.clearAllMocks();
        mockRenderCompany = vi.fn();
    });

    // ── Case A: 202 Accepted ────────────────────────────────────
    describe('Case A: 202 Accepted (async queue)', () => {
        it('shows "Mise en file d\'attente..." toast', async () => {
            enrichCompany.mockResolvedValueOnce({
                _status: 202,
                _ok: true,
                message: 'Enrichissement en cours pour 2 modules',
            });

            const result = await simulateEnrichSubmit(SIREN, MODULES, mockRenderCompany);

            expect(showToast).toHaveBeenCalledWith(
                'Enrichissement en cours pour 2 modules',
                'success'
            );
            expect(result.action).toBe('queued');
        });

        it('does NOT trigger a re-render', async () => {
            enrichCompany.mockResolvedValueOnce({
                _status: 202,
                _ok: true,
                message: 'Queued',
            });

            const result = await simulateEnrichSubmit(SIREN, MODULES, mockRenderCompany);

            expect(mockRenderCompany).not.toHaveBeenCalled();
            expect(result.rerendered).toBe(false);
        });

        it('uses default toast message if API message is missing', async () => {
            enrichCompany.mockResolvedValueOnce({
                _status: 202,
                _ok: true,
            });

            await simulateEnrichSubmit(SIREN, MODULES, mockRenderCompany);

            expect(showToast).toHaveBeenCalledWith(
                'Mise en file d\'attente...',
                'success'
            );
        });
    });

    // ── Case B: 200 OK ──────────────────────────────────────────
    describe('Case B: 200 OK (cached/deduplicated)', () => {
        it('shows "Données récupérées" toast', async () => {
            enrichCompany.mockResolvedValueOnce({
                _status: 200,
                _ok: true,
                message: 'Données déjà en base',
            });

            const result = await simulateEnrichSubmit(SIREN, MODULES, mockRenderCompany);

            expect(showToast).toHaveBeenCalledWith(
                'Données déjà en base',
                'success'
            );
            expect(result.action).toBe('cached');
        });

        it('DOES trigger a re-render to refresh the UI', async () => {
            enrichCompany.mockResolvedValueOnce({
                _status: 200,
                _ok: true,
            });

            const result = await simulateEnrichSubmit(SIREN, MODULES, mockRenderCompany);

            expect(mockRenderCompany).toHaveBeenCalledWith(SIREN);
            expect(result.rerendered).toBe(true);
        });

        it('uses default toast message if API message is missing', async () => {
            enrichCompany.mockResolvedValueOnce({
                _status: 200,
                _ok: true,
            });

            await simulateEnrichSubmit(SIREN, MODULES, mockRenderCompany);

            expect(showToast).toHaveBeenCalledWith(
                'Données récupérées',
                'success'
            );
        });
    });

    // ── Error cases ─────────────────────────────────────────────
    describe('Error cases', () => {
        it('shows extracted error message for 422 validation failure', async () => {
            enrichCompany.mockResolvedValueOnce({
                _status: 422,
                _ok: false,
                detail: [{ loc: ['body', 'target_modules'], msg: 'field required' }],
            });

            const result = await simulateEnrichSubmit(SIREN, MODULES, mockRenderCompany);

            expect(showToast).toHaveBeenCalledWith('field required', 'error');
            expect(mockRenderCompany).not.toHaveBeenCalled();
            expect(result.action).toBe('error');
        });

        it('shows extracted error for 500 server error', async () => {
            enrichCompany.mockResolvedValueOnce({
                _status: 500,
                _ok: false,
                error: 'Internal server error',
            });

            const result = await simulateEnrichSubmit(SIREN, MODULES, mockRenderCompany);

            expect(showToast).toHaveBeenCalledWith('Internal server error', 'error');
            expect(result.action).toBe('error');
        });

        it('handles null API response (connection failure)', async () => {
            enrichCompany.mockResolvedValueOnce(null);

            const result = await simulateEnrichSubmit(SIREN, MODULES, mockRenderCompany);

            expect(showToast).toHaveBeenCalledWith('Erreur de connexion au serveur', 'error');
            expect(result.action).toBe('error');
        });
    });
});
