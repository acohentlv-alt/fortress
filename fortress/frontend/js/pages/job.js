/**
 * Job Page — Drill-down into a specific job
 */

import { getJob, getJobCompanies, getJobQuality, getJobSummary, getExportUrl, deleteJob, untagCompany, enrichCompany, startDeepEnrich, resumeJob, getCachedUser } from '../api.js';
import { renderGauge, companyCard, renderPagination, breadcrumb, statusBadge, formatDateTime, escapeHtml, showConfirmModal, showToast } from '../components.js';
import { GlobalSelection } from '../state.js';
import { t } from '../i18n.js';
import { DEPT_NAMES } from '../constants.js';
import { renderQueriesPanel, bindQueriesPanelClicks } from '../components/queries_panel.js';

// ── Selection state ──────────────────────────────────────────────
let selectionMode = false;
let selectedSirens = GlobalSelection;
let _currentBatchId = null;
let _currentBatchName = null;
let _currentPage = 1;
let _currentSort = 'completude';
let _currentFilter = '';  // '' | 'naf_confirmed' | 'naf_sibling' | 'pending' | 'unlinked'
let _currentSearchQuery = '';  // E4.A — active drill-down query filter

/** Parse ?q= param from hash-based URL (#/job/ID?q=...) */
function _readQueryParamFromHash() {
    const hash = window.location.hash || '';
    const idx = hash.indexOf('?');
    if (idx < 0) return '';
    const params = new URLSearchParams(hash.slice(idx + 1));
    return params.get('q') || '';
}

// ── Scoreboard card — hero card at top of job page ───────────────
// Replaces buildSummaryCard + buildLinkStatsCard + the Progress card.
// Contains: header row, batch shape bar + legend, hero metrics, action chips,
// and disclosure "Voir le détail par méthode".
function renderBulkDiscoveryPanel(meta, data) {
    const actualEntities = data.companies_qualified || 0;
    const targetEntities = meta.expected_entities || 0;
    const pct = targetEntities ? Math.round(100 * actualEntities / targetEntities) : 0;
    return `
        <div class="bulk-discovery-summary">
            <h3>🌍 ${t('job.bulkDiscovery.title')}</h3>
            <table>
                <tr><td>${t('job.bulkDiscovery.dept')}</td><td>${meta.dept || ''} — ${meta.dept_name || ''}</td></tr>
                <tr><td>${t('job.bulkDiscovery.communesTargeted')}</td><td>${meta.recommended} / ${meta.communes_with_sirene_match}</td></tr>
                <tr><td>${t('job.bulkDiscovery.skipped')}</td><td>${meta.skipped}</td></tr>
                <tr><td>${t('job.bulkDiscovery.estimation')}</td><td>${targetEntities} entités · ≈${meta.estimated_minutes} min</td></tr>
                <tr><td>${t('job.bulkDiscovery.actual')}</td><td>${actualEntities} entités · ${pct}% du target</td></tr>
            </table>
        </div>
    `;
}

// TODO consolidate: monitor.js + dashboard.js have similar duration formatters; extract to components.js when convenient.
function formatLongDuration(sec) {
    if (sec == null || sec < 0) return '';
    if (sec < 60) return `${sec}s`;
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = sec % 60;
    if (h > 0) return `${h}h ${m}m ${s}s`;
    return `${m}m ${s}s`;
}

/**
 * Build the "Vitesse du batch" panel from job.timing_breakdown JSONB.
 * Returns empty string when data is unavailable (legacy batches with NULL timing_breakdown).
 */
function buildTimingPanel(job) {
    const tb = job.timing_breakdown;
    if (!tb || typeof tb !== 'object') return '';

    const totalSec = Number(tb.total_runtime_sec || 0);
    const qual = Number(tb.qualified_count || 0);
    const rate = Number(tb.qualified_per_min || 0);
    const totalMin = Math.round(totalSec / 60 * 10) / 10;

    // Bucket display order — sorted by percentage descending
    const sb = tb.shortfall_breakdown || {};
    const buckets = [
        { key: 'maps_scrape', i18n: 'job.timing.bucketMapsScrape',
          steps: ['maps_scroll', 'maps_card'] },
        { key: 'match_cascade', i18n: 'job.timing.bucketMatchCascade',
          steps: ['match_cascade', 'inpi_step0', 'inpi_step0_assoc_fallback',
                  'step_2_7_siret_addr_naf'] },
        { key: 'website_crawl', i18n: 'job.timing.bucketWebsiteCrawl',
          steps: ['crawl'] },
        { key: 'inpi_officers', i18n: 'job.timing.bucketInpiOfficers',
          steps: ['inpi_officers'] },
        { key: 'gemini_judge', i18n: 'job.timing.bucketGeminiJudge',
          steps: ['gemini_judge'] },
        { key: 'db_write', i18n: 'job.timing.bucketDbWrite',
          steps: ['db_write'] },
    ];

    const perStep = tb.per_step || {};
    const rows = buckets.map(b => {
        const totalSecBucket = b.steps.reduce(
            (acc, s) => acc + (perStep[s]?.total_sec || 0), 0
        );
        const pct = sb[b.key + '_pct'] || 0;
        // Aggregate p50/p95 across steps in bucket — use weighted-by-n when multiple
        let p50 = 0, p95 = 0, n = 0;
        b.steps.forEach(s => {
            const sd = perStep[s];
            if (sd && sd.n > 0) {
                p50 += (sd.p50_ms || 0) * sd.n;
                p95 += Math.max(p95, sd.p95_ms || 0); // p95 we take MAX (worst-case across steps)
                n += sd.n;
            }
        });
        p50 = n > 0 ? Math.round(p50 / n) : 0;
        return { ...b, totalSec: totalSecBucket, pct, p50, p95: Math.round(p95), n };
    }).filter(r => r.n > 0).sort((a, b) => b.pct - a.pct);

    const otherPct = sb.other_pct || 0;
    const otherSec = totalSec * otherPct;

    const fmtMs = (ms) => ms >= 1000 ? `${(ms / 1000).toFixed(1)}s` : `${ms}ms`;
    const fmtSec = (s) => s >= 60 ? `${Math.round(s / 60 * 10) / 10}min` : `${Math.round(s)}s`;
    const fmtPct = (p) => `${Math.round(p * 100)}%`;

    return `
        <div class="card" id="timing-card" style="margin-bottom:var(--space-xl)">
            <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                ${t('job.timing.title')}
            </h3>
            <div style="display:flex; align-items:baseline; gap:var(--space-md); margin-bottom:var(--space-lg); flex-wrap:wrap">
                <span style="font-size:var(--font-lg); font-weight:600">
                    ${t('job.timing.headline', { qualified: qual, minutes: totalMin })}
                </span>
                <span style="color:var(--text-muted); font-size:var(--font-sm)">
                    ${t('job.timing.rate', { rate: rate.toFixed(2) })}
                </span>
            </div>
            <table style="width:100%; border-collapse:collapse; font-size:var(--font-sm)">
                <thead>
                    <tr style="border-bottom:1px solid var(--border-default); color:var(--text-muted); font-size:var(--font-xs)">
                        <th style="text-align:left; padding:var(--space-sm) var(--space-md)">${t('job.timing.colStep')}</th>
                        <th style="text-align:right; padding:var(--space-sm) var(--space-md)">${t('job.timing.colP50')}</th>
                        <th style="text-align:right; padding:var(--space-sm) var(--space-md)">${t('job.timing.colP95')}</th>
                        <th style="text-align:right; padding:var(--space-sm) var(--space-md)">${t('job.timing.colTotal')}</th>
                        <th style="text-align:right; padding:var(--space-sm) var(--space-md)">${t('job.timing.colPct')}</th>
                    </tr>
                </thead>
                <tbody>
                    ${rows.map(r => `
                        <tr style="border-bottom:1px solid var(--border-default); ${r.pct >= 0.4 ? 'background:rgba(245,158,11,0.08)' : ''}">
                            <td style="padding:var(--space-sm) var(--space-md)">${t(r.i18n)} <span style="color:var(--text-muted); font-size:var(--font-xs)">(n=${r.n})</span></td>
                            <td style="text-align:right; padding:var(--space-sm) var(--space-md); font-variant-numeric:tabular-nums">${fmtMs(r.p50)}</td>
                            <td style="text-align:right; padding:var(--space-sm) var(--space-md); font-variant-numeric:tabular-nums">${fmtMs(r.p95)}</td>
                            <td style="text-align:right; padding:var(--space-sm) var(--space-md); font-variant-numeric:tabular-nums">${fmtSec(r.totalSec)}</td>
                            <td style="text-align:right; padding:var(--space-sm) var(--space-md); font-variant-numeric:tabular-nums; font-weight:600">${fmtPct(r.pct)}</td>
                        </tr>
                    `).join('')}
                    ${otherPct > 0.01 ? `
                        <tr style="color:var(--text-muted)">
                            <td style="padding:var(--space-sm) var(--space-md)">${t('job.timing.bucketOther')}</td>
                            <td colspan="2"></td>
                            <td style="text-align:right; padding:var(--space-sm) var(--space-md); font-variant-numeric:tabular-nums">${fmtSec(otherSec)}</td>
                            <td style="text-align:right; padding:var(--space-sm) var(--space-md); font-variant-numeric:tabular-nums">${fmtPct(otherPct)}</td>
                        </tr>
                    ` : ''}
                </tbody>
            </table>
            <div style="margin-top:var(--space-md); color:var(--text-muted); font-size:var(--font-xs)">
                ${t('job.timing.footnote')}
            </div>
        </div>
    `;
}

/**
 * Compute a human-readable scope label for the hero tile.
 * Reflects which subset of data is being shown (strict NAF, dept filter, or both).
 */
function computeScopeLabel(job) {
    const strict = Boolean(job.strict_naf);
    let filtersObj = job.filters_json;
    if (typeof filtersObj === 'string') {
        try { filtersObj = JSON.parse(filtersObj); } catch (_) { filtersObj = null; }
    }
    const dept = (filtersObj && filtersObj.department) || '';
    const deptFilter = (dept && dept !== 'FR' && dept !== 'ALL') ? dept : null;
    if (strict && deptFilter) return t('job.scopeStrictDept', { dept: deptFilter });
    if (strict) return t('job.scopeStrict');
    if (deptFilter) return t('job.scopeDept', { dept: deptFilter });
    return t('job.scopeAllBatch');
}

function buildScoreboardCard(job, linkStats, summary) {
    if (!linkStats) return '';
    const confirmed = linkStats.confirmed || 0;
    const pending = linkStats.pending || 0;
    const unlinked = linkStats.unlinked || 0;
    const total = linkStats.total || (confirmed + pending + unlinked);
    // Bug 2 fix: keep both gross + clickable. Bar/legend/greenCount use clickable;
    // nafNotEvaluated calc at line 366 uses gross to avoid pending-row double-counting.
    const nafVerified = linkStats.naf_verified || 0;                       // gross — used in nafNotEvaluated calc
    const nafMismatch = linkStats.naf_mismatch || 0;                       // gross — used in nafNotEvaluated calc
    const nafVerifiedClickable = linkStats.naf_verified_clickable || 0;    // legend + bar
    const nafMismatchClickable = linkStats.naf_mismatch_clickable || 0;    // legend + bar
    const nafEvaluated = linkStats.naf_evaluated || 0;
    const byMethod = linkStats.by_method || {};
    const byNaf = linkStats.by_naf || [];
    const pickedNafs = job.picked_nafs || [];
    const scraped = job.companies_scraped || 0;
    const qualified = job.companies_qualified || 0;
    const batchSize = job.batch_size || job.total_companies || 1;

    // Determine dominant NAF early — needed by both legend (section B) and hero cards (section C)
    const hasPicked = pickedNafs.length > 0;
    const dominantNaf = (!hasPicked && byNaf.length > 0) ? byNaf[0] : null;

    const pct = (num) => total > 0 ? Math.round((num / total) * 100) : 0;
    const rateColour = (p) => p >= 85 ? 'var(--success)' : p >= 50 ? '#f59e0b' : '#ef4444';

    // ── Method label lookup (ported from old buildLinkStatsCard) ──
    const methodLabel = (m) => {
        const map = {
            inpi: 'company.linkReasonInpi',
            enseigne: 'company.linkReasonEnseigne',
            enseigne_weak: 'company.linkReasonEnseigneWeak',
            phone: 'company.linkReasonPhone',
            phone_weak: 'company.linkReasonPhoneWeak',
            address: 'company.linkReasonAddress',
            siren_website: 'company.linkReasonSirenWebsite',
            fuzzy_name: 'company.linkReasonFuzzy',
            manual: 'company.linkReasonManual',
            surname: 'company.linkReasonSurname',
            inpi_fuzzy_agree: 'company.linkReasonInpiFuzzyAgree',
            inpi_mentions_legales: 'company.linkReasonInpiMentionsLegales',
            chain: 'company.linkReasonChain',
            gemini_judge: 'company.linkReasonGeminiJudge',
            gemini_promoted: 'company.linkReasonGeminiPromoted',
            geo_proximity: 'company.linkReasonGeoProximity',
            cp_name_disamb: 'company.linkReasonCpNameDisamb',
            cp_name_disamb_indiv: 'company.linkReasonCpNameDisambIndiv',
        };
        return map[m] ? t(map[m]) : m;
    };

    // ── A: Header row (h1 + status + actions) ─────────────────────
    const headerHtml = `
        <div style="display:flex; align-items:flex-start; justify-content:space-between; gap:var(--space-xl); flex-wrap:wrap; margin-bottom:var(--space-xl)">
            <div>
                <h1 class="page-title" style="margin-bottom:var(--space-sm)">
                    ${escapeHtml(job.batch_name)}
                    ${job.batch_number ? `<span style="font-size:var(--font-sm); font-weight:400; color:var(--text-muted); margin-left:var(--space-sm)">${t('job.batchNumber', { number: job.batch_number })}${job.duration_sec != null ? ` · ${formatLongDuration(job.duration_sec)}` : ''}</span>` : ''}
                </h1>
                <div style="display:flex; align-items:center; gap:var(--space-md); flex-wrap:wrap">
                    ${statusBadge(job.status)}
                    ${job.strict_naf ? `<span class="strict-mode-badge" title="${t('job.strictMode.tooltip')}">${t('job.strictMode.badge')}</span>` : ''}
                    <span style="color:var(--text-secondary); font-size:var(--font-sm)">
                        ${t('job.createdOn')} ${formatDateTime(job.created_at)}
                    </span>
                    ${(job.triage_green || 0) > 0 ? `<span class="badge" style="background:rgba(34,197,94,0.15); color:rgb(34,197,94); border:1px solid rgba(34,197,94,0.3)">🟢 ${job.triage_green} ${t('job.existingData')}</span>` : ''}
                </div>
            </div>
            <div style="display:flex; gap:var(--space-sm); flex-wrap:wrap">
                ${(job.status === 'completed' || job.status === 'in_progress' || job.status === 'interrupted' || job.status === 'cancelled' || job.status === 'failed') ? `
                    <button id="btn-rapport" class="btn btn-secondary rapport-btn" type="button" title="${t('job.rapportButton')}">
                        📋 ${t('job.rapportButton')}
                    </button>` : ''}
                <div style="position:relative">
                    <button id="btn-download-dropdown" class="btn btn-primary" title="${t('job.download')}">${t('job.download')}</button>
                </div>
                ${(() => {
                    if (job.status !== 'interrupted') return '';
                    const size = job.batch_size || 0;
                    const done = job.companies_scraped || 0;
                    if (size > 0 && done >= size) return '';
                    return `<button id="btn-resume" class="btn btn-primary" title="${t('job.resume')}">${t('job.resume')}</button>`;
                })()}
                ${(job.status === 'completed' || job.status === 'failed' || job.status === 'interrupted' || job.status === 'cancelled') ? `<button id="btn-rerun" class="btn btn-primary" title="${t('job.rerun')}">${t('job.rerun')}</button>` : ''}
                <button id="btn-delete-job" class="btn btn-danger" title="${t('job.delete')}">🗑️</button>
                ${job.status === 'in_progress' ?
                    `<a href="#/monitor/${encodeURIComponent(job.batch_id)}" class="btn btn-primary">${t('job.liveMonitor')}</a>` : ''}
            </div>
        </div>
    `;

    // ── B: Batch shape bar + clickable legend ─────────────────────
    const verified = nafVerifiedClickable;
    const mismatch = nafMismatchClickable;
    const barTotal = total || 1;

    // Build bar segments (omit zero-count)
    const segments = [
        { count: verified, color: 'var(--success)', filter: 'naf_confirmed', label: 'Secteur confirmé' },
        { count: mismatch, color: '#f59e0b', filter: 'naf_sibling', label: 'Secteur proche' },
        { count: pending, color: '#fb923c', filter: 'pending', label: 'À vérifier' },
        { count: unlinked, color: 'var(--text-muted)', filter: 'unlinked', label: 'Sans correspondance' },
    ].filter(s => s.count > 0);

    // If no NAF filter, show confirmed vs pending vs unlinked using overall confirmed
    const simpleSegments = pickedNafs.length === 0 ? [
        { count: confirmed, color: 'var(--success)', filter: 'naf_confirmed', label: 'Identifiées' },
        { count: pending, color: '#fb923c', filter: 'pending', label: 'À vérifier' },
        { count: unlinked, color: 'var(--text-muted)', filter: 'unlinked', label: 'Sans correspondance' },
    ].filter(s => s.count > 0) : segments;

    const useSegments = simpleSegments;

    const barHtml = total > 0 ? `
        <div style="margin-bottom:var(--space-lg)">
            <div style="font-size:var(--font-xs); color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-sm); font-weight:700">
                ${t('job.batchShapeTitle', { total })}
            </div>
            <div style="height:12px; border-radius:6px; overflow:hidden; display:flex; background:var(--bg-elevated)">
                ${useSegments.map(s => {
                    const w = Math.round((s.count / barTotal) * 100);
                    const pctLabel = w + '%';
                    return `<div style="width:${pctLabel}; background:${s.color}; transition:width 0.3s ease" title="${s.label} : ${s.count} (${pctLabel})"></div>`;
                }).join('')}
            </div>
        </div>
    ` : '';

    // ── Legend rows ───────────────────────────────────────────────
    const siblingNafs = byNaf.filter(r => !r.is_picked && r.count > 0);
    const pickedNafEntries = byNaf.filter(r => r.is_picked && r.count > 0);

    // Green legend label (confirmed in picked NAF)
    let greenLegendLabel = '';
    if (pickedNafs.length === 0) {
        // No picked NAFs: show dominant NAF label if available, otherwise plain "Identifiées"
        if (confirmed > 0) {
            if (dominantNaf) {
                greenLegendLabel = t('job.legendSectorConfirmedOne', { code: dominantNaf.code, label: dominantNaf.label });
            } else {
                greenLegendLabel = 'Identifiées dans le registre SIRENE';
            }
        }
    } else if (pickedNafs.length === 1) {
        const entry = byNaf.find(r => r.is_picked);
        greenLegendLabel = t('job.legendSectorConfirmedOne', { code: pickedNafs[0], label: entry ? entry.label : pickedNafs[0] });
    } else {
        greenLegendLabel = t('job.legendSectorConfirmedMany');
    }

    // Amber legend label (siblings)
    let amberLegendLabel = '';
    if (siblingNafs.length === 1) {
        amberLegendLabel = t('job.legendSectorOneSibling', { code: siblingNafs[0].code, label: siblingNafs[0].label });
    } else if (siblingNafs.length === 2 || siblingNafs.length === 3) {
        amberLegendLabel = t('job.legendSectorTwoSiblings', { code1: siblingNafs[0].code, label1: siblingNafs[0].label, code2: siblingNafs[1].code, label2: siblingNafs[1].label });
    } else if (siblingNafs.length >= 4) {
        amberLegendLabel = t('job.legendSectorManySiblings', { count: siblingNafs.length });
    }

    const legendRow = (filter, color, count, label) => `
        <div class="legend-row" data-filter="${filter}" style="cursor:pointer; display:flex; align-items:center; gap:var(--space-sm); padding:6px 8px; border-radius:4px">
            <span style="width:10px; height:10px; border-radius:2px; background:${color}; flex-shrink:0"></span>
            <span style="font-weight:700; min-width:30px">${count}</span>
            <span style="color:var(--text-secondary)">${label}</span>
        </div>
    `;

    // Bug 2 fix (reviewer B1): use verified (= verified_clickable) always.
    // In no-NAF batches, naf_status is never 'verified' so verified_clickable === 0 →
    // the legend self-hides via the `greenCount > 0` guard at line 198. This avoids
    // the silent 4→0 click divergence in no-NAF batches.
    const greenCount = verified;
    // Change 8: drop pending legend row — pending hero card is the canonical surface.
    // Bar segment still present for visual proportion (see barHtml above).
    const legendHtml = `
        <div style="margin-bottom:var(--space-lg)">
            ${greenLegendLabel && greenCount > 0 ? legendRow('naf_confirmed', 'var(--success)', greenCount, greenLegendLabel) : ''}
            ${amberLegendLabel && mismatch > 0 ? legendRow('naf_sibling', '#f59e0b', mismatch, amberLegendLabel) : ''}
            ${unlinked > 0 ? legendRow('unlinked', 'var(--text-muted)', unlinked, t('job.legendUnlinked')) : ''}
        </div>
    `;

    // ── C: Two hero metric cards ───────────────────────────────────
    // Hide the SECTEUR DOMINANT tile for wide batches with no picker — the
    // "dominant" code is essentially random when the matcher had no sector
    // constraint. Keep it for picker batches AND for strict batches (where it
    // shows the strict-match rate).
    const isStrict = !!job.strict_naf;
    const showSectorCard = hasPicked || isStrict;

    const confirmPct = pct(confirmed);

    // Numerator/denominator for Secteur metric
    let sectorNum, sectorDenom, sectorPct;
    if (hasPicked) {
        sectorNum = nafVerifiedClickable;
        sectorDenom = total;
        sectorPct = pct(nafVerifiedClickable);
    } else if (dominantNaf) {
        sectorNum = dominantNaf.count;
        sectorDenom = confirmed;
        sectorPct = confirmed > 0 ? Math.round((dominantNaf.count / confirmed) * 100) : 0;
    } else {
        sectorNum = 0;
        sectorDenom = total;
        sectorPct = 0;
    }

    // Header label for Secteur card
    const sectorCardLabel = hasPicked ? t('job.scoreboardSectorMatch') : t('job.scoreboardSectorDominant');

    // Subtext for Identifiées card (with optional strict/rescued sub-line)
    const rb = summary?.result_breakdown;
    const showStrictSplit = rb && (rb.strict_naf_active || (rb.confirmed_rescued ?? 0) > 0);
    const strictSplitLine = showStrictSplit
        ? `<div class="hero-card-subline-strict">${t('job.heroStrictRescued', { strict: rb.confirmed_strict, rescued: rb.confirmed_rescued })}</div>`
        : '';
    const identifiedSubtext = t('job.scoreboardSubtextIdentified', { confirmed, total }) + strictSplitLine;

    // Subtext for Secteur card
    let sectorSubtext = '';
    if (showSectorCard) {
        if (hasPicked) {
            const pickedCode = pickedNafs[0];
            if (siblingNafs.length === 0) {
                sectorSubtext = t('job.scoreboardSubtextSectorAllMatch');
            } else if (siblingNafs.length === 1) {
                sectorSubtext = t('job.scoreboardSubtextSectorOneSibling', {
                    verified: nafVerifiedClickable,
                    pickedCode,
                    mismatch: nafMismatchClickable,
                    siblingCode: siblingNafs[0].code,
                    siblingLabel: siblingNafs[0].label,
                });
            } else {
                const top3SiblingsStr = siblingNafs.slice(0, 3).map(s => `${s.code} (${s.label})`).join(', ');
                const moreSiblings = Math.max(0, siblingNafs.length - 3);
                const siblingList = moreSiblings > 0
                    ? `${top3SiblingsStr} ${t('job.scoreboardSubtextSiblingsMoreSuffix', { count: moreSiblings })}`
                    : top3SiblingsStr;
                sectorSubtext = t('job.scoreboardSubtextSectorManySiblings', {
                    verified: nafVerifiedClickable,
                    pickedCode,
                    mismatch: nafMismatchClickable,
                    siblingList,
                });
            }
        } else if (dominantNaf) {
            // No picked NAFs — dominant NAF fallback
            const otherNafs = byNaf.filter(r => r.code !== dominantNaf.code && r.count > 0);
            if (otherNafs.length === 0) {
                sectorSubtext = t('job.scoreboardSubtextSectorImpliedAll', {
                    N: dominantNaf.count,
                    code: dominantNaf.code,
                    label: dominantNaf.label,
                });
            } else {
                const top3OthersStr = otherNafs.slice(0, 3).map(s => `${s.code} (${s.label})`).join(', ');
                const moreOthers = Math.max(0, otherNafs.length - 3);
                const siblingList = moreOthers > 0
                    ? `${top3OthersStr} ${t('job.scoreboardSubtextSiblingsMoreSuffix', { count: moreOthers })}`
                    : top3OthersStr;
                sectorSubtext = t('job.scoreboardSubtextSectorImpliedMixed', {
                    N: dominantNaf.count,
                    code: dominantNaf.code,
                    label: dominantNaf.label,
                    siblingList,
                });
            }
        }
    }

    // heroCard: denom and pctVal are optional — pass null to suppress the "/ N" and "N%" display.
    const heroCard = (icon, label, num, denom, pctVal, subtext, fullWidth, scopeLabel) => {
        const colour = denom != null ? rateColour(pctVal) : 'var(--accent)';
        return `
            <div style="${fullWidth ? 'grid-column: 1 / -1;' : ''} background:var(--bg-elevated); border-radius:var(--radius); padding:var(--space-lg); border:1px solid var(--border-subtle)">
                <div style="font-size:var(--font-xs); color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; font-weight:700; margin-bottom:var(--space-sm)">${icon} ${label}</div>
                <div style="display:flex; align-items:baseline; gap:var(--space-sm); margin-bottom:var(--space-sm)">
                    <span style="font-size:var(--font-2xl); font-weight:800; color:${colour}">${num}</span>
                    ${denom != null ? `<span style="color:var(--text-muted); font-size:var(--font-sm)">/ ${denom}</span>` : ''}
                    ${pctVal != null ? `<span style="font-size:var(--font-lg); font-weight:700; color:${colour}; margin-left:auto">${pctVal}%</span>` : ''}
                </div>
                ${scopeLabel ? `<div style="font-size:var(--font-xs); color:var(--text-muted); margin-bottom:var(--space-xs); font-style:italic">${scopeLabel}</div>` : ''}
                <div style="font-size:var(--font-sm); color:var(--text-secondary); line-height:1.4">${subtext}</div>
            </div>
        `;
    };

    // Pending hero card — promotes the previously-buried badge to first-class.
    // Hidden when pending=0. Click toggles the existing 'pending' filter (same
    // semantics as the legend row + the deleted badge).
    // Pending split — show only when picker was set (NULL naf_status batches have no in/out distinction)
    const verifiedPending = linkStats.naf_verified_pending || 0;
    const mismatchPending = linkStats.naf_mismatch_pending || 0;
    const showPendingSplit = pending > 0 && pickedNafs.length > 0;
    const pendingSplitHtml = showPendingSplit ? `
        <div style="font-size:var(--font-xs); color:var(--text-muted); margin-top:var(--space-xs); line-height:1.4">
            <span style="color:var(--success)">${t('job.pendingSplitInPicker', { count: verifiedPending })}</span>
            ·
            <span style="color:#f59e0b">${t('job.pendingSplitOutOfPicker', { count: mismatchPending })}</span>
        </div>
    ` : '';
    const pendingHeroCardHtml = pending > 0 ? `
        <div class="pending-hero-card" data-filter="pending"
             style="cursor:pointer; background:var(--bg-elevated); border-radius:var(--radius);
                    padding:var(--space-lg); border:1px solid rgba(245,158,11,0.4)">
            <div style="font-size:var(--font-xs); color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; font-weight:700; margin-bottom:var(--space-sm)">⏳ ${t('job.scoreboardPendingApproval')}</div>
            <div style="display:flex; align-items:baseline; gap:var(--space-sm); margin-bottom:var(--space-sm)">
                <span style="font-size:var(--font-2xl); font-weight:800; color:#f59e0b">${pending}</span>
                <span style="color:var(--text-muted); font-size:var(--font-sm)">/ ${total}</span>
            </div>
            <div style="font-size:var(--font-sm); color:var(--text-secondary); line-height:1.4">${t('job.scoreboardSubtextPending', { count: pending })}</div>
            ${pendingSplitHtml}
        </div>
    ` : '';

    // Grid columns: count visible cards (Identifiées always, Pending if pending>0, Sector if showSectorCard).
    const _showPendingHero = pending > 0;
    const _heroCardCount = 1 + (_showPendingHero ? 1 : 0) + (showSectorCard ? 1 : 0);
    const _gridCols = _heroCardCount === 1 ? '1fr' : (_heroCardCount === 2 ? '1fr 1fr' : '1fr 1fr 1fr');
    const _identifiedFullWidth = _heroCardCount === 1;

    // Change 1 (A1=c): scope label below confirmed count; drop tautological denom/pct.
    const scopeLabel = computeScopeLabel(job);

    // Cross-dept hint REMOVED 2026-05-10 per Alan's Option A decision: the Job page shows
    // only entities matching the batch's stored dept filter — no clutter about cross-dept
    // entities. Users who want cross-dept exploration navigate via Dashboard.
    // Backend dept_override (jobs.py + api.js _deptOverrideFromHash) is left in place as a
    // dormant escape hatch — bookmarked URLs with ?override_dept=ALL still work.
    const crossDeptHtml = '';

    const heroGridHtml = `
        <div style="display:grid; grid-template-columns:${_gridCols}; gap:var(--space-xl); margin-bottom:var(--space-lg)" class="scoreboard-hero-grid">
            ${heroCard('🎯', t('job.scoreboardIdentified'), confirmed, null, null, identifiedSubtext + crossDeptHtml, _identifiedFullWidth, scopeLabel)}
            ${pendingHeroCardHtml}
            ${showSectorCard ? heroCard('🏷️', sectorCardLabel, sectorNum, sectorDenom, sectorPct, sectorSubtext, false) : ''}
        </div>
    `;

    // ── D: Action chip row ─────────────────────────────────────────
    const depts = job.departments || [];
    let deptChip = '';
    if (depts.length === 1) {
        const deptCode = depts[0].departement;
        const deptName = DEPT_NAMES[deptCode] || deptCode;
        deptChip = `<span style="color:var(--text-secondary); font-size:var(--font-sm)">📍 ${t('job.chipDepartment', { code: deptCode, name: deptName })}</span>`;
    } else if (depts.length > 1) {
        deptChip = `<span style="color:var(--text-secondary); font-size:var(--font-sm)">📍 ${t('job.chipDepartments', { count: depts.length })}</span>`;
    }

    // Change 4 (C1=b): use matching_search_count (Brief 06 field); backward-compat fallback to companies_qualified.
    const matchingSearchCount = (summary && summary.matching_search_count) || qualified;
    const qualifiedSubtitle = isStrict ? t('job.searchCriteriaSubtitleStrict') : t('job.searchCriteriaSubtitleWide');
    const contactableChip = `
        <div style="display:flex; flex-direction:column; gap:2px">
            <span style="font-size:var(--font-sm); font-weight:600">🎯 ${t('job.qualifiedChipMain', { count: matchingSearchCount })}</span>
            <span style="font-size:var(--font-xs); color:var(--text-muted)">${qualifiedSubtitle}</span>
        </div>
    `;

    const sh = summary?.system_health;
    const anomaliesChip = (sh && sh.total_errors > 0) ? `
        <button class="feedback-cta-chip" data-test-id="feedback-cta-chip" type="button"
            data-batch-id="${escapeHtml(job.batch_id || '')}"
            data-batch-name="${escapeHtml(job.batch_name || '')}">
            📨 ${t('job.feedbackCtaButton')}
        </button>
    ` : '';

    const chipsHtml = `
        <div style="display:flex; align-items:center; gap:var(--space-2xl); flex-wrap:wrap; padding:var(--space-md) 0; border-top:1px solid var(--border-subtle); border-bottom:1px solid var(--border-subtle); margin-bottom:var(--space-lg)">
            ${contactableChip}
            ${deptChip}
            ${anomaliesChip}
        </div>
    `;

    // ── E: Disclosure "▸ Voir le détail par méthode" — admin-only (Change 6) ──
    const _currentUser = getCachedUser();
    const isAdmin = _currentUser?.role === 'admin';

    const methodEntries = Object.entries(byMethod)
        .filter(([, n]) => n > 0)
        .sort((a, b) => b[1] - a[1]);
    const nafExact = linkStats.naf_exact || 0;
    const nafRelated = linkStats.naf_related || 0;
    const nafMismatchConfirmed = linkStats.naf_mismatch_confirmed || 0;
    const nafMismatchPending = linkStats.naf_mismatch_pending || 0;
    const showNaf = nafEvaluated > 0;
    const nafNotEvaluated = showNaf ? Math.max(0, total - nafVerified - nafMismatch) : 0;
    const hasDetail = methodEntries.length > 0 || showNaf;

    const methodRows = methodEntries.map(([method, n]) => `
        <div style="display:flex; justify-content:space-between; padding:3px 0; font-size:var(--font-sm)">
            <span style="color:var(--text-secondary)">${escapeHtml(methodLabel(method))}</span>
            <span style="font-weight:700; color:var(--text-primary); min-width:28px; text-align:right">${n}</span>
        </div>
    `).join('');

    // Change 6 (B4): only render the method-disclosure for admins.
    const disclosureHtml = (isAdmin && hasDetail) ? `
        <div style="display:flex; justify-content:flex-end; margin-top:var(--space-sm)">
            <button id="link-stats-toggle" style="background:none; border:1px solid var(--border-subtle); color:var(--accent); cursor:pointer; font-size:var(--font-xs); font-weight:600; padding:4px 10px; border-radius:var(--radius-sm); display:flex; align-items:center; gap:4px; white-space:nowrap" onmouseover="this.style.background='var(--bg-secondary)'" onmouseout="this.style.background='transparent'">
                <span id="link-stats-chevron" style="display:inline-block; transition:transform 0.2s">▸</span>
                ${t('job.disclosureMethod')}
            </button>
        </div>
        <div id="link-stats-detail" style="display:none; margin-top:var(--space-md); padding-top:var(--space-md); border-top:1px solid var(--border-subtle)">
            ${showNaf ? `
                <div style="font-size:var(--font-xs); color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-sm); font-weight:700">
                    ${t('job.linkStatsNafBreakdown')}
                </div>
                <div style="display:flex; gap:var(--space-xl); flex-wrap:wrap; font-size:var(--font-sm); margin-bottom:var(--space-md)">
                    ${nafExact > 0 ? `<span title="${escapeHtml(t('job.linkStatsNafExactTooltip'))}"><span style="color:var(--success)">✓✓</span> <strong>${nafExact}</strong> ${t('job.linkStatsNafExact')}</span>` : ''}
                    ${nafRelated > 0 ? `<span title="${escapeHtml(t('job.linkStatsNafRelatedTooltip'))}"><span style="color:var(--success)">~</span> <strong>${nafRelated}</strong> ${t('job.linkStatsNafRelated')}</span>` : ''}
                    <span title="${escapeHtml(t('job.linkStatsNafMismatchTooltip'))}">
                        ${/* Disclosure subline keeps gross naf_mismatch_confirmed + naf_mismatch_pending —
                           provides breakdown context for the smaller clickable parent count above. */''}
                        <span style="color:#ef4444">✗</span> <strong>${nafMismatch}</strong> ${t('job.linkStatsNafMismatch')}
                        ${nafMismatch > 0 ? `<span style="color:var(--text-muted); font-weight:400; margin-left:4px">${t('job.linkStatsNafMismatchSubline', { confirmed: nafMismatchConfirmed, pending: nafMismatchPending })}</span>` : ''}
                    </span>
                    <span><span style="color:var(--text-muted)">—</span> <strong>${nafNotEvaluated}</strong> ${t('job.linkStatsNafNotEvaluated')}</span>
                </div>
            ` : ''}
            ${methodEntries.length > 0 ? `
                <div style="font-size:var(--font-xs); color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-sm); font-weight:700; margin-top:var(--space-md)">
                    <span title="${escapeHtml(t('job.linkStatsByMethodTooltip'))}" style="border-bottom:1px dotted var(--text-muted); cursor:help">
                        ${t('job.linkStatsByMethod')} ⓘ
                    </span>
                </div>
                <div style="display:grid; grid-template-columns:repeat(auto-fill, minmax(200px, 1fr)); gap:0 var(--space-xl)">
                    ${methodRows}
                </div>
            ` : ''}
        </div>
    ` : '';

    return `
        <style>
            .legend-row:hover { background: var(--bg-secondary); }
            .legend-row.active { background: var(--bg-secondary); outline: 1px solid var(--accent); }
            .pending-hero-card:hover { background: var(--bg-secondary); }
            @media (max-width: 768px) {
                .scoreboard-hero-grid { grid-template-columns: 1fr !important; }
            }
        </style>
        <div class="card" id="scoreboard-card" style="margin-bottom:var(--space-xl)">
            ${headerHtml}
            ${barHtml}
            ${legendHtml}
            ${heroGridHtml}
            ${chipsHtml}
            ${disclosureHtml}
        </div>
        ${(() => {
            try {
                const fj = job.filters_json;
                const parsed = typeof fj === 'string' ? JSON.parse(fj) : fj;
                const bulkMeta = parsed && parsed.bulk_discovery;
                return bulkMeta ? renderBulkDiscoveryPanel(bulkMeta, job) : '';
            } catch (_e) { return ''; }
        })()}
    `;
}

function buildBatchReport(summary, job) {
    if (!summary) return '';
    const rb = summary.result_breakdown || {};
    const sh = summary.system_health || { total_errors: 0, categories: {} };
    const lines = [
        `=== ${t('job.rapportButton')} — ${job.batch_name || job.batch_id} ===`,
        `Date: ${new Date().toISOString()}`,
        ``,
        `${t('job.systemHealth.resultsTitle')}:`,
        `  ${t('job.systemHealth.discovered')}: ${rb.discovered ?? '-'}`,
        `  ${t('job.systemHealth.confirmed')}: ${rb.confirmed ?? '-'} (${t('job.heroStrictRescued', { strict: rb.confirmed_strict ?? '-', rescued: rb.confirmed_rescued ?? '-' })})`,
        `  ${t('job.systemHealth.pendingReview')}: ${rb.pending_review ?? '-'}`,
        `  ${t('job.systemHealth.unmatchedMaps')}: ${rb.unmatched_maps ?? '-'}`,
        `  ${t('job.systemHealth.strictNafMode')}: ${rb.strict_naf_active ? t('common.yes') : t('common.no')}`,
        ``,
        sh.total_errors === 0
            ? t('job.cleanReport')
            : `${t('job.popoverTitle')} (${sh.total_errors}):`,
    ];
    if (sh.total_errors > 0) {
        Object.entries(sh.categories).forEach(([key, data]) => {
            const samples = data.samples?.length ? ` (${t('job.systemHealth.sample')}: ${data.samples.join(', ')})` : '';
            lines.push(`  ${t('job.systemHealth.cat.' + key)}: ${data.count}${samples}`);
        });
    }
    return lines.join('\n');
}

// Brief 08 — Feedback modal for batch quality reporting
function openFeedbackModal(batchId, batchName, summary) {
    const existing = document.getElementById('feedback-modal-overlay');
    if (existing) existing.remove();

    const sh = summary?.system_health || { total_errors: 0, categories: {} };
    const anomalyTotal = sh.total_errors || 0;
    const anomalyCounts = sh.categories || {};
    const sampleSirens = summary?.sample_anomaly_sirens || [];

    // Build anomaly detail lines for context preview
    const catLines = Object.entries(anomalyCounts).map(([key, data]) =>
        `${t('job.systemHealth.cat.' + key)}: ${data.count}`
    ).join(', ');
    const contextPreview = catLines
        ? `${t('job.feedbackModalAnomalyCount', { count: anomalyTotal })} (${catLines})`
        : t('job.feedbackModalAnomalyCount', { count: anomalyTotal });
    const sirenPreview = sampleSirens.length
        ? t('job.feedbackModalSampleSirens', { sirens: sampleSirens.join(', ') })
        : '';

    const overlay = document.createElement('div');
    overlay.id = 'feedback-modal-overlay';
    overlay.className = 'modal-overlay';
    overlay.innerHTML = `
        <div class="modal-content" style="max-width:540px">
            <h3 class="modal-title">${t('job.feedbackModalTitle', { batchName: escapeHtml(batchName) })}</h3>
            <div class="modal-body">
                <p style="margin-bottom:var(--space-sm)">${t('job.feedbackModalSubtitle')}</p>
                <div style="background:var(--bg-secondary); border:1px solid var(--border-subtle); border-radius:var(--radius); padding:var(--space-sm) var(--space-md); font-size:var(--font-xs); color:var(--text-muted); margin-bottom:var(--space-md)">
                    <div style="font-weight:600; margin-bottom:4px">${t('job.feedbackModalContextLabel')}</div>
                    <div>${escapeHtml(contextPreview)}</div>
                    ${sirenPreview ? `<div style="margin-top:2px">${escapeHtml(sirenPreview)}</div>` : ''}
                </div>
                <textarea id="feedback-modal-textarea"
                    placeholder="${t('job.feedbackModalTextareaPlaceholder')}"
                    style="width:100%; min-height:120px; box-sizing:border-box; padding:var(--space-sm); border:1px solid var(--border-default); border-radius:var(--radius); background:var(--bg-input, var(--bg-elevated)); color:var(--text-primary); font-size:var(--font-sm); resize:vertical; font-family:inherit"
                ></textarea>
            </div>
            <div class="modal-actions">
                <button id="feedback-modal-cancel" class="btn btn-secondary">${t('job.feedbackModalCancel')}</button>
                <button id="feedback-modal-submit" class="btn btn-primary">${t('job.feedbackModalSubmit')}</button>
            </div>
        </div>
    `;
    document.body.appendChild(overlay);
    requestAnimationFrame(() => overlay.classList.add('visible'));

    const close = () => {
        overlay.classList.remove('visible');
        setTimeout(() => overlay.remove(), 200);
    };

    overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });

    document.getElementById('feedback-modal-cancel').addEventListener('click', close);

    document.getElementById('feedback-modal-submit').addEventListener('click', async () => {
        const textarea = document.getElementById('feedback-modal-textarea');
        const userMessage = (textarea?.value || '').trim();
        if (!userMessage) return;

        const submitBtn = document.getElementById('feedback-modal-submit');
        if (submitBtn) { submitBtn.disabled = true; submitBtn.textContent = '…'; }

        const contextJson = JSON.stringify({
            page_url: window.location.hash,
            batch_id: batchId,
            batch_name: batchName,
            anomaly_total: anomalyTotal,
            anomaly_counts: anomalyCounts,
            sample_sirens: sampleSirens,
            timestamp: new Date().toISOString(),
        });

        try {
            const formData = new FormData();
            formData.append('description', userMessage);
            formData.append('context', contextJson);
            formData.append('feedback_type', 'batch_quality');

            const resp = await fetch('/api/bug-report', { method: 'POST', body: formData, credentials: 'include' });
            close();
            if (resp.ok) {
                showToast(t('job.feedbackToastSuccess'), 'success');
            } else {
                showToast(t('job.feedbackToastError'), 'error');
            }
        } catch (_err) {
            close();
            showToast(t('job.feedbackToastError'), 'error');
        }
    });

    requestAnimationFrame(() => {
        document.getElementById('feedback-modal-textarea')?.focus();
    });
}

export async function renderJob(container, batchId) {
    batchId = decodeURIComponent(batchId);
    // E4.A — strip ?q=... from batchId. Router pattern #/job/(.+) captures
    // the query string into batchId; API calls would 404 without this.
    const qIdx = batchId.indexOf('?');
    if (qIdx >= 0) batchId = batchId.slice(0, qIdx);

    const [job, quality, summary] = await Promise.all([
        getJob(batchId),
        getJobQuality(batchId),
        getJobSummary(batchId),
    ]);

    const queriesResp = await fetch(`/api/jobs/${encodeURIComponent(batchId)}/queries`, { credentials: 'include' });
    const queriesData = queriesResp.ok ? await queriesResp.json() : { queries: [], time_cap_min: null };

    if (!job || job._ok === false || job.error) {
        const isServerError = job && job._ok === false && job._status >= 500;
        container.innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon">${isServerError ? '⚠️' : '❌'}</div>
                <div class="empty-state-text">${isServerError ? t('job.serverUnavailable') : t('job.jobNotFound')}</div>
                <p style="color:var(--text-muted)">${isServerError ? t('job.retryInMoment') : ''}</p>
                <a href="#/" class="btn btn-primary">${t('job.backToDashboard')}</a>
            </div>
        `;
        return;
    }

    const batchSize = job.batch_size || job.total_companies || 1;
    const scraped = job.companies_scraped || 0;
    const qualified = job.companies_qualified || 0;
    const entityCount = job.link_stats?.total || scraped || qualified || batchSize;
    const q = quality || {};

    // Change 7 (D4=a): admin sees Avancé expanded; non-admin sees it collapsed.
    const _pageUser = getCachedUser();
    const _pageIsAdmin = _pageUser?.role === 'admin';

    // Retrieve stored Avancé disclosure state (persist across page reloads for this batch)
    const _advancedStorageKey = `fortress_advanced_open_${batchId}`;
    const _advancedStoredOpen = localStorage.getItem(_advancedStorageKey);
    // If stored, use stored value; else default: open for admin, closed for non-admin.
    const _advancedOpen = _advancedStoredOpen !== null
        ? _advancedStoredOpen === 'true'
        : _pageIsAdmin;

    container.innerHTML = `
        ${breadcrumb([
            { label: 'Dashboard', href: '#/' },
            { label: job.batch_name },
        ])}

        ${buildScoreboardCard(job, job.link_stats, summary)}

        <!-- Quality Gauges -->
        <div class="card" style="margin-bottom:var(--space-xl)">
            <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                ${t('job.qualityTitle')}
            </h3>
            <div class="quality-gauges-grid" style="display:grid; grid-template-columns:repeat(6, 1fr); gap:var(--space-md); align-items:start; justify-items:center">
                ${renderGauge(q.phone_pct || 0, t('job.gaugePhone'))}
                ${renderGauge(q.email_pct || 0, t('job.gaugeEmail'))}
                ${renderGauge(q.website_pct || 0, t('job.gaugeWeb'))}
                ${renderGauge(q.officers_pct || 0, t('job.gaugeOfficers'))}
                ${renderGauge(q.financials_pct || 0, t('job.gaugeFinancials'))}
                ${renderGauge(q.siret_pct || q.social_pct || 0, t('job.gaugeSocial'))}
            </div>
            <div style="text-align:center; font-size:var(--font-sm); color:var(--text-muted); margin-top:var(--space-lg)">
                ${t('job.companiesCount', { count: entityCount, plural: entityCount > 1 ? 's' : '' })}
            </div>
        </div>

        <!-- Companies -->
        <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:var(--space-lg)">
            <h2 style="font-size:var(--font-lg); font-weight:600">${t('job.companiesLabel')}</h2>
            <div style="display:flex; gap:var(--space-sm)">
                <button id="btn-select-mode" class="btn-select-mode" title="${t('job.selectMode')}">
                    ${t('job.selectMode')}
                </button>
                <select id="job-sort" style="background:var(--bg-input); border:1px solid var(--border-default); border-radius:var(--radius-sm); padding:var(--space-sm) var(--space-md); color:var(--text-primary); font-family:var(--font-family); font-size:var(--font-sm)">
                    <option value="completude">${t('job.sortCompleteness')}</option>
                    <option value="name">${t('job.sortName')}</option>
                    <option value="date">${t('job.sortDate')}</option>
                </select>
            </div>
        </div>
        <div id="job-companies-container">
            <div class="loading"><div class="spinner"></div></div>
        </div>

        <!-- Departments covered — always shown when at least 1 dept present -->
        ${job.departments && job.departments.length > 0 ? `
            <div class="card" style="margin-bottom:var(--space-xl); margin-top:var(--space-xl)">
                <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                    ${t('job.deptsCovered')}
                </h3>
                <div style="display:flex; gap:var(--space-sm); flex-wrap:wrap">
                    ${job.departments.map(d => `
                        <a href="#/department/${d.departement}" class="badge badge-accent" style="cursor:pointer; text-decoration:none">
                            ${d.departement} (${d.company_count})
                        </a>
                    `).join('')}
                </div>
            </div>
        ` : ''}

        <!-- Change 7 (D4=a): Engineering surfaces in collapsible "Avancé" disclosure -->
        <details class="advanced-section" id="advanced-section" ${_advancedOpen ? 'open' : ''} style="margin-top:var(--space-xl)">
            <summary class="advanced-section-summary">${t('job.advancedSection')}</summary>
            <div class="advanced-section-body">
                <!-- Recherches effectuées — foldable card; collapsed by default per Alan
                     (don't need to see it every time you visit). 2026-05-10 hotfix. -->
                <details class="card" id="queries-card" style="margin-bottom:var(--space-xl)">
                    <summary style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg); cursor:pointer; list-style:revert">
                        ${t('job.queriesCardTitle')}
                    </summary>
                    <div id="queries-card-list" style="margin-top:var(--space-md)"><span style="color:var(--text-muted)">${t('common.loading')}</span></div>
                </details>

                ${buildTimingPanel(job)}
            </div>
        </details>

    `;

    // Populate queries card — always visible; replace placeholder with data or empty message
    const qPanel = document.getElementById('queries-card-list');
    if (qPanel) {
        if (queriesData.queries && queriesData.queries.length > 0) {
            qPanel.innerHTML = renderQueriesPanel(
                queriesData.queries,
                { collapsible: true, capMin: queriesData.time_cap_min }
            );
        } else {
            qPanel.innerHTML = `<span style="color:var(--text-muted)">${t('monitor.queriesEmpty')}</span>`;
        }

        // E4.A — wire drill-down click handlers (once per renderJob call, qPanel is recreated)
        bindQueriesPanelClicks(qPanel);
        qPanel.addEventListener('qp:filter', async (ev) => {
            const sq = ev.detail.searchQuery;
            // Toggle off if same query clicked again
            _currentSearchQuery = (_currentSearchQuery === sq) ? '' : sq;
            _currentPage = 1;
            await loadCompanies(_currentBatchId, 1, _currentSort, _currentFilter, _currentSearchQuery);
            document.getElementById('job-companies-container')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
        });
    }

    // Reset selection + filter state for this job
    selectionMode = false;
    selectedSirens.clear();
    _currentBatchId = batchId;
    _currentBatchName = job.batch_name;
    _currentPage = 1;
    _currentSort = 'completude';
    _currentFilter = '';  // Reset filter when opening a new job
    _currentSearchQuery = _readQueryParamFromHash();  // E4.A — auto-apply ?q= from URL

    // Load companies
    await loadCompanies(batchId, 1, 'completude', '', _currentSearchQuery);

    // Change 7: persist Avancé disclosure state in localStorage
    const advancedSection = document.getElementById('advanced-section');
    if (advancedSection) {
        advancedSection.addEventListener('toggle', () => {
            localStorage.setItem(_advancedStorageKey, advancedSection.open ? 'true' : 'false');
        });
    }

    // Link stats disclosure toggle (Request A)
    const linkStatsBtn = document.getElementById('link-stats-toggle');
    if (linkStatsBtn) {
        linkStatsBtn.addEventListener('click', () => {
            const detail = document.getElementById('link-stats-detail');
            const chevron = document.getElementById('link-stats-chevron');
            if (!detail) return;
            const visible = detail.style.display !== 'none';
            detail.style.display = visible ? 'none' : 'block';
            if (chevron) chevron.style.transform = visible ? '' : 'rotate(90deg)';
        });
    }

    // Rapport button — copies batch report text to clipboard
    const rapportBtn = document.getElementById('btn-rapport');
    if (rapportBtn) {
        rapportBtn.addEventListener('click', async () => {
            const text = buildBatchReport(summary, job);
            try {
                await navigator.clipboard.writeText(text);
                rapportBtn.textContent = `✅ ${t('job.systemHealth.copied')}`;
                setTimeout(() => { rapportBtn.innerHTML = `📋 ${t('job.rapportButton')}`; }, 2000);
            } catch (err) {
                console.error('clipboard write failed', err);
            }
        });
    }

    // Feedback CTA chip — opens feedback modal (Brief 08)
    const feedbackCtaChip = document.querySelector('[data-test-id="feedback-cta-chip"]');
    if (feedbackCtaChip) {
        feedbackCtaChip.addEventListener('click', () => {
            openFeedbackModal(batchId, job.batch_name || batchId, summary);
        });
    }

    // Sort change handler
    document.getElementById('job-sort')?.addEventListener('change', (e) => {
        loadCompanies(batchId, 1, e.target.value, _currentFilter, _currentSearchQuery);
    });

    // Legend row click handlers — filter company list by state
    document.querySelectorAll('.legend-row').forEach(row => {
        row.addEventListener('click', async () => {
            const filter = row.dataset.filter || '';
            _currentFilter = (_currentFilter === filter) ? '' : filter;  // toggle off if same
            document.querySelectorAll('.legend-row').forEach(r => r.classList.remove('active'));
            if (_currentFilter) row.classList.add('active');
            _currentPage = 1;
            await loadCompanies(_currentBatchId, 1, _currentSort, _currentFilter, _currentSearchQuery);
            document.getElementById('job-companies-container')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
        });
    });

    // Pending hero card click — same toggle semantics as the legend row 'pending' filter.
    const pendingHeroCard = document.querySelector('.pending-hero-card');
    if (pendingHeroCard) {
        pendingHeroCard.addEventListener('click', async () => {
            _currentFilter = (_currentFilter === 'pending') ? '' : 'pending';
            // Sync legend rows: only the pending row is active (or none if toggled off)
            document.querySelectorAll('.legend-row').forEach(r => {
                r.classList.toggle('active', _currentFilter === 'pending' && r.dataset.filter === 'pending');
            });
            _currentPage = 1;
            await loadCompanies(_currentBatchId, 1, _currentSort, _currentFilter, _currentSearchQuery);
            document.getElementById('job-companies-container')?.scrollIntoView({ behavior: 'smooth', block: 'start' });
        });
    }

    // Selection mode toggle
    _setupSelectionMode(batchId);

    // Delete button
    const deleteBtn = document.getElementById('btn-delete-job');
    if (deleteBtn) {
        deleteBtn.addEventListener('click', () => {
            const currentUser = getCachedUser();
            const isCrossWorkspace = (
                currentUser?.role === 'admin'
                && job.workspace_id != null
                && job.workspace_id !== currentUser.workspace_id
            );
            showConfirmModal({
                title: isCrossWorkspace ? t('monitor.crossWorkspaceDelete.title') : t('job.confirmDelete'),
                body: isCrossWorkspace
                    ? `<p>${t('monitor.crossWorkspaceDelete.prompt', { name: escapeHtml(job.batch_name) })}</p>`
                    : `
                    <p><strong>Batch :</strong> ${escapeHtml(job.batch_name)}</p>
                    <p><strong>${t('job.createdOn')} :</strong> ${formatDateTime(job.created_at)}</p>
                    <p><strong>${entityCount}</strong> entreprises collectées</p>
                    <p style="color:var(--danger)">⚠️ <strong>${t('job.confirmDeleteWithInfo')}</strong></p>
                    <p style="color:var(--text-muted)">${t('job.confirmDeleteKeep')}</p>
                `,
                confirmLabel: isCrossWorkspace ? t('monitor.crossWorkspaceDelete.submitLabel') : t('job.deleteConfirmPermanent'),
                danger: true,
                requiredText: isCrossWorkspace ? job.batch_name : null,
                onConfirm: async () => {
                    const result = isCrossWorkspace
                        ? await deleteJob(batchId, { confirmName: job.batch_name })
                        : await deleteJob(batchId);
                    if (result._ok !== false) {
                        showToast(t('job.deleteSuccess', { contacts: result.deleted_contacts || 0, sirens: result.sirens_affected || 0 }), 'success');
                        window.location.hash = '#/';
                    } else {
                        const msg = result._status === 422
                            ? (result.error || 'Confirmation requise.')
                            : (result.error || t('job.deleteError'));
                        showToast(msg, 'error');
                    }
                },
            });
        });
    }

    // Download dropdown (CSV / XLSX / JSONL)
    const downloadBtn = document.getElementById('btn-download-dropdown');
    if (downloadBtn) {
        downloadBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            let dd = document.getElementById('download-dropdown');
            if (dd) { dd.remove(); return; }
            dd = document.createElement('div');
            dd.id = 'download-dropdown';
            dd.style.cssText = 'position:absolute; top:100%; left:0; margin-top:var(--space-xs); background:var(--bg-elevated); border:1px solid var(--border-default); border-radius:var(--radius); box-shadow:0 8px 24px rgba(0,0,0,0.3); z-index:50; min-width:160px; overflow:hidden;';
            dd.innerHTML = `
                <a href="#" id="btn-download-csv" download style="display:block; padding:var(--space-sm) var(--space-lg); color:var(--text-primary); text-decoration:none; transition:background 0.15s" onmouseover="this.style.background='var(--bg-hover)'" onmouseout="this.style.background=''">📥 CSV</a>
                <a href="${getExportUrl(batchId, 'xlsx')}" download style="display:block; padding:var(--space-sm) var(--space-lg); color:var(--text-primary); text-decoration:none; transition:background 0.15s" onmouseover="this.style.background='var(--bg-hover)'" onmouseout="this.style.background=''">📗 XLSX</a>
                <a href="${getExportUrl(batchId, 'jsonl')}" download style="display:block; padding:var(--space-sm) var(--space-lg); color:var(--text-primary); text-decoration:none; transition:background 0.15s" onmouseover="this.style.background='var(--bg-hover)'" onmouseout="this.style.background=''">📄 JSONL</a>
            `;
            downloadBtn.parentElement.appendChild(dd);
            // E4.A — CSV click handler picks up current _currentSearchQuery at click time
            document.getElementById('btn-download-csv')?.addEventListener('click', (e) => {
                e.preventDefault();
                const url = getExportUrl(batchId, 'csv', _currentSearchQuery || null);
                window.location.href = url;
            });
            const close = (e2) => { if (!dd.contains(e2.target) && e2.target !== downloadBtn) { dd.remove(); document.removeEventListener('click', close); } };
            setTimeout(() => document.addEventListener('click', close), 0);
        });
    }

    // Rerun button
    const rerunBtn = document.getElementById('btn-rerun');
    if (rerunBtn) {
        rerunBtn.addEventListener('click', () => {
            // Pre-fill new batch form using search_queries (current format)
            const queries = job.search_queries || [];
            const parsedQueries = typeof queries === 'string' ? JSON.parse(queries) : queries;
            // Parse filters_json (the source-of-truth for original launch state)
            const filters = (() => {
                const f = job.filters_json;
                if (!f) return {};
                try {
                    return typeof f === 'string' ? JSON.parse(f) : f;
                } catch { return {}; }
            })();
            if (parsedQueries.length > 0) {
                sessionStorage.setItem('fortress_expansion_prefill', JSON.stringify({
                    queries: parsedQueries,
                    size: job.batch_size || 20,
                    // NEW — preserve all launch-time filter state
                    department: filters.department || null,
                    naf_codes: filters.naf_codes || (filters.naf_code ? [filters.naf_code] : null),
                    time_cap_per_query_min: job.time_cap_per_query_min ?? null,
                    time_cap_total_min: job.time_cap_total_min ?? null,
                    entity_cap_confirmed: job.entity_cap_confirmed ?? null,
                    strict_naf: Boolean(job.strict_naf),
                }));
                window.location.hash = '#/new-batch';
            } else {
                // Fallback to old filters_json format
                const params = new URLSearchParams();
                if (job.filters_json) {
                    const f = typeof job.filters_json === 'string' ? JSON.parse(job.filters_json) : job.filters_json;
                    if (f.sector) params.set('sector', f.sector);
                    if (f.department) params.set('department', f.department);
                    if (f.size) params.set('size', f.size);
                }
                window.location.hash = `#/new-batch?${params.toString()}`;
            }
        });
    }

    // Resume button (interrupted batches only)
    const resumeBtn = document.getElementById('btn-resume');
    if (resumeBtn) {
        resumeBtn.addEventListener('click', async () => {
            const currentUser = getCachedUser();
            const isCrossWorkspace = (
                currentUser?.role === 'admin'
                && job.workspace_id != null
                && job.workspace_id !== currentUser.workspace_id
            );

            const performResume = async () => {
                resumeBtn.disabled = true;
                const originalLabel = resumeBtn.textContent;
                resumeBtn.textContent = t('job.resumeLaunching');
                const result = isCrossWorkspace
                    ? await resumeJob(batchId, { confirmName: job.batch_name })
                    : await resumeJob(batchId);
                if (result && result._ok !== false) {
                    showToast(t('job.resumeSuccess'), 'success');
                    window.location.hash = `#/monitor/${encodeURIComponent(batchId)}`;
                } else {
                    resumeBtn.disabled = false;
                    resumeBtn.textContent = originalLabel;
                    const status = result?._status;
                    const msg = status === 422
                        ? (result.error || 'Confirmation requise.')
                        : ((result && (result.error || result.detail)) || t('job.resumeError'));
                    showToast(msg, 'error');
                }
            };

            if (isCrossWorkspace) {
                showConfirmModal({
                    title: t('monitor.crossWorkspaceResume.title'),
                    body: `<p>${t('monitor.crossWorkspaceResume.prompt', { name: escapeHtml(job.batch_name) })}</p>`,
                    confirmLabel: t('monitor.crossWorkspaceResume.submitLabel'),
                    danger: true,
                    requiredText: job.batch_name,
                    onConfirm: performResume,
                });
            } else {
                await performResume();
            }
        });
    }

}

async function loadCompanies(batchId, page, sort, filter = '', searchQuery = '') {
    _currentPage = page;
    _currentSort = sort;
    _currentFilter = filter;
    _currentSearchQuery = searchQuery;
    const companiesContainer = document.getElementById('job-companies-container');
    companiesContainer.innerHTML = '<div class="loading"><div class="spinner"></div></div>';

    const data = await getJobCompanies(batchId, { page, pageSize: 20, sort, filter, searchQuery });

    // E4.A — filter chip rendered in BOTH empty-state and populated paths so
    // the user can always see + clear an active filter even when 0 results.
    const chipHtml = _currentSearchQuery ? `
        <div id="qp-filter-chip" style="display:inline-flex; align-items:center; gap:6px; padding:4px 10px; margin-bottom:var(--space-sm); border-radius:var(--radius-md); background:var(--bg-elevated); border:1px solid var(--border-subtle); font-size:var(--font-sm); color:var(--text-secondary)">
            <span>${t('job.queriesFilterChip', { query: escapeHtml(_currentSearchQuery) })}</span>
            <button id="qp-filter-clear" style="background:none; border:none; color:var(--text-muted); cursor:pointer; padding:0 4px; font-size:var(--font-md)" aria-label="${t('job.queriesFilterClear')}">×</button>
        </div>
    ` : '';
    const attachChipClearHandler = () => {
        const clearBtn = document.getElementById('qp-filter-clear');
        if (clearBtn) {
            clearBtn.addEventListener('click', async () => {
                _currentSearchQuery = '';
                await loadCompanies(_currentBatchId, 1, _currentSort, _currentFilter, '');
            });
        }
    };

    if (!data || !data.companies || data.companies.length === 0) {
        // Context-aware empty state
        const job = await getJob(batchId).catch(() => null);
        const greenCount = job?.triage_green || 0;
        const batchName = job?.batch_name || '';

        if (greenCount > 0) {
            // All-green: all companies were already Maps-enriched
            companiesContainer.innerHTML = `
                ${chipHtml}
                <div style="padding:var(--space-2xl); background:var(--bg-secondary); border-radius:var(--radius-md); border:1px solid rgba(34,197,94,0.3); text-align:center; max-width:560px; margin:0 auto">
                    <div style="font-size:2.5rem; margin-bottom:var(--space-lg)">✅</div>
                    <div style="font-size:var(--font-lg); font-weight:600; color:rgb(34,197,94); margin-bottom:var(--space-md)">
                        ${t('job.allEnriched')}
                    </div>
                    <p style="color:var(--text-secondary); margin-bottom:var(--space-lg)">
                        ${t('job.allEnrichedBody', { count: greenCount, name: escapeHtml(batchName) })}
                    </p>
                    <p style="color:var(--text-muted); font-size:var(--font-sm); margin-bottom:var(--space-xl)">
                        ${t('job.allEnrichedSub')}
                    </p>
                    <a href="#/new-batch" class="btn btn-primary">${t('job.newSearch')}</a>
                </div>
            `;
        } else if (searchQuery) {
            // Active text search returned nothing — tell user why and offer clear
            companiesContainer.innerHTML = `
                ${chipHtml}
                <div class="empty-state">
                    <div class="empty-state-icon">🔍</div>
                    <div class="empty-state-text">${t('job.emptyStateFilterActive', { query: escapeHtml(searchQuery) })}</div>
                    <button id="clear-filter-btn" class="btn btn-secondary" style="margin-top:var(--space-md)">${t('job.clearFilter')}</button>
                </div>
            `;
        } else if (filter && filter !== 'all') {
            // Legend filter active (pending / naf_confirmed / etc.) but returned nothing
            const filterMsgKey = `job.emptyStateLegendFilter_${filter}`;
            companiesContainer.innerHTML = `
                ${chipHtml}
                <div class="empty-state">
                    <div class="empty-state-icon">🔎</div>
                    <div class="empty-state-text">${t(filterMsgKey) || t('job.noCompaniesFound')}</div>
                    <button id="clear-filter-btn" class="btn btn-secondary" style="margin-top:var(--space-md)">${t('job.clearFilter')}</button>
                </div>
            `;
        } else if ((job?.companies_scraped || 0) > 0) {
            // Companies were scraped but none matched SIRENE
            companiesContainer.innerHTML = `
                ${chipHtml}
                <div class="empty-state">
                    <div class="empty-state-icon">📭</div>
                    <div class="empty-state-text">${t('job.emptyStateAllUnmatched')}</div>
                    <p style="color:var(--text-muted); font-size:var(--font-sm); margin-top:var(--space-sm)">${t('job.emptyStateAllUnmatchedDescription')}</p>
                </div>
            `;
        } else if (job?.status === 'completed') {
            // Batch completed but found nothing at all
            companiesContainer.innerHTML = `
                ${chipHtml}
                <div class="empty-state">
                    <div class="empty-state-icon">📭</div>
                    <div class="empty-state-text">${t('job.emptyStateBatchEmpty')}</div>
                    <p style="color:var(--text-muted); font-size:var(--font-sm); margin-top:var(--space-sm)">${t('job.emptyStateBatchEmptyDescription')}</p>
                </div>
            `;
        } else {
            companiesContainer.innerHTML = `
                ${chipHtml}
                <div class="empty-state">
                    <div class="empty-state-icon">📭</div>
                    <div class="empty-state-text">${t('job.emptyStateGeneric')}</div>
                </div>
            `;
        }
        // Wire "clear filter" button if rendered
        document.getElementById('clear-filter-btn')?.addEventListener('click', async () => {
            _currentFilter = '';
            _currentSearchQuery = '';
            document.querySelectorAll('.legend-row').forEach(r => r.classList.remove('active'));
            await loadCompanies(_currentBatchId, 1, _currentSort, '', '');
        });
        attachChipClearHandler();
        return;
    }

    const totalPages = Math.ceil((data.total || 0) / (data.page_size || 20));
    const totalCompanies = data.total || 0;

    // Companies list header — count only. The pending count is now shown
    // as a hero card in the scoreboard above (see buildScoreboardCard).
    const listHeaderHtml = `
        <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:var(--space-md); padding:var(--space-sm) 0; border-bottom:1px solid var(--border-subtle)">
            <span style="font-size:var(--font-sm); color:var(--text-secondary); font-weight:600">
                ${totalCompanies} ${totalCompanies > 1 ? 'entreprises' : 'entreprise'}
            </span>
        </div>
    `;

    const gridContent = `
        <div class="company-grid">
            ${data.companies.map(c => companyCard(c, {
                removable: !selectionMode,
                selectable: selectionMode,
                checked: selectedSirens.has(c.siren),
            })).join('')}
        </div>
    `;

    companiesContainer.innerHTML = `
        <div id="job-company-grid">
            ${chipHtml}
            ${listHeaderHtml}
            ${gridContent}
        </div>
        ${renderPagination(data.page, totalPages, (p) => loadCompanies(batchId, p, sort, filter, _currentSearchQuery))}
    `;

    // E4.A — attach clear handler for filter chip
    attachChipClearHandler();

    // Restore checkbox state after re-render
    if (selectionMode) {
        document.querySelectorAll('.card-checkbox').forEach(cb => {
            if (selectedSirens.has(cb.dataset.siren)) {
                cb.checked = true;
                cb.closest('.company-card')?.classList.add('card-selected');
            }
        });
        _updateBulkBar();
    }

    // Event delegation for × remove buttons
    const grid = document.getElementById('job-company-grid');
    if (grid) {
        grid.addEventListener('click', (e) => {
            // Remove button handler
            const btn = e.target.closest('.card-remove-btn');
            if (btn) {
                e.stopPropagation();
                const siren = btn.dataset.siren;
                const card = btn.closest('.company-card');
                const name = card?.querySelector('.company-card-name')?.textContent || siren;

                showConfirmModal({
                    title: t('job.removeConfirmTitle'),
                    body: `<p>${t('job.removeConfirmBody', { name: escapeHtml(name) })}</p>`,
                    confirmLabel: t('common.delete'),
                    danger: true,
                    checkboxLabel: t('job.alsoBlacklist'),
                    onConfirm: async (checkboxChecked) => {
                        const result = await untagCompany(siren, _currentBatchName);
                        if (result._ok !== false) {
                            if (checkboxChecked) {
                                try {
                                    await fetch('/api/blacklist', {
                                        method: 'POST',
                                        headers: { 'Content-Type': 'application/json' },
                                        body: JSON.stringify({ siren, reason: 'Supprimé manuellement' }),
                                        credentials: 'same-origin',
                                    });
                                } catch { /* best effort */ }
                            }
                            showToast(t('job.removeSuccess', { name }), 'success');
                            if (card) {
                                card.classList.add('card-fade-out');
                                card.addEventListener('animationend', async () => {
                                    card.remove();
                                    await loadCompanies(_currentBatchId, _currentPage, _currentSort, _currentFilter, _currentSearchQuery);
                                });
                            } else {
                                await loadCompanies(_currentBatchId, _currentPage, _currentSort, _currentFilter, _currentSearchQuery);
                            }
                        } else {
                            showToast(t('job.removeError'), 'error');
                        }
                    },
                });
                return;
            }
        });

        // Checkbox handler for selection mode
        grid.addEventListener('change', (e) => {
            const cb = e.target.closest('.card-checkbox');
            if (!cb) return;
            const siren = cb.dataset.siren;
            const card = cb.closest('.company-card');
            if (cb.checked) {
                selectedSirens.add(siren);
                card?.classList.add('card-selected');
            } else {
                selectedSirens.delete(siren);
                card?.classList.remove('card-selected');
            }
            _updateBulkBar();
        });
    }
}

// ── Selection mode toggle ────────────────────────────────────────
function _setupSelectionMode(batchId) {
    const btn = document.getElementById('btn-select-mode');
    if (!btn) return;
    btn.addEventListener('click', () => {
        selectionMode = !selectionMode;
        btn.classList.toggle('active', selectionMode);
        btn.innerHTML = selectionMode ? t('job.cancelSelect') : t('job.selectMode');
        if (!selectionMode) {
            selectedSirens.clear();
            _removeBulkBar();
        }
        // Re-render cards with/without checkboxes
        loadCompanies(batchId, _currentPage, _currentSort, _currentFilter, _currentSearchQuery);
    });
}

// ── Floating action bar ─────────────────────────────────────────
function _updateBulkBar() {
    let bar = document.getElementById('bulk-action-bar');
    if (selectedSirens.size === 0) {
        _removeBulkBar();
        return;
    }
    if (!bar) {
        bar = document.createElement('div');
        bar.id = 'bulk-action-bar';
        bar.className = 'bulk-action-bar';
        document.body.appendChild(bar);
    }
    const n = selectedSirens.size;
    bar.innerHTML = `
        <span class="bulk-count">${t('job.selected', { count: n, plural: n > 1 ? 's' : '' })}</span>
        <button class="btn btn-secondary" id="bulk-select-all">${t('job.selectAll')}</button>
        <button class="btn btn-primary" id="bulk-enrich-web">${t('job.enrichWeb')}</button>
        <button class="btn btn-danger" id="bulk-delete">${t('job.bulkDelete')}</button>
    `;

    // Select all on current page
    document.getElementById('bulk-select-all').onclick = () => {
        const grid = document.getElementById('job-company-grid');
        if (!grid) return;
        const boxes = grid.querySelectorAll('.card-checkbox');
        const allChecked = [...boxes].every(b => b.checked);
        boxes.forEach(b => {
            b.checked = !allChecked;
            const siren = b.dataset.siren;
            const card = b.closest('.company-card');
            if (!allChecked) {
                selectedSirens.add(siren);
                card?.classList.add('card-selected');
            } else {
                selectedSirens.delete(siren);
                card?.classList.remove('card-selected');
            }
        });
        _updateBulkBar();
    };

    // Enrich via web
    document.getElementById('bulk-enrich-web').onclick = async () => {
        const sirens = [...selectedSirens];
        if (!sirens.length) return;
        if (sirens.length > 20) {
            showToast(t('job.maxEnrich'), 'error');
            return;
        }
        showToast(t('job.enrichLaunching', { count: sirens.length }), 'info');
        const result = await startDeepEnrich(sirens);
        if (result && result._ok !== false) {
            showToast(t('job.enrichLaunched', { count: sirens.length }), 'success');
            selectionMode = false;
            selectedSirens.clear();
            _removeBulkBar();
            await loadCompanies(_currentBatchId, _currentPage, _currentSort, _currentFilter, _currentSearchQuery);
        } else {
            showToast(t('job.enrichError'), 'error');
        }
    };

    // Delete
    document.getElementById('bulk-delete').onclick = () => _bulkDelete();
}

function _removeBulkBar() {
    const bar = document.getElementById('bulk-action-bar');
    if (bar) bar.remove();
}


async function _bulkDelete() {
    const sirens = [...selectedSirens];
    if (!sirens.length) return;
    showConfirmModal({
        title: t('job.bulkDeleteTitle', { count: sirens.length, plural: sirens.length > 1 ? 's' : '' }),
        body: `<p>${t('job.bulkDeleteBody', { count: sirens.length, plural: sirens.length > 1 ? 's' : '' })}</p>`,
        confirmLabel: t('job.suppressPermanent'),
        danger: true,
        checkboxLabel: t('job.alsoBlacklistBulk'),
        onConfirm: async (blacklist) => {
            let ok = 0;
            for (const siren of sirens) {
                const res = await untagCompany(siren, _currentBatchName);
                if (res && res._ok !== false) ok++;
                if (blacklist) {
                    await fetch('/api/blacklist', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({siren, reason: 'Supprimé en masse'}),
                        credentials: 'same-origin',
                    });
                }
            }
            showToast(t('job.bulkDeleteSuccess', { ok, total: sirens.length }), 'success');
            selectionMode = false;
            selectedSirens.clear();
            _removeBulkBar();
            await loadCompanies(_currentBatchId, _currentPage, _currentSort, _currentFilter, _currentSearchQuery);
        }
    });
}
