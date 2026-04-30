/**
 * Queries Panel Component — 3-level collapsible tree for batch search history
 *
 * Renders primary queries with optional expansion sub-buckets (cities / postal codes).
 * Used in both monitor.js (live, not collapsible) and job.js (post-batch, collapsible).
 */

import { escapeHtml } from '../components.js';
import { t } from '../i18n.js';

/**
 * Translate a widening stop_reason code to a French label.
 * @param {string|null} reason
 * @param {number|null} cumulative
 * @param {number|null} capMin - time cap in minutes (forwarded from batch settings)
 */
export function stopReasonText(reason, cumulative, capMin) {
    switch (reason) {
        case 'threshold_met_dry_streak':
            return t('monitor.queriesStopThreshold').replace('{{n}}', cumulative != null ? cumulative : '?');
        case 'candidates_exhausted':
            return t('monitor.queriesStopExhausted');
        case 'max_per_primary':
            return t('monitor.queriesStopMaxPerPrimary').replace('{{n}}', '12');
        case 'time_cap_reached':
            return t('monitor.queriesStopTimeCap').replace('{{n}}', capMin != null ? capMin : '?');
        default:
            return t('monitor.queriesStopGeneric');
    }
}

/**
 * Render the queries panel HTML — 3-level fold/unfold tree.
 * @param {Array} queries - array of query stat objects from /api/jobs/:id/queries
 * @param {{ collapsible?: boolean, capMin?: number|null }} opts
 * @returns {string} HTML string
 */
export function renderQueriesPanel(queries, opts = { collapsible: true, capMin: null }) {
    if (!queries || queries.length === 0) {
        return `<span style="color:var(--text-muted)">${t('monitor.queriesEmpty')}</span>`;
    }

    const collapsible = opts.collapsible !== false;
    const capMin = opts.capMin != null ? opts.capMin : null;

    const primaries = queries.filter(q => !q.is_expansion);
    const lines = [];

    for (const p of primaries) {
        const expansions = queries.filter(q => q.is_expansion && q.primary_query === p.query);

        // Separate expansions into city and postal sub-buckets
        const cityExp = expansions.filter(e => e.widening_type === 'city');
        const postalExp = expansions.filter(e => e.widening_type === 'postal_code');

        // E4.B — compute cumulative total including all expansion entities
        const primaryEntityCount = p.new_companies || 0;
        const expansionEntityTotal = expansions.reduce((s, e) => s + (e.new_companies || 0), 0);
        const expansionCount = expansions.length;
        const totalEntityCount = primaryEntityCount + expansionEntityTotal;
        const durationStr = p.duration_sec != null ? `${p.duration_sec}s` : '';

        const primaryId = `qp-primary-${Math.random().toString(36).slice(2, 8)}`;

        if (collapsible) {
            // Level 1: primary row — E4.B phrasing + E4.A clickable row
            const summaryParts = [];
            if (expansionCount > 0) {
                // E4.B — headline cumulative total, expansion breakdown in parens.
                // FR: "13 total (dont 11 par élargissement)"
                summaryParts.push(t('monitor.queriesPrimaryWithExpansions', {
                    total: totalEntityCount,
                    expansion: expansionEntityTotal,
                }));
            } else {
                summaryParts.push(`${primaryEntityCount} ${t('monitor.queriesNewEntities')}`);
            }
            if (durationStr) summaryParts.push(durationStr);

            if (expansionCount === 0) {
                // Flat row — nothing to expand; clickable for drill-down
                lines.push(`
                    <div style="margin-bottom:var(--space-sm)">
                        <div
                            class="qp-row-clickable"
                            data-search-query="${escapeHtml(p.query)}"
                            role="button"
                            tabindex="0"
                            style="display:flex; align-items:center; gap:8px; padding:6px var(--space-sm); cursor:pointer; border-radius:var(--radius-sm); background:var(--bg-elevated); border:1px solid var(--border-subtle)"
                            title="${t('job.queriesClickToFilter')}"
                        >
                            <strong style="font-size:var(--font-sm)">${escapeHtml(p.query)}</strong>
                            <span style="color:var(--text-muted); font-size:var(--font-xs); margin-left:auto">→ ${summaryParts.join(' · ')}</span>
                        </div>
                    </div>
                `);
            } else {
                // Row with expansions: chevron toggles body, rest of row fires filter.
                lines.push(`
                    <div style="margin-bottom:var(--space-sm)">
                        <div
                            class="qp-row-clickable"
                            data-search-query="${escapeHtml(p.query)}"
                            role="button"
                            tabindex="0"
                            style="display:flex; align-items:center; gap:8px; padding:6px var(--space-sm); cursor:pointer; border-radius:var(--radius-sm); background:var(--bg-elevated); border:1px solid var(--border-subtle)"
                            title="${t('job.queriesClickToFilter')}"
                        >
                            <span
                                class="qp-chevron-btn"
                                data-toggle-target="${primaryId}"
                                style="cursor:pointer; padding:0 4px"
                                onclick="event.stopPropagation(); (function(el){
                                    var body=document.getElementById(el.dataset.toggleTarget);
                                    if(!body) return;
                                    var chevron=el.querySelector('.qp-chevron');
                                    var hidden=body.style.display==='none';
                                    body.style.display=hidden?'':'none';
                                    if(chevron) chevron.style.transform=hidden?'rotate(90deg)':'';
                                })(this)"
                            >
                                <span class="qp-chevron" style="display:inline-block; transition:transform 0.2s; color:var(--text-muted); font-size:var(--font-xs)">▸</span>
                            </span>
                            <span class="qp-state-icon" aria-label="Terminé">✓</span>
                            <strong style="font-size:var(--font-sm)">${escapeHtml(p.query)}</strong>
                            <span style="color:var(--text-muted); font-size:var(--font-xs); margin-left:auto">→ ${summaryParts.join(' · ')}</span>
                        </div>
                        <div id="${primaryId}" style="display:none; padding-left:var(--space-lg); padding-top:var(--space-xs)">
                            ${_renderExpansionBuckets(cityExp, postalExp, expansions, capMin, collapsible)}
                        </div>
                    </div>
                `);
            }
        } else {
            // Live monitor: fully expanded, no chevrons
            const summaryParts = [];
            if (expansionCount > 0) {
                summaryParts.push(t('monitor.queriesPrimaryWithExpansions', {
                    total: totalEntityCount,
                    expansion: expansionEntityTotal,
                }));
            } else {
                summaryParts.push(`${primaryEntityCount} ${t('monitor.queriesNewEntities')}`);
            }
            if (durationStr) summaryParts.push(durationStr);

            lines.push(`
                <div style="margin-bottom:var(--space-sm)">
                    <div
                        class="qp-row-clickable"
                        data-search-query="${escapeHtml(p.query)}"
                        role="button"
                        tabindex="0"
                        style="display:flex; justify-content:space-between; gap:24px; padding:6px 12px 6px 0; cursor:pointer"
                        title="${t('job.queriesClickToFilter')}"
                    >
                        <span><strong>${escapeHtml(p.query)}</strong></span>
                        <span style="color:var(--text-muted); font-size:var(--font-sm)">→ ${summaryParts.join(' · ')}</span>
                    </div>
                    <div style="padding-left:var(--space-lg)">
                        ${_renderExpansionBuckets(cityExp, postalExp, expansions, capMin, collapsible)}
                    </div>
                </div>
            `);
        }
    }

    return `<div style="font-size:var(--font-sm)">${lines.join('')}</div>`;
}

/**
 * Internal: render level-2 sub-buckets (cities / postal codes) and level-3 branches.
 */
function _renderExpansionBuckets(cityExp, postalExp, allExpansions, capMin, collapsible) {
    if (allExpansions.length === 0) return '';

    const lines = [];

    // Stop reason chip
    const lastExp = allExpansions[allExpansions.length - 1];
    if (lastExp && lastExp.stop_reason) {
        const reasonText = stopReasonText(lastExp.stop_reason, lastExp.primary_cumulative_yield_after, capMin);
        lines.push(`<div style="padding:4px 0 6px 0; font-size:var(--font-xs); color:var(--text-muted); font-style:italic">[${escapeHtml(reasonText)}]</div>`);
    }

    if (cityExp.length > 0) {
        const cityTotal = cityExp.reduce((s, e) => s + (e.new_companies || 0), 0);
        const cityId = `qp-city-${Math.random().toString(36).slice(2, 8)}`;

        if (collapsible) {
            lines.push(`
                <div style="margin-bottom:var(--space-xs)">
                    <div
                        style="display:flex; align-items:center; gap:6px; padding:4px var(--space-xs); cursor:pointer; color:var(--text-secondary); font-size:var(--font-xs)"
                        onclick="(function(el){
                            var body=document.getElementById('${cityId}');
                            if(!body) return;
                            var chevron=el.querySelector('.qp-chevron');
                            var hidden=body.style.display==='none';
                            body.style.display=hidden?'':'none';
                            if(chevron) chevron.style.transform=hidden?'rotate(90deg)':'';
                        })(this)"
                    >
                        <span style="color:var(--text-muted)">└─</span>
                        <span class="qp-chevron" style="display:inline-block; transition:transform 0.2s; color:var(--text-muted)">▸</span>
                        <span>${t('job.queriesCitiesBucket').replace('{{count}}', cityExp.length)}</span>
                        <span style="margin-left:auto; color:var(--text-muted)">→ ${cityTotal} ${t('monitor.queriesNewEntities')}</span>
                    </div>
                    <div id="${cityId}" style="display:none; padding-left:var(--space-lg)">
                        ${cityExp.map(e => _renderBranchRow(e)).join('')}
                    </div>
                </div>
            `);
        } else {
            lines.push(`
                <div style="margin-bottom:var(--space-xs)">
                    <div style="display:flex; gap:6px; padding:4px 0; color:var(--text-secondary); font-size:var(--font-xs)">
                        <span style="color:var(--text-muted)">└─</span>
                        <span>${t('job.queriesCitiesBucket').replace('{{count}}', cityExp.length)}</span>
                        <span style="margin-left:auto; color:var(--text-muted)">→ ${cityTotal} ${t('monitor.queriesNewEntities')}</span>
                    </div>
                    <div style="padding-left:var(--space-lg)">
                        ${cityExp.map(e => _renderBranchRow(e)).join('')}
                    </div>
                </div>
            `);
        }
    }

    if (postalExp.length > 0) {
        const postalTotal = postalExp.reduce((s, e) => s + (e.new_companies || 0), 0);
        const postalId = `qp-postal-${Math.random().toString(36).slice(2, 8)}`;

        if (collapsible) {
            lines.push(`
                <div style="margin-bottom:var(--space-xs)">
                    <div
                        style="display:flex; align-items:center; gap:6px; padding:4px var(--space-xs); cursor:pointer; color:var(--text-secondary); font-size:var(--font-xs)"
                        onclick="(function(el){
                            var body=document.getElementById('${postalId}');
                            if(!body) return;
                            var chevron=el.querySelector('.qp-chevron');
                            var hidden=body.style.display==='none';
                            body.style.display=hidden?'':'none';
                            if(chevron) chevron.style.transform=hidden?'rotate(90deg)':'';
                        })(this)"
                    >
                        <span style="color:var(--text-muted)">└─</span>
                        <span class="qp-chevron" style="display:inline-block; transition:transform 0.2s; color:var(--text-muted)">▸</span>
                        <span>${t('job.queriesPostalsBucket').replace('{{count}}', postalExp.length)}</span>
                        <span style="margin-left:auto; color:var(--text-muted)">→ ${postalTotal} ${t('monitor.queriesNewEntities')}</span>
                    </div>
                    <div id="${postalId}" style="display:none; padding-left:var(--space-lg)">
                        ${postalExp.map(e => _renderBranchRow(e)).join('')}
                    </div>
                </div>
            `);
        } else {
            lines.push(`
                <div style="margin-bottom:var(--space-xs)">
                    <div style="display:flex; gap:6px; padding:4px 0; color:var(--text-secondary); font-size:var(--font-xs)">
                        <span style="color:var(--text-muted)">└─</span>
                        <span>${t('job.queriesPostalsBucket').replace('{{count}}', postalExp.length)}</span>
                        <span style="margin-left:auto; color:var(--text-muted)">→ ${postalTotal} ${t('monitor.queriesNewEntities')}</span>
                    </div>
                    <div style="padding-left:var(--space-lg)">
                        ${postalExp.map(e => _renderBranchRow(e)).join('')}
                    </div>
                </div>
            `);
        }
    }

    return lines.join('');
}

/**
 * Internal: render a single level-3 branch row (E4.A: clickable if e.query is populated).
 */
function _renderBranchRow(e) {
    const n = e.new_companies || 0;
    const dur = e.duration_sec;
    const durColor = dur != null && dur > 60 ? 'var(--danger)' : 'var(--text-muted)';
    const durStr = dur != null ? `<span style="color:${durColor}">${dur}s</span>` : '';
    const errorChip = e.error
        ? `<span style="color:var(--danger); cursor:help; margin-left:4px" title="${escapeHtml(e.error)}">❌ ${t('job.queriesBranchError')}</span>`
        : '';
    const branchQuery = e.query || '';

    return `
        <div
            class="qp-row-clickable qp-row-clickable--branch"
            data-search-query="${escapeHtml(branchQuery)}"
            role="button"
            tabindex="0"
            style="display:flex; gap:6px; padding:3px 0; font-size:var(--font-xs); color:var(--text-secondary); cursor:${branchQuery ? 'pointer' : 'default'}"
            title="${branchQuery ? t('job.queriesClickToFilter') : ''}"
        >
            <span style="color:var(--text-muted)">└─</span>
            <span style="flex:1">${escapeHtml(e.value || '')}</span>
            <span>→ ${n} ${t('monitor.queriesNewEntities')}</span>
            ${durStr}
            ${errorChip}
        </div>
    `;
}

/**
 * Bind click + keyboard handlers on the panel's clickable rows.
 * Dispatches a `qp:filter` CustomEvent with detail.searchQuery on the panel root.
 *
 * @param {HTMLElement} root - the container that wraps renderQueriesPanel output
 */
export function bindQueriesPanelClicks(root) {
    if (!root) return;
    const handler = (e) => {
        const target = e.target.closest('.qp-row-clickable');
        if (!target) return;
        const sq = target.dataset.searchQuery;
        if (!sq) return;
        root.dispatchEvent(new CustomEvent('qp:filter', { detail: { searchQuery: sq }, bubbles: true }));
    };
    root.addEventListener('click', handler);
    root.addEventListener('keydown', (e) => {
        if (e.key !== 'Enter' && e.key !== ' ') return;
        const target = e.target.closest('.qp-row-clickable');
        if (!target) return;
        e.preventDefault();
        const sq = target.dataset.searchQuery;
        if (!sq) return;
        root.dispatchEvent(new CustomEvent('qp:filter', { detail: { searchQuery: sq }, bubbles: true }));
    });
}
