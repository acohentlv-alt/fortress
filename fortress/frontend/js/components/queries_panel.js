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
 * Render the queries panel HTML — 3-level fold/unfold tree with optional
 * running + queued rows for live monitor.
 * @param {Array} queries - completed query stats from /api/jobs/:id/queries.queries
 * @param {{ collapsible?: boolean, capMin?: number|null,
 *           running?: {query: string, live_count: number}|null,
 *           queued?: string[] }} opts
 */
export function renderQueriesPanel(queries, opts = {}) {
    const collapsible = opts.collapsible !== false;
    const capMin = opts.capMin != null ? opts.capMin : null;
    const running = opts.running || null;
    const queued = Array.isArray(opts.queued) ? opts.queued : [];

    if ((!queries || queries.length === 0) && !running && queued.length === 0) {
        return `<span style="color:var(--text-muted)">${t('monitor.queriesEmpty')}</span>`;
    }

    const primaries = (queries || []).filter(q => !q.is_expansion);
    const lines = [];

    for (const p of primaries) {
        const expansions = (queries || []).filter(q => q.is_expansion && q.primary_query === p.query);
        const cityExp = expansions.filter(e => e.widening_type === 'city');
        const postalExp = expansions.filter(e => e.widening_type === 'postal_code');

        const primaryEntityCount = p.new_companies || 0;
        const expansionEntityTotal = expansions.reduce((s, e) => s + (e.new_companies || 0), 0);
        const expansionCount = expansions.length;
        const totalEntityCount = primaryEntityCount + expansionEntityTotal;
        const durationStr = p.duration_sec != null ? `${p.duration_sec}s` : '';
        const primaryId = `qp-primary-${Math.random().toString(36).slice(2, 8)}`;

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

        if (collapsible) {
            if (expansionCount === 0) {
                lines.push(`
                    <div class="qp-row qp-row--done" style="margin-bottom:var(--space-sm)">
                        <div
                            class="qp-row-clickable"
                            data-search-query="${escapeHtml(p.query)}"
                            role="button"
                            tabindex="0"
                            style="display:flex; align-items:center; gap:8px; padding:6px var(--space-sm); cursor:pointer; border-radius:var(--radius-sm); background:var(--bg-elevated); border:1px solid var(--border-subtle)"
                            title="${t('job.queriesClickToFilter')}"
                        >
                            <span class="qp-state-icon" aria-label="${t('monitor.queriesStateDone')}">✓</span>
                            <strong style="font-size:var(--font-sm)">${escapeHtml(p.query)}</strong>
                            <span style="color:var(--text-muted); font-size:var(--font-xs); margin-left:auto">→ ${summaryParts.join(' · ')}</span>
                        </div>
                    </div>
                `);
            } else {
                lines.push(`
                    <div class="qp-row qp-row--done" style="margin-bottom:var(--space-sm)">
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
                            <span class="qp-state-icon" aria-label="${t('monitor.queriesStateDone')}">✓</span>
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
            lines.push(`
                <div class="qp-row qp-row--done" style="margin-bottom:var(--space-sm)">
                    <div
                        class="qp-row-clickable"
                        data-search-query="${escapeHtml(p.query)}"
                        role="button"
                        tabindex="0"
                        style="display:flex; justify-content:space-between; gap:24px; padding:6px 12px 6px 0; cursor:pointer"
                        title="${t('job.queriesClickToFilter')}"
                    >
                        <span><span class="qp-state-icon" aria-label="${t('monitor.queriesStateDone')}">✓</span> <strong>${escapeHtml(p.query)}</strong></span>
                        <span style="color:var(--text-muted); font-size:var(--font-sm)">→ ${summaryParts.join(' · ')}</span>
                    </div>
                    <div style="padding-left:var(--space-lg)">
                        ${_renderExpansionBuckets(cityExp, postalExp, expansions, capMin, collapsible)}
                    </div>
                </div>
            `);
        }
    }

    if (running && running.query) {
        lines.push(`
            <div class="qp-row qp-row--running" style="margin-bottom:var(--space-sm)">
                <div style="display:flex; align-items:center; gap:8px; padding:6px var(--space-sm); border-radius:var(--radius-sm); background:var(--bg-secondary); border:1px solid var(--warning, #f0ad4e)">
                    <span class="qp-state-icon qp-state-icon--running" aria-label="${t('monitor.queriesStateRunning')}">🔴</span>
                    <strong style="font-size:var(--font-sm)">${escapeHtml(running.query)}</strong>
                    <span style="color:var(--text-muted); font-size:var(--font-xs); margin-left:auto">${t('monitor.queriesRunningLive', { count: running.live_count != null ? running.live_count : 0 })}</span>
                </div>
            </div>
        `);
    }

    for (const q of queued) {
        if (!q) continue;
        lines.push(`
            <div class="qp-row qp-row--queued" style="margin-bottom:var(--space-xs)">
                <div style="display:flex; align-items:center; gap:8px; padding:6px var(--space-sm); border-radius:var(--radius-sm); background:transparent; border:1px dashed var(--border-subtle); opacity:0.7">
                    <span class="qp-state-icon" aria-label="${t('monitor.queriesStateQueued')}">⏸</span>
                    <span style="font-size:var(--font-sm); color:var(--text-secondary)">${escapeHtml(q)}</span>
                    <span style="color:var(--text-muted); font-size:var(--font-xs); margin-left:auto">${t('monitor.queriesStateQueued')}</span>
                </div>
            </div>
        `);
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
 * Internal: render a single level-3 branch row.
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
 * !!! NON-IDEMPOTENT — DO NOT CALL MORE THAN ONCE PER ROOT ELEMENT !!!
 * Each call adds a fresh pair of listeners. Calling twice on the same root
 * causes click events to fire qp:filter TWICE; calling N times causes Nx stacking.
 *
 * Safe usage:
 *   - monitor.js: bind ONCE, OUTSIDE the 1.5s polling callback
 *   - job.js: bind per renderJob() call (qPanel is freshly recreated each render)
 *   - DO NOT call from inside setInterval / poll loops on persistent DOM nodes
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
