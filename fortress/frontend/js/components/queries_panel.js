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

    // Done rows
    for (const p of primaries) {
        const expansions = (queries || []).filter(q => q.is_expansion && q.primary_query === p.query);
        const cityExp = expansions.filter(e => e.widening_type === 'city');
        const postalExp = expansions.filter(e => e.widening_type === 'postal_code');
        const primaryEntityCount = p.new_companies || 0;
        const expansionCount = expansions.length;
        const durationStr = p.duration_sec != null ? `${p.duration_sec}s` : '';
        const primaryId = `qp-primary-${Math.random().toString(36).slice(2, 8)}`;

        const summaryParts = [`${primaryEntityCount} ${t('monitor.queriesNewEntities')}`];
        if (expansionCount > 0) summaryParts.push(`${expansionCount} ${t('monitor.queriesElargissements') || 'élargissements'}`);
        if (durationStr) summaryParts.push(durationStr);

        if (collapsible) {
            if (expansionCount === 0) {
                lines.push(`
                    <div class="qp-row qp-row--done" style="margin-bottom:var(--space-sm)">
                        <div style="display:flex; align-items:center; gap:8px; padding:6px var(--space-sm); border-radius:var(--radius-sm); background:var(--bg-elevated); border:1px solid var(--border-subtle)">
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
                            style="display:flex; align-items:center; gap:8px; padding:6px var(--space-sm); cursor:pointer; border-radius:var(--radius-sm); background:var(--bg-elevated); border:1px solid var(--border-subtle)"
                            onclick="(function(el){
                                var body=document.getElementById('${primaryId}');
                                if(!body) return;
                                var chevron=el.querySelector('.qp-chevron');
                                var hidden=body.style.display==='none';
                                body.style.display=hidden?'':'none';
                                if(chevron) chevron.style.transform=hidden?'rotate(90deg)':'';
                            })(this)"
                        >
                            <span class="qp-chevron" style="display:inline-block; transition:transform 0.2s; color:var(--text-muted); font-size:var(--font-xs)">▸</span>
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
                    <div style="display:flex; justify-content:space-between; gap:24px; padding:6px 12px 6px 0">
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

    // Running row (single, if present and not already rendered as done)
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

    // Queued rows
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

    return `
        <div style="display:flex; gap:6px; padding:3px 0; font-size:var(--font-xs); color:var(--text-secondary)">
            <span style="color:var(--text-muted)">└─</span>
            <span style="flex:1">${escapeHtml(e.value || '')}</span>
            <span>→ ${n} ${t('monitor.queriesNewEntities')}</span>
            ${durStr}
            ${errorChip}
        </div>
    `;
}
