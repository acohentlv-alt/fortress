/**
 * Open Query Page — LLM-powered free-form query placeholder
 *
 * This is a designed placeholder awaiting backend LLM integration.
 * Shows the intended UX with example queries and a disabled input.
 */

import { t } from '../i18n.js';

export async function renderOpenQuery(container) {
    container.innerHTML = `
        <h1 class="page-title">💬 ${t('openQuery.title')}</h1>
        <p class="page-subtitle">${t('openQuery.subtitle')}</p>

        <div class="query-placeholder">
            <div class="query-placeholder-icon">🧠</div>
            <div class="query-placeholder-title">${t('openQuery.aiTitle')}</div>
            <div class="query-placeholder-desc">
                ${t('openQuery.description')}
            </div>

            <!-- Disabled input preview -->
            <div style="position:relative; max-width:480px; margin:0 auto var(--space-2xl)">
                <input type="text" disabled
                    placeholder="${t('openQuery.placeholder')}"
                    style="width:100%; padding:var(--space-lg) var(--space-xl);
                           padding-left:44px; padding-right:50px;
                           background:var(--bg-input); border:1px solid var(--border-default);
                           border-radius:var(--radius); color:var(--text-disabled);
                           font-family:var(--font-family); font-size:var(--font-base);
                           opacity:0.5; cursor:not-allowed">
                <span style="position:absolute; left:14px; top:50%; transform:translateY(-50%); font-size:1.1rem; opacity:0.5">💬</span>
                <span style="position:absolute; right:12px; top:50%; transform:translateY(-50%);
                             font-size:var(--font-xs); font-weight:600;
                             background:var(--accent-subtle); color:var(--accent);
                             padding:4px 10px; border-radius:var(--radius-full)">
                    ${t('openQuery.comingSoon')}
                </span>
            </div>

            <!-- Example queries -->
            <div style="margin-top:var(--space-xl)">
                <div style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-md)">
                    ${t('openQuery.examplesTitle')}
                </div>
                <div class="query-examples">
                    <div class="query-example">${t('openQuery.example1')}</div>
                    <div class="query-example">${t('openQuery.example2')}</div>
                    <div class="query-example">${t('openQuery.example3')}</div>
                    <div class="query-example">${t('openQuery.example4')}</div>
                    <div class="query-example">${t('openQuery.example5')}</div>
                </div>
            </div>

            <!-- Roadmap -->
            <div style="margin-top:var(--space-3xl); text-align:left; max-width:480px; margin-left:auto; margin-right:auto">
                <div style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                    ${t('openQuery.roadmap')}
                </div>
                <div style="display:flex; flex-direction:column; gap:var(--space-md)">
                    <div style="display:flex; align-items:center; gap:var(--space-md)">
                        <span style="color:var(--success)">✅</span>
                        <span style="font-size:var(--font-sm); color:var(--text-secondary)">${t('openQuery.roadmap1')}</span>
                    </div>
                    <div style="display:flex; align-items:center; gap:var(--space-md)">
                        <span style="color:var(--success)">✅</span>
                        <span style="font-size:var(--font-sm); color:var(--text-secondary)">${t('openQuery.roadmap2')}</span>
                    </div>
                    <div style="display:flex; align-items:center; gap:var(--space-md)">
                        <span style="color:var(--warning)">⏳</span>
                        <span style="font-size:var(--font-sm); color:var(--text-secondary)">${t('openQuery.roadmap3')}</span>
                    </div>
                    <div style="display:flex; align-items:center; gap:var(--space-md)">
                        <span style="color:var(--text-disabled)">⬜</span>
                        <span style="font-size:var(--font-sm); color:var(--text-muted)">${t('openQuery.roadmap4')}</span>
                    </div>
                    <div style="display:flex; align-items:center; gap:var(--space-md)">
                        <span style="color:var(--text-disabled)">⬜</span>
                        <span style="font-size:var(--font-sm); color:var(--text-muted)">${t('openQuery.roadmap5')}</span>
                    </div>
                </div>
            </div>
        </div>
    `;
}
