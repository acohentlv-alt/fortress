/**
 * Open Query Page — LLM-powered free-form query placeholder
 *
 * This is a designed placeholder awaiting backend LLM integration.
 * Shows the intended UX with example queries and a disabled input.
 */

export async function renderOpenQuery(container) {
    container.innerHTML = `
        <h1 class="page-title">💬 Requête Libre</h1>
        <p class="page-subtitle">Interrogez vos données en langage naturel</p>

        <div class="query-placeholder">
            <div class="query-placeholder-icon">🧠</div>
            <div class="query-placeholder-title">Intelligence Artificielle</div>
            <div class="query-placeholder-desc">
                Cette fonctionnalité permettra d'interroger votre base de données
                avec des questions en français. Le moteur LLM traduira vos requêtes
                en actions sur les données de Fortress.
            </div>

            <!-- Disabled input preview -->
            <div style="position:relative; max-width:480px; margin:0 auto var(--space-2xl)">
                <input type="text" disabled
                    placeholder="Posez une question sur vos données..."
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
                    Bientôt
                </span>
            </div>

            <!-- Example queries -->
            <div style="margin-top:var(--space-xl)">
                <div style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-md)">
                    Exemples de requêtes possibles
                </div>
                <div class="query-examples">
                    <div class="query-example">
                        "Quelles entreprises du 66 n'ont pas encore de téléphone ?"
                    </div>
                    <div class="query-example">
                        "Liste les SARL de transport en Haute-Garonne avec plus de 10 salariés"
                    </div>
                    <div class="query-example">
                        "Combien d'entreprises ont été enrichies cette semaine ?"
                    </div>
                    <div class="query-example">
                        "Exporte les boulangeries de Paris avec email et site web"
                    </div>
                    <div class="query-example">
                        "Quels dirigeants apparaissent dans plusieurs entreprises ?"
                    </div>
                </div>
            </div>

            <!-- Roadmap -->
            <div style="margin-top:var(--space-3xl); text-align:left; max-width:480px; margin-left:auto; margin-right:auto">
                <div style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                    Roadmap
                </div>
                <div style="display:flex; flex-direction:column; gap:var(--space-md)">
                    <div style="display:flex; align-items:center; gap:var(--space-md)">
                        <span style="color:var(--success)">✅</span>
                        <span style="font-size:var(--font-sm); color:var(--text-secondary)">Pipeline d'enrichissement stable</span>
                    </div>
                    <div style="display:flex; align-items:center; gap:var(--space-md)">
                        <span style="color:var(--success)">✅</span>
                        <span style="font-size:var(--font-sm); color:var(--text-secondary)">Interface frontend opérationnelle</span>
                    </div>
                    <div style="display:flex; align-items:center; gap:var(--space-md)">
                        <span style="color:var(--warning)">⏳</span>
                        <span style="font-size:var(--font-sm); color:var(--text-secondary)">Intégration API INPI (données financières)</span>
                    </div>
                    <div style="display:flex; align-items:center; gap:var(--space-md)">
                        <span style="color:var(--text-disabled)">⬜</span>
                        <span style="font-size:var(--font-sm); color:var(--text-muted)">Moteur LLM — Requête libre</span>
                    </div>
                    <div style="display:flex; align-items:center; gap:var(--space-md)">
                        <span style="color:var(--text-disabled)">⬜</span>
                        <span style="font-size:var(--font-sm); color:var(--text-muted)">Export intelligent (CSV/API)</span>
                    </div>
                </div>
            </div>
        </div>
    `;
}
