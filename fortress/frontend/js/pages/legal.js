/**
 * Legal Page — Mentions Légales & Politique de Confidentialité
 *
 * Public page (no auth required). Dark theme matching landing page.
 * All text hardcoded in French — no i18n/t() calls. Legal text must never
 * change with language toggle.
 */

export function renderLegal(container) {
    // Remove page-container constraints for full-bleed dark theme
    const pageContainer = container.closest('.page-container');
    if (pageContainer) {
        pageContainer.style.padding = '0';
        pageContainer.style.maxWidth = 'none';
    }
    const mainContent = document.querySelector('.main-content');
    if (mainContent) {
        mainContent.style.background = 'transparent';
    }
    document.body.style.overflow = 'auto';

    container.innerHTML = `
        <div class="landing">
            <!-- ═══════ Fixed Navigation ═══════ -->
            <nav class="land-nav">
                <a href="#/intro" class="land-nav-logo">
                    <span class="land-nav-logo-icon">🏰</span>
                    <span>Fortress</span>
                </a>
                <div class="land-nav-links">
                    <a href="#/intro" class="land-nav-link">← Retour à l'accueil</a>
                </div>
            </nav>

            <!-- ═══════ Legal Content ═══════ -->
            <div class="legal-page">

                <!-- ══ Section A : Mentions Légales ══ -->
                <div class="legal-section">
                    <h1 class="legal-title">Mentions Légales</h1>

                    <h2 class="legal-subtitle">Éditeur du site</h2>
                    <p class="legal-text">
                        <strong>Fortress</strong><br>
                        Responsable : Alan Cohen<br>
                        Forme juridique : [À COMPLÉTER]<br>
                        SIREN : [À COMPLÉTER]<br>
                        Adresse : [À COMPLÉTER]<br>
                        Email : <a href="mailto:acohen.tlv@gmail.com">acohen.tlv@gmail.com</a>
                    </p>

                    <h2 class="legal-subtitle">Hébergement</h2>
                    <p class="legal-text">
                        Ce site est hébergé par <strong>Render.com</strong> — Render Services, Inc., San Francisco, CA, USA.<br>
                        Les transferts de données vers les États-Unis sont encadrés par les Clauses Contractuelles Types (CCT).
                    </p>

                    <h2 class="legal-subtitle">Base de données</h2>
                    <p class="legal-text">
                        La base de données est hébergée par <strong>Neon.tech</strong> — région EU (Francfort, Allemagne).
                    </p>
                </div>

                <!-- ══ Section B : Politique de Confidentialité ══ -->
                <div class="legal-section">
                    <h1 class="legal-title">Politique de Confidentialité</h1>
                    <p class="legal-text"><em>Dernière mise à jour : 30 mars 2026</em></p>

                    <!-- 1. Responsable du traitement -->
                    <h2 class="legal-subtitle">1. Responsable du traitement</h2>
                    <p class="legal-text">
                        Alan Cohen — <a href="mailto:acohen.tlv@gmail.com">acohen.tlv@gmail.com</a>
                    </p>

                    <!-- 2. Données personnelles collectées -->
                    <h2 class="legal-subtitle">2. Données personnelles collectées</h2>

                    <h3 class="legal-sub-subtitle">a) Comptes utilisateurs</h3>
                    <p class="legal-text">
                        Données collectées : identifiant, mot de passe hashé, nom d'affichage.<br>
                        Base légale : exécution du contrat (Art. 6.1.b du RGPD).<br>
                        Durée de conservation : durée de vie du compte.
                    </p>

                    <h3 class="legal-sub-subtitle">b) Formulaire de contact</h3>
                    <p class="legal-text">
                        Données collectées : nom, email, entreprise, message.<br>
                        Base légale : consentement (Art. 6.1.a du RGPD).<br>
                        Durée de conservation : 12 mois.
                    </p>

                    <h3 class="legal-sub-subtitle">c) Données B2B professionnelles</h3>
                    <p class="legal-text">
                        Données traitées : raisons sociales, noms de dirigeants, emails professionnels, numéros de téléphone, adresses, codes NAF.<br>
                        Sources : SIRENE (INSEE), Google Maps, sites web d'entreprises.<br>
                        Base légale : intérêt légitime (Art. 6.1.f du RGPD) — prospection commerciale B2B.<br>
                        Durée de conservation : tant que l'entreprise est active au registre SIRENE.
                    </p>

                    <h3 class="legal-sub-subtitle">d) Journal d'activité</h3>
                    <p class="legal-text">
                        Données collectées : actions effectuées dans l'application (recherches, modifications, exports).<br>
                        Base légale : intérêt légitime (sécurité et audit interne).<br>
                        Durée de conservation : 12 mois.
                    </p>

                    <h3 class="legal-sub-subtitle">e) Cookies</h3>
                    <p class="legal-text">
                        Ce site utilise un seul cookie de session : <strong>fortress_session</strong>.<br>
                        Ce cookie est strictement nécessaire au fonctionnement du service (authentification). Il ne contient aucune donnée personnelle et n'est pas utilisé à des fins de traçage ou de publicité.<br>
                        Conformément à l'article 82 de la loi Informatique et Libertés, ce type de cookie est exempté de consentement préalable.<br>
                        Aucun cookie de traçage ou de mesure d'audience n'est utilisé.
                    </p>

                    <!-- 3. Destinataires -->
                    <h2 class="legal-subtitle">3. Destinataires des données</h2>
                    <p class="legal-text">
                        Les données sont accessibles à :
                    </p>
                    <ul class="legal-list">
                        <li>Alan Cohen (administrateur) — accès global à toutes les données</li>
                        <li>Les responsables d'espace de travail — accès limité aux données de leur espace uniquement</li>
                    </ul>
                    <p class="legal-text">
                        Aucune donnée n'est partagée, vendue ou transmise à des tiers à des fins commerciales.
                    </p>

                    <!-- 4. Transferts internationaux -->
                    <h2 class="legal-subtitle">4. Transferts internationaux</h2>
                    <p class="legal-text">
                        L'hébergement applicatif est assuré par Render.com (États-Unis). Ce transfert est encadré par les Clauses Contractuelles Types (CCT) adoptées par la Commission européenne.<br>
                        La base de données est hébergée par Neon.tech dans la région EU (Francfort, Allemagne) — aucun transfert hors UE pour les données stockées.
                    </p>

                    <!-- 5. Vos droits -->
                    <h2 class="legal-subtitle">5. Vos droits</h2>
                    <p class="legal-text">
                        Conformément au RGPD, vous disposez des droits suivants sur vos données personnelles :
                    </p>
                    <ul class="legal-list">
                        <li>Droit d'accès (Art. 15)</li>
                        <li>Droit de rectification (Art. 16)</li>
                        <li>Droit à l'effacement (Art. 17)</li>
                        <li>Droit d'opposition (Art. 21)</li>
                        <li>Droit à la portabilité (Art. 20)</li>
                        <li>Droit au retrait du consentement à tout moment</li>
                    </ul>
                    <p class="legal-text">
                        Pour exercer ces droits, contactez : <a href="mailto:acohen.tlv@gmail.com">acohen.tlv@gmail.com</a>
                    </p>
                    <p class="legal-text">
                        Vous pouvez également introduire une réclamation auprès de la CNIL :<br>
                        Commission Nationale de l'Informatique et des Libertés<br>
                        3 Place de Fontenoy, TSA 80715, 75334 Paris Cedex 07<br>
                        <a href="https://www.cnil.fr" target="_blank" rel="noopener noreferrer">www.cnil.fr</a>
                    </p>
                </div>

                <!-- ═══════ Back link ═══════ -->
                <div style="text-align:center; padding-bottom:40px">
                    <a href="#/intro" class="land-nav-link" style="font-size:1rem">← Retour à l'accueil</a>
                </div>
            </div>
        </div>
    `;
}
