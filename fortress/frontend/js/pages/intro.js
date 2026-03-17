/**
 * Introduction Page — Public landing page for Fortress
 *
 * Visitors see this FIRST. Shows the value proposition, 3-step flow,
 * key stats, and a CTA to the login page.
 * No authentication required.
 */

export function renderIntro(container) {
    // Hide sidebar + header for intro page
    container.innerHTML = `
        <div class="intro-page">
            <!-- ═══════ Hero Section ═══════ -->
            <section class="intro-hero">
                <div class="intro-hero-inner">
                    <div class="intro-logo">
                        <span class="intro-logo-icon">🏰</span>
                        <span class="intro-logo-text">Fortress</span>
                    </div>

                    <h1 class="intro-headline">
                        Intelligence B2B<br>
                        <span class="intro-headline-accent">du marché français</span>
                    </h1>

                    <p class="intro-subtitle">
                        Trouvez, enrichissez et exportez des leads B2B qualifiés.<br>
                        14.7 millions d'entreprises françaises à portée de clic.
                    </p>

                    <div class="intro-cta-row">
                        <a href="#/login" class="intro-cta-primary">
                            Commencer →
                        </a>
                    </div>
                </div>

                <!-- Floating accent orbs (pure CSS) -->
                <div class="intro-orb intro-orb-1"></div>
                <div class="intro-orb intro-orb-2"></div>
                <div class="intro-orb intro-orb-3"></div>
            </section>

            <!-- ═══════ How It Works ═══════ -->
            <section class="intro-section">
                <h2 class="intro-section-title">Comment ça marche</h2>
                <p class="intro-section-subtitle">Trois étapes pour transformer une idée en fichier de prospection</p>

                <div class="intro-steps-grid">
                    <div class="intro-step-card">
                        <div class="intro-step-num">1</div>
                        <div class="intro-step-icon">🔍</div>
                        <h3 class="intro-step-name">Recherche</h3>
                        <p class="intro-step-text">
                            Décrivez ce que vous cherchez : <strong>"camping Perpignan"</strong>, 
                            <strong>"transport 66"</strong>, <strong>"restaurant Lyon"</strong>. 
                            Le moteur parcourt Google Maps pour trouver les entreprises actives dans votre zone.
                        </p>
                    </div>

                    <div class="intro-step-card">
                        <div class="intro-step-num">2</div>
                        <div class="intro-step-icon">⚡</div>
                        <h3 class="intro-step-name">Enrichissement</h3>
                        <p class="intro-step-text">
                            Chaque entreprise est enrichie automatiquement : 
                            <strong>téléphone</strong>, <strong>email</strong>, <strong>site web</strong>, 
                            <strong>réseaux sociaux</strong>. Les données sont croisées avec le registre officiel SIRENE.
                        </p>
                    </div>

                    <div class="intro-step-card">
                        <div class="intro-step-num">3</div>
                        <div class="intro-step-icon">📥</div>
                        <h3 class="intro-step-name">Export</h3>
                        <p class="intro-step-text">
                            Exportez vos leads en <strong>CSV</strong> ou <strong>XLSX</strong>. 
                            Données structurées, prêtes pour votre CRM, vos campagnes email 
                            ou votre prospection commerciale.
                        </p>
                    </div>
                </div>
            </section>

            <!-- ═══════ Key Numbers ═══════ -->
            <section class="intro-section intro-numbers-section">
                <div class="intro-numbers">
                    <div class="intro-number-item">
                        <div class="intro-number-value">14.7M+</div>
                        <div class="intro-number-label">Entreprises indexées</div>
                        <div class="intro-number-desc">Base SIRENE complète</div>
                    </div>
                    <div class="intro-number-divider"></div>
                    <div class="intro-number-item">
                        <div class="intro-number-value">~86%</div>
                        <div class="intro-number-label">Taux de succès Maps</div>
                        <div class="intro-number-desc">Téléphone trouvé</div>
                    </div>
                    <div class="intro-number-divider"></div>
                    <div class="intro-number-item">
                        <div class="intro-number-value">0€</div>
                        <div class="intro-number-label">Coût d'utilisation</div>
                        <div class="intro-number-desc">Aucun abonnement</div>
                    </div>
                    <div class="intro-number-divider"></div>
                    <div class="intro-number-item">
                        <div class="intro-number-value">2</div>
                        <div class="intro-number-label">Sources de données</div>
                        <div class="intro-number-desc">Maps + Crawl</div>
                    </div>
                </div>
            </section>

            <!-- ═══════ What You Get ═══════ -->
            <section class="intro-section">
                <h2 class="intro-section-title">Ce que vous obtenez</h2>
                <p class="intro-section-subtitle">Pour chaque entreprise trouvée</p>

                <div class="intro-features-grid">
                    <div class="intro-feature">
                        <span class="intro-feature-icon">📞</span>
                        <span class="intro-feature-text">Numéro de téléphone</span>
                    </div>
                    <div class="intro-feature">
                        <span class="intro-feature-icon">✉️</span>
                        <span class="intro-feature-text">Adresse email</span>
                    </div>
                    <div class="intro-feature">
                        <span class="intro-feature-icon">🌐</span>
                        <span class="intro-feature-text">Site web</span>
                    </div>
                    <div class="intro-feature">
                        <span class="intro-feature-icon">🔗</span>
                        <span class="intro-feature-text">LinkedIn & Facebook</span>
                    </div>
                    <div class="intro-feature">
                        <span class="intro-feature-icon">📍</span>
                        <span class="intro-feature-text">Adresse complète</span>
                    </div>
                    <div class="intro-feature">
                        <span class="intro-feature-icon">🏢</span>
                        <span class="intro-feature-text">Données SIRENE (NAF, SIRET)</span>
                    </div>
                    <div class="intro-feature">
                        <span class="intro-feature-icon">⭐</span>
                        <span class="intro-feature-text">Note Google Maps</span>
                    </div>
                    <div class="intro-feature">
                        <span class="intro-feature-icon">👥</span>
                        <span class="intro-feature-text">Dirigeants (si disponible)</span>
                    </div>
                </div>
            </section>

            <!-- ═══════ Bottom CTA ═══════ -->
            <section class="intro-section intro-bottom-cta">
                <h2 class="intro-section-title">Prêt à commencer ?</h2>
                <p class="intro-section-subtitle">Connectez-vous pour accéder à la plateforme</p>
                <a href="#/login" class="intro-cta-primary" style="margin-top:var(--space-lg)">
                    Se connecter →
                </a>
                <p class="intro-brand-footer">
                    Fortress v1.0 · Données SIRENE + Google Maps + Crawl · Zéro coût
                </p>
            </section>
        </div>
    `;
}
