/**
 * Premium Landing Page — Marketing page for Fortress
 *
 * respond.io-quality design with:
 * - Gradient mesh hero with floating particles
 * - Animated counters
 * - 3-step process flow
 * - Bento grid feature showcase
 * - Trust indicators
 * - Contact CTA
 * - Scroll-triggered animations
 *
 * All text in French. Target: potential B2B clients.
 */

import { registerCleanup } from '../app.js';

export function renderIntro(container) {

    container.innerHTML = `
        <div class="landing">
            <!-- ═══════ Fixed Navigation ═══════ -->
            <nav class="land-nav">
                <a href="#/intro" class="land-nav-logo">
                    <span class="land-nav-logo-icon">🏰</span>
                    <span>Fortress</span>
                </a>
                <div class="land-nav-links">
                    <a href="#section-how" class="land-nav-link" data-scroll="how">Fonctionnement</a>
                    <a href="#section-features" class="land-nav-link" data-scroll="features">Fonctionnalités</a>
                    <a href="#section-contact" class="land-nav-link" data-scroll="contact">Contact</a>
                    <a href="#/login" class="land-nav-cta">Se connecter</a>
                </div>
            </nav>

            <!-- ═══════ Hero Section ═══════ -->
            <section class="land-hero">
                <div class="land-grid-overlay"></div>
                ${Array.from({length: 8}, (_, i) => `<div class="land-particle"></div>`).join('')}

                <div class="land-hero-content">
                    <div class="land-badge">
                        <span class="land-badge-dot"></span>
                        Plateforme d'intelligence commerciale B2B
                    </div>

                    <h1 class="land-h1">
                        Trouvez vos prochains<br>
                        <span class="land-h1-gradient">clients en France</span>
                    </h1>

                    <p class="land-hero-sub">
                        Accédez à <strong>14.7 millions d'entreprises françaises</strong>,
                        enrichies avec téléphones, emails et contacts dirigeants.
                        Exportez vos fichiers de prospection en quelques clics.
                    </p>

                    <div class="land-hero-ctas">
                        <a href="#section-contact" class="land-btn-primary" data-scroll="contact">
                            Demander un accès →
                        </a>
                        <a href="#section-how" class="land-btn-secondary" data-scroll="how">
                            Découvrir la plateforme
                        </a>
                    </div>
                </div>
            </section>

            <!-- ═══════ Stats Bar ═══════ -->
            <section class="land-section land-section-centered">
                <div class="land-stats-bar land-reveal">
                    <div class="land-stat">
                        <div class="land-stat-value" data-count="14700000" data-suffix="M+" data-display="14.7">14.7M+</div>
                        <div class="land-stat-label">Entreprises françaises</div>
                    </div>
                    <div class="land-stat">
                        <div class="land-stat-value" data-count="86" data-suffix="%">86%</div>
                        <div class="land-stat-label">Taux de découverte téléphone</div>
                    </div>
                    <div class="land-stat">
                        <div class="land-stat-value" data-count="101" data-suffix="" data-display="101">101</div>
                        <div class="land-stat-label">Départements couverts</div>
                    </div>
                    <div class="land-stat">
                        <div class="land-stat-value">Temps réel</div>
                        <div class="land-stat-label">Données actualisées</div>
                    </div>
                </div>
            </section>

            <!-- ═══════ How It Works ═══════ -->
            <section class="land-section" id="section-how">
                <div class="land-section-label land-reveal">COMMENT ÇA MARCHE</div>
                <h2 class="land-section-title land-reveal land-reveal-delay-1">
                    De la recherche à la prospection<br>en <span class="land-h1-gradient">3 étapes</span>
                </h2>
                <p class="land-section-sub land-reveal land-reveal-delay-2">
                    Fortress automatise l'ensemble du processus de constitution de fichiers B2B.
                    Plus besoin de scraper manuellement ou d'acheter des bases obsolètes.
                </p>

                <div class="land-steps">
                    <div class="land-step land-reveal land-reveal-delay-1">
                        <div class="land-step-num">1</div>
                        <div class="land-step-title">Ciblez votre marché</div>
                        <div class="land-step-desc">
                            Décrivez simplement ce que vous cherchez :
                            <strong>« restaurant Lyon »</strong>,
                            <strong>« transport Perpignan »</strong>,
                            <strong>« agence immobilière Paris »</strong>.
                            Le moteur traduit votre recherche en requête sur la base SIRENE officielle.
                        </div>
                        <div class="land-step-tags">
                            <span class="land-step-tag">Secteur d'activité</span>
                            <span class="land-step-tag">Localisation</span>
                            <span class="land-step-tag">Code NAF</span>
                        </div>
                    </div>

                    <div class="land-step land-reveal land-reveal-delay-2">
                        <div class="land-step-num">2</div>
                        <div class="land-step-title">Enrichissement automatique</div>
                        <div class="land-step-desc">
                            Fortress parcourt Google Maps et les sites web de chaque entreprise
                            pour extraire les données de contact :
                            <strong>téléphone</strong>, <strong>email</strong>, <strong>site web</strong>,
                            <strong>réseaux sociaux</strong>, <strong>dirigeants</strong>.
                        </div>
                        <div class="land-step-tags">
                            <span class="land-step-tag">Google Maps</span>
                            <span class="land-step-tag">Crawl Web</span>
                            <span class="land-step-tag">SIRENE</span>
                        </div>
                    </div>

                    <div class="land-step land-reveal land-reveal-delay-3">
                        <div class="land-step-num">3</div>
                        <div class="land-step-title">Exportez et prospectez</div>
                        <div class="land-step-desc">
                            Téléchargez vos leads qualifiés en
                            <strong>CSV</strong> ou <strong>Excel</strong>.
                            Données structurées, prêtes pour votre CRM,
                            vos campagnes email ou vos appels commerciaux.
                        </div>
                        <div class="land-step-tags">
                            <span class="land-step-tag">CSV / Excel</span>
                            <span class="land-step-tag">CRM-ready</span>
                            <span class="land-step-tag">RGPD</span>
                        </div>
                    </div>
                </div>
            </section>

            <!-- ═══════ Feature Bento Grid ═══════ -->
            <section class="land-section" id="section-features">
                <div class="land-section-label land-reveal">FONCTIONNALITÉS</div>
                <h2 class="land-section-title land-reveal land-reveal-delay-1">
                    Tout ce dont vous avez besoin<br>pour <span class="land-h1-gradient">prospecter efficacement</span>
                </h2>

                <div class="land-bento">
                    <!-- Wide card: Data quality -->
                    <div class="land-bento-card land-bento-wide land-reveal land-reveal-delay-1">
                        <div class="land-data-bars">
                            <div class="land-data-bar"></div>
                            <div class="land-data-bar"></div>
                            <div class="land-data-bar"></div>
                            <div class="land-data-bar"></div>
                            <div class="land-data-bar"></div>
                            <div class="land-data-bar"></div>
                        </div>
                        <div class="land-bento-title">Données vérifiées en temps réel</div>
                        <div class="land-bento-desc">
                            Chaque contact est extrait et vérifié au moment de votre recherche.
                            Pas de base de données périmée — les données datent du jour même.
                        </div>
                    </div>

                    <!-- Tall card: Contact data -->
                    <div class="land-bento-card land-bento-tall land-reveal land-reveal-delay-2">
                        <div class="land-bento-icon">📞</div>
                        <div class="land-bento-title">Coordonnées directes</div>
                        <div class="land-bento-desc">
                            Téléphone fixe et mobile, email professionnel,
                            site web, adresse postale — pour chaque entreprise trouvée.
                            86% de taux de découverte sur les téléphones.
                        </div>
                    </div>

                    <!-- Standard card: Social networks -->
                    <div class="land-bento-card land-reveal land-reveal-delay-1">
                        <div class="land-connections">
                            <div class="land-connection-dot"></div>
                            <div class="land-connection-dot"></div>
                            <div class="land-connection-dot"></div>
                            <div class="land-connection-dot"></div>
                            <div class="land-connection-dot"></div>
                            <div class="land-connection-dot"></div>
                        </div>
                        <div class="land-bento-title">Réseaux sociaux</div>
                        <div class="land-bento-desc">
                            LinkedIn, Facebook, Instagram, Twitter et 30+ réseaux détectés
                            automatiquement depuis les sites web.
                        </div>
                    </div>

                    <!-- Standard card: Directors -->
                    <div class="land-bento-card land-reveal land-reveal-delay-2">
                        <div class="land-bento-icon">👥</div>
                        <div class="land-bento-title">Dirigeants & décideurs</div>
                        <div class="land-bento-desc">
                            Noms, fonctions, lignes directes et emails personnels
                            des dirigeants — extraits des registres officiels.
                        </div>
                    </div>

                    <!-- Wide card: SIRENE -->
                    <div class="land-bento-card land-bento-wide land-reveal land-reveal-delay-3">
                        <div class="land-bento-icon">🏛️</div>
                        <div class="land-bento-title">Base SIRENE officielle</div>
                        <div class="land-bento-desc">
                            Croisement systématique avec les données officielles de l'INSEE :
                            SIREN, SIRET, code NAF, adresse du siège, statut juridique, date de création.
                            14.7 millions d'établissements indexés et consultables en moins d'une seconde.
                        </div>
                    </div>

                    <!-- Standard card: Maps -->
                    <div class="land-bento-card land-reveal land-reveal-delay-1">
                        <div class="land-bento-icon">📍</div>
                        <div class="land-bento-title">Google Maps intégré</div>
                        <div class="land-bento-desc">
                            Notes Google, nombre d'avis, horaires d'ouverture,
                            catégorie Maps — des indicateurs de vitalité commerciale.
                        </div>
                    </div>

                    <!-- Standard card: Export -->
                    <div class="land-bento-card land-reveal land-reveal-delay-2">
                        <div class="land-bento-icon">📊</div>
                        <div class="land-bento-title">Export sur mesure</div>
                        <div class="land-bento-desc">
                            CSV, Excel avec les colonnes de votre choix.
                            Compatible Salesforce, HubSpot, Pipedrive
                            et tout CRM standard.
                        </div>
                    </div>

                    <!-- Standard card: Multi-user -->
                    <div class="land-bento-card land-reveal land-reveal-delay-3">
                        <div class="land-bento-icon">🔐</div>
                        <div class="land-bento-title">Multi-utilisateurs</div>
                        <div class="land-bento-desc">
                            Chaque membre de votre équipe a son espace.
                            L'administrateur voit l'ensemble des données enrichies
                            et l'activité de chaque utilisateur.
                        </div>
                    </div>
                </div>
            </section>

            <!-- ═══════ Trust Indicators ═══════ -->
            <div class="land-trust land-reveal">
                <div class="land-trust-item">
                    <span class="land-trust-icon">🇫🇷</span>
                    <span class="land-trust-label">Données françaises</span>
                </div>
                <div class="land-trust-item">
                    <span class="land-trust-icon">🔒</span>
                    <span class="land-trust-label">Hébergement sécurisé</span>
                </div>
                <div class="land-trust-item">
                    <span class="land-trust-icon">⚡</span>
                    <span class="land-trust-label">Résultats en minutes</span>
                </div>
                <div class="land-trust-item">
                    <span class="land-trust-icon">🏛️</span>
                    <span class="land-trust-label">Source officielle INSEE</span>
                </div>
                <div class="land-trust-item">
                    <span class="land-trust-icon">📋</span>
                    <span class="land-trust-label">Conforme RGPD</span>
                </div>
            </div>

            <!-- ═══════ CTA / Contact Section ═══════ -->
            <section class="land-cta-section" id="section-contact">
                <div class="land-cta-content land-reveal">
                    <h2 class="land-cta-title">
                        Prêt à développer<br>
                        <span class="land-h1-gradient">votre portefeuille clients ?</span>
                    </h2>
                    <p class="land-cta-sub">
                        Demandez un accès à la plateforme ou contactez-nous
                        pour une démonstration personnalisée.
                    </p>
                    <div class="land-cta-buttons">
                        <a href="mailto:acohen.tlv@gmail.com?subject=Demande%20d'accès%20Fortress" class="land-btn-primary">
                            ✉️ Contactez-nous
                        </a>
                        <a href="#/login" class="land-btn-secondary">
                            Se connecter →
                        </a>
                    </div>
                </div>
            </section>

            <!-- ═══════ Footer ═══════ -->
            <footer class="land-footer">
                <p>
                    🏰 Fortress — Intelligence commerciale B2B · France
                    <br>
                    <span style="font-size:0.8rem">
                        Données publiques SIRENE (INSEE) · Enrichissement Google Maps
                    </span>
                </p>
            </footer>
        </div>
    `;

    // ── Scroll-triggered reveal animations ───────────────────────
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                entry.target.classList.add('visible');
            }
        });
    }, { threshold: 0.1, rootMargin: '0px 0px -40px 0px' });

    container.querySelectorAll('.land-reveal').forEach(el => observer.observe(el));

    // ── Smooth scroll for nav links ──────────────────────────────
    container.querySelectorAll('[data-scroll]').forEach(link => {
        link.addEventListener('click', (e) => {
            const targetId = link.dataset.scroll;
            const targetEl = document.getElementById('section-' + targetId);
            if (targetEl) {
                e.preventDefault();
                targetEl.scrollIntoView({ behavior: 'smooth', block: 'start' });
            }
        });
    });

    // ── Animated counters ────────────────────────────────────────
    const counterObserver = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (!entry.isIntersecting) return;
            const el = entry.target;
            if (el.dataset.animated) return;
            el.dataset.animated = 'true';

            const targetText = el.textContent.trim();
            const countTo = parseInt(el.dataset.count);
            if (!countTo) return;

            const suffix = el.dataset.suffix || '';
            const displayVal = el.dataset.display;
            const duration = 2000;
            const start = Date.now();

            function animate() {
                const elapsed = Date.now() - start;
                const progress = Math.min(elapsed / duration, 1);
                // Ease out cubic
                const eased = 1 - Math.pow(1 - progress, 3);

                if (displayVal) {
                    // For values like "14.7" — animate to the display value
                    const current = (parseFloat(displayVal) * eased).toFixed(1);
                    el.textContent = current + suffix;
                } else {
                    const current = Math.floor(countTo * eased);
                    el.textContent = current + suffix;
                }

                if (progress < 1) {
                    requestAnimationFrame(animate);
                } else {
                    el.textContent = (displayVal || countTo) + suffix;
                }
            }

            animate();
            counterObserver.unobserve(el);
        });
    }, { threshold: 0.5 });

    container.querySelectorAll('[data-count]').forEach(el => counterObserver.observe(el));

    // ── Nav background on scroll ─────────────────────────────────
    let handleScroll = null;
    const nav = container.querySelector('.land-nav');
    if (nav) {
        handleScroll = () => {
            if (window.scrollY > 50) {
                nav.style.background = 'rgba(10, 10, 15, 0.95)';
            } else {
                nav.style.background = 'rgba(10, 10, 15, 0.8)';
            }
        };
        window.addEventListener('scroll', handleScroll, { passive: true });
    }

    // ── Register SPA cleanup ─────────────────────────────────────
    registerCleanup(() => {
        if (handleScroll) window.removeEventListener('scroll', handleScroll);
        observer.disconnect();
        counterObserver.disconnect();
    });
}
