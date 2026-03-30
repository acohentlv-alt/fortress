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
import { t, getLang } from '../i18n.js';

export function renderIntro(container) {
    // Remove page-container constraints for full-bleed landing
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
    document.body.style.background = '#0a0a0f';

    container.innerHTML = `
        <div class="landing">
            <!-- ═══════ Fixed Navigation ═══════ -->
            <nav class="land-nav">
                <div class="land-nav-left">
                    <a href="#/intro" class="land-nav-logo">
                        <span class="land-nav-logo-icon">🏰</span>
                        <span>Fortress</span>
                    </a>
                    <a href="#section-how" class="land-nav-link" data-scroll="how">${t('intro.navHow')}</a>
                    <a href="#section-features" class="land-nav-link" data-scroll="features">${t('intro.navFeatures')}</a>
                    <a href="#section-contact" class="land-nav-link" data-scroll="contact">${t('intro.navContact')}</a>
                </div>
                <div class="land-nav-right">
                    <a href="#/login" class="land-nav-cta">${t('intro.navLogin')}</a>
                    <button class="land-lang-toggle" onclick="window.__toggleLang && window.__toggleLang()"
                        style="background:rgba(255,255,255,0.08); border:1px solid rgba(255,255,255,0.15); color:#f0f0f5; padding:4px 14px; border-radius:8px; font-size:13px; font-weight:700; cursor:pointer; letter-spacing:0.5px; transition:background 0.2s;"
                        onmouseover="this.style.background='rgba(255,255,255,0.15)'"
                        onmouseout="this.style.background='rgba(255,255,255,0.08)'"
                    >${getLang() === 'fr' ? 'EN' : 'FR'}</button>
                </div>
            </nav>

            <!-- ═══════ Hero Section ═══════ -->
            <section class="land-hero">
                <div class="land-grid-overlay"></div>
                ${Array.from({length: 8}, (_, i) => `<div class="land-particle"></div>`).join('')}

                <div class="land-hero-content">
                    <div class="land-badge">
                        <span class="land-badge-dot"></span>
                        ${t('intro.badge')}
                    </div>

                    <h1 class="land-h1">
                        ${t('intro.h1Line1')}<br>
                        <span class="land-h1-gradient">${t('intro.h1Line2')}</span>
                    </h1>

                    <p class="land-hero-sub">
                        ${t('intro.heroParagraph')}
                    </p>

                    <div class="land-hero-ctas">
                        <a href="#section-contact" class="land-btn-primary" data-scroll="contact">
                            ${t('intro.ctaRequest')}
                        </a>
                        <a href="#section-how" class="land-btn-secondary" data-scroll="how">
                            ${t('intro.ctaDiscover')}
                        </a>
                    </div>
                </div>
            </section>

            <!-- ═══════ Stats Bar ═══════ -->
            <section class="land-section land-section-centered">
                <div class="land-stats-bar land-reveal">
                    <div class="land-stat">
                        <div class="land-stat-value" data-count="14700000" data-suffix="M+" data-display="14.7">14.7M+</div>
                        <div class="land-stat-label">${t('intro.statCompanies')}</div>
                    </div>
                    <div class="land-stat">
                        <div class="land-stat-value" data-count="86" data-suffix="%">86%</div>
                        <div class="land-stat-label">${t('intro.statPhoneRate')}</div>
                    </div>
                    <div class="land-stat">
                        <div class="land-stat-value" data-count="101" data-suffix="" data-display="101">101</div>
                        <div class="land-stat-label">${t('intro.statDepts')}</div>
                    </div>
                    <div class="land-stat">
                        <div class="land-stat-value">${t('intro.statRealtime')}</div>
                        <div class="land-stat-label">${t('intro.statUpdated')}</div>
                    </div>
                </div>
            </section>

            <!-- ═══════ How It Works ═══════ -->
            <section class="land-section" id="section-how">
                <div class="land-section-label land-reveal">${t('intro.howLabel')}</div>
                <h2 class="land-section-title land-reveal land-reveal-delay-1">
                    ${t('intro.howTitle1')}<br>${t('intro.howTitle2')} <span class="land-h1-gradient">${t('intro.howTitle3')}</span>
                </h2>
                <p class="land-section-sub land-reveal land-reveal-delay-2">
                    ${t('intro.howSub')}
                </p>

                <div class="land-steps">
                    <div class="land-step land-reveal land-reveal-delay-1">
                        <div class="land-step-num">1</div>
                        <div class="land-step-title">${t('intro.step1Title')}</div>
                        <div class="land-step-desc">
                            ${t('intro.step1Desc')}
                        </div>
                        <div class="land-step-tags">
                            <span class="land-step-tag">${t('intro.step1Tag1')}</span>
                            <span class="land-step-tag">${t('intro.step1Tag2')}</span>
                            <span class="land-step-tag">${t('intro.step1Tag3')}</span>
                        </div>
                    </div>

                    <div class="land-step land-reveal land-reveal-delay-2">
                        <div class="land-step-num">2</div>
                        <div class="land-step-title">${t('intro.step2Title')}</div>
                        <div class="land-step-desc">
                            ${t('intro.step2Desc')}
                        </div>
                        <div class="land-step-tags">
                            <span class="land-step-tag">${t('intro.step2Tag1')}</span>
                            <span class="land-step-tag">${t('intro.step2Tag2')}</span>
                            <span class="land-step-tag">${t('intro.step2Tag3')}</span>
                        </div>
                    </div>

                    <div class="land-step land-reveal land-reveal-delay-3">
                        <div class="land-step-num">3</div>
                        <div class="land-step-title">${t('intro.step3Title')}</div>
                        <div class="land-step-desc">
                            ${t('intro.step3Desc')}
                        </div>
                        <div class="land-step-tags">
                            <span class="land-step-tag">${t('intro.step3Tag1')}</span>
                            <span class="land-step-tag">${t('intro.step3Tag2')}</span>
                            <span class="land-step-tag">${t('intro.step3Tag3')}</span>
                        </div>
                    </div>
                </div>
            </section>

            <!-- ═══════ Feature Bento Grid ═══════ -->
            <section class="land-section" id="section-features">
                <div class="land-section-label land-reveal">${t('intro.featuresLabel')}</div>
                <h2 class="land-section-title land-reveal land-reveal-delay-1">
                    ${t('intro.featuresTitle1')}<br>${t('intro.featuresTitle2')} <span class="land-h1-gradient">${t('intro.featuresTitle3')}</span>
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
                        <div class="land-bento-title">${t('intro.bentoDataTitle')}</div>
                        <div class="land-bento-desc">
                            ${t('intro.bentoDataDesc')}
                        </div>
                    </div>

                    <!-- Tall card: Contact data -->
                    <div class="land-bento-card land-bento-tall land-reveal land-reveal-delay-2">
                        <div class="land-bento-icon">📞</div>
                        <div class="land-bento-title">${t('intro.bentoContactTitle')}</div>
                        <div class="land-bento-desc">
                            ${t('intro.bentoContactDesc')}
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
                        <div class="land-bento-title">${t('intro.bentoSocialTitle')}</div>
                        <div class="land-bento-desc">
                            ${t('intro.bentoSocialDesc')}
                        </div>
                    </div>

                    <!-- Standard card: Directors -->
                    <div class="land-bento-card land-reveal land-reveal-delay-2">
                        <div class="land-bento-icon">👥</div>
                        <div class="land-bento-title">${t('intro.bentoDirectorsTitle')}</div>
                        <div class="land-bento-desc">
                            ${t('intro.bentoDirectorsDesc')}
                        </div>
                    </div>

                    <!-- Wide card: SIRENE -->
                    <div class="land-bento-card land-bento-wide land-reveal land-reveal-delay-3">
                        <div class="land-bento-icon">🏛️</div>
                        <div class="land-bento-title">${t('intro.bentoSireneTitle')}</div>
                        <div class="land-bento-desc">
                            ${t('intro.bentoSireneDesc')}
                        </div>
                    </div>

                    <!-- Standard card: Maps -->
                    <div class="land-bento-card land-reveal land-reveal-delay-1">
                        <div class="land-bento-icon">📍</div>
                        <div class="land-bento-title">${t('intro.bentoMapsTitle')}</div>
                        <div class="land-bento-desc">
                            ${t('intro.bentoMapsDesc')}
                        </div>
                    </div>

                    <!-- Standard card: Export -->
                    <div class="land-bento-card land-reveal land-reveal-delay-2">
                        <div class="land-bento-icon">📊</div>
                        <div class="land-bento-title">${t('intro.bentoExportTitle')}</div>
                        <div class="land-bento-desc">
                            ${t('intro.bentoExportDesc')}
                        </div>
                    </div>

                    <!-- Standard card: Multi-user -->
                    <div class="land-bento-card land-reveal land-reveal-delay-3">
                        <div class="land-bento-icon">🔐</div>
                        <div class="land-bento-title">${t('intro.bentoMultiTitle')}</div>
                        <div class="land-bento-desc">
                            ${t('intro.bentoMultiDesc')}
                        </div>
                    </div>
                </div>
            </section>

            <!-- ═══════ Trust Indicators ═══════ -->
            <div class="land-trust land-reveal">
                <div class="land-trust-item">
                    <span class="land-trust-icon">🇫🇷</span>
                    <span class="land-trust-label">${t('intro.trustFrench')}</span>
                </div>
                <div class="land-trust-item">
                    <span class="land-trust-icon">🔒</span>
                    <span class="land-trust-label">${t('intro.trustSecure')}</span>
                </div>
                <div class="land-trust-item">
                    <span class="land-trust-icon">⚡</span>
                    <span class="land-trust-label">${t('intro.trustFast')}</span>
                </div>
                <div class="land-trust-item">
                    <span class="land-trust-icon">🏛️</span>
                    <span class="land-trust-label">${t('intro.trustInsee')}</span>
                </div>
                <div class="land-trust-item">
                    <span class="land-trust-icon">📋</span>
                    <span class="land-trust-label">${t('intro.trustRgpd')}</span>
                </div>
            </div>

            <!-- ═══════ CTA / Contact Section ═══════ -->
            <section class="land-cta-section" id="section-contact">
                <div class="land-cta-content land-reveal">
                    <h2 class="land-cta-title">
                        ${t('intro.ctaTitle1')}<br>
                        <span class="land-h1-gradient">${t('intro.ctaTitle2')}</span>
                    </h2>
                    <p class="land-cta-sub">
                        ${t('intro.ctaSub')}
                    </p>

                    <!-- Contact Form -->
                    <form id="land-contact-form" class="land-form" autocomplete="on">
                        <div class="land-form-row">
                            <div class="land-form-field">
                                <label class="land-form-label" for="land-name">${t('intro.formNameLabel')}</label>
                                <input type="text" id="land-name" class="land-form-input"
                                    placeholder="${t('intro.formNamePlaceholder')}" required maxlength="200" autocomplete="name">
                            </div>
                            <div class="land-form-field">
                                <label class="land-form-label" for="land-email">${t('intro.formEmailLabel')}</label>
                                <input type="email" id="land-email" class="land-form-input"
                                    placeholder="${t('intro.formEmailPlaceholder')}" required maxlength="200" autocomplete="email">
                            </div>
                        </div>
                        <div class="land-form-field">
                            <label class="land-form-label" for="land-company">${t('intro.formCompanyLabel')}</label>
                            <input type="text" id="land-company" class="land-form-input"
                                placeholder="${t('intro.formCompanyPlaceholder')}" maxlength="200" autocomplete="organization">
                        </div>
                        <div class="land-form-field">
                            <label class="land-form-label" for="land-message">${t('intro.formMessageLabel')}</label>
                            <textarea id="land-message" class="land-form-input land-form-textarea"
                                placeholder="${t('intro.formMessagePlaceholder')}"
                                required maxlength="2000" rows="4"></textarea>
                        </div>
                        <div class="land-form-field land-consent-field">
                            <label class="land-consent-label">
                                <input type="checkbox" id="land-consent" class="land-consent-checkbox" required>
                                <span>J'accepte que mes données soient traitées conformément à la <a href="#/legal" class="land-consent-link">politique de confidentialité</a></span>
                            </label>
                        </div>
                        <div class="land-form-actions">
                            <button type="submit" id="land-submit" class="land-btn-primary" style="width:100%" disabled>
                                ${t('intro.formSubmit')}
                            </button>
                        </div>
                        <div id="land-form-status" class="land-form-status" style="display:none"></div>
                    </form>

                    <div style="margin-top:var(--space-xl, 24px)">
                        <a href="#/login" class="land-btn-secondary">
                            ${t('intro.alreadyAccount')}
                        </a>
                    </div>
                </div>
            </section>

            <!-- ═══════ Footer ═══════ -->
            <footer class="land-footer">
                <p>
                    ${t('intro.footerText')}
                    <br>
                    <span style="font-size:0.8rem">
                        ${t('intro.footerSub')}
                    </span>
                </p>
                <div class="land-footer-links">
                    <a href="#/legal">Mentions légales</a>
                    <span class="land-footer-sep">|</span>
                    <a href="#/legal">Politique de confidentialité</a>
                </div>
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


    // ── Consent checkbox — enable/disable submit button ──────────
    const consentCheckbox = document.getElementById('land-consent');
    const submitBtn = document.getElementById('land-submit');
    if (consentCheckbox && submitBtn) {
        consentCheckbox.addEventListener('change', () => {
            submitBtn.disabled = !consentCheckbox.checked;
        });
    }

    // ── Contact form submission ──────────────────────────────────
    const contactForm = document.getElementById('land-contact-form');
    if (contactForm) {
        contactForm.addEventListener('submit', async (e) => {
            e.preventDefault();
            if (!document.getElementById('land-consent')?.checked) return;
            const btn = document.getElementById('land-submit');
            const statusEl = document.getElementById('land-form-status');

            const name = document.getElementById('land-name').value.trim();
            const email = document.getElementById('land-email').value.trim();
            const company = document.getElementById('land-company').value.trim();
            const message = document.getElementById('land-message').value.trim();

            if (!name || !email || !message) return;

            btn.disabled = true;
            btn.textContent = t('intro.formSending');
            statusEl.style.display = 'none';

            try {
                const res = await fetch('/api/contact', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ name, email, company, message })
                });
                const data = await res.json();

                if (res.ok && data.ok) {
                    statusEl.className = 'land-form-status land-form-success';
                    statusEl.innerHTML = '✅ ' + (data.message || t('intro.formSent'));
                    statusEl.style.display = 'block';
                    contactForm.reset();
                    if (submitBtn) submitBtn.disabled = true;
                    btn.textContent = t('intro.formSent');
                    // Re-enable after 5s (user must re-check consent)
                    setTimeout(() => {
                        btn.textContent = t('intro.formSubmit');
                    }, 5000);
                } else {
                    const errMsg = data.detail || data.message || t('intro.formError');
                    statusEl.className = 'land-form-status land-form-error';
                    statusEl.innerHTML = '❌ ' + errMsg;
                    statusEl.style.display = 'block';
                    btn.disabled = false;
                    btn.textContent = t('intro.formSubmit');
                }
            } catch (err) {
                statusEl.className = 'land-form-status land-form-error';
                statusEl.innerHTML = '❌ ' + t('intro.formNetworkError');
                statusEl.style.display = 'block';
                btn.disabled = false;
                btn.textContent = t('intro.formSubmit');
            }
        });
    }

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
        document.body.style.background = '';
    });
}
