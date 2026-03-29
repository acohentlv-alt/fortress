/**
 * Fortress i18n — lightweight internationalization following i18next conventions.
 * Supports: nested keys, {{variable}} interpolation, _one/_other pluralization.
 */

let _translations = {};
let _lang = 'fr';
let _ready = false;
let _onLanguageChange = null;

/**
 * Initialize i18n: load translation files and set language.
 */
export async function initI18n(lang) {
    _lang = lang || localStorage.getItem('fortress_language') || 'fr';

    // Load both languages
    const [fr, en] = await Promise.all([
        fetch('/translations/fr.json').then(r => r.json()),
        fetch('/translations/en.json').then(r => r.json()),
    ]);
    _translations = { fr, en };
    _ready = true;

    document.documentElement.lang = _lang;
    _updateToggle();
    translateDOM();
}

/**
 * Translate a key. Supports:
 * - Nested keys: t('dashboard.title')
 * - Interpolation: t('greeting', { name: 'Alan' }) → "Hello Alan"
 * - Pluralization: t('company', { count: 5 }) uses key "company_one" or "company_other"
 */
export function t(key, params) {
    if (!_ready) return key;

    const dict = _translations[_lang] || _translations['fr'] || {};
    let value = _resolve(dict, key);

    // Pluralization: if params.count exists, try _one/_other suffixes
    if (params && typeof params.count === 'number' && value === undefined) {
        const suffix = params.count === 1 ? '_one' : '_other';
        value = _resolve(dict, key + suffix);
    }

    if (value === undefined) {
        // Fallback to French
        const frDict = _translations['fr'] || {};
        value = _resolve(frDict, key);
        if (params && typeof params.count === 'number' && value === undefined) {
            const suffix = params.count === 1 ? '_one' : '_other';
            value = _resolve(frDict, key + suffix);
        }
    }

    if (value === undefined) return key; // Return raw key as last resort

    // Interpolation: replace {{var}} with params.var
    if (params) {
        value = value.replace(/\{\{(\w+)\}\}/g, (_, k) =>
            params[k] !== undefined ? String(params[k]) : `{{${k}}}`
        );
    }

    return value;
}

/**
 * Get current language code.
 */
export function getLang() {
    return _lang;
}

/**
 * Change language and re-render the current page.
 */
export function changeLanguage(lang) {
    _lang = lang;
    localStorage.setItem('fortress_language', lang);
    document.documentElement.lang = lang;
    _updateToggle();
    translateDOM();
    // Call registered callback to re-render the page
    if (_onLanguageChange) _onLanguageChange();
}

/**
 * Register a callback that fires after language change (used by app.js to re-render).
 */
export function onLanguageChange(fn) {
    _onLanguageChange = fn;
}

/**
 * Update all DOM elements with data-i18n / data-i18n-title attributes.
 */
export function translateDOM() {
    document.querySelectorAll('[data-i18n]').forEach(el => {
        const key = el.getAttribute('data-i18n');
        el.textContent = t(key);
    });
    document.querySelectorAll('[data-i18n-title]').forEach(el => {
        const key = el.getAttribute('data-i18n-title');
        el.title = t(key);
    });
}

/**
 * Resolve a dotted key like "dashboard.title" from a nested object.
 */
function _resolve(obj, key) {
    const parts = key.split('.');
    let current = obj;
    for (const part of parts) {
        if (current == null || typeof current !== 'object') return undefined;
        current = current[part];
    }
    return typeof current === 'string' ? current : undefined;
}

/**
 * Update the language toggle button appearance.
 */
function _updateToggle() {
    const btn = document.getElementById('lang-toggle');
    if (btn) {
        btn.textContent = _lang === 'fr' ? 'EN' : 'FR';
        btn.title = _lang === 'fr' ? 'Switch to English' : 'Passer en français';
    }
}
