/**
 * Client Upload Page — drag & drop CSV for BLUE triage dedup
 *
 * Allows the client to upload their existing CRM database as a CSV.
 * Uploaded SIRENs are stored in client_sirens and used by triage
 * to classify companies the client already owns as BLUE (skip).
 */

import { uploadClientCSV, getClientStats, clearClientSirens } from '../api.js';
import { breadcrumb, formatDateTime, escapeHtml } from '../components.js';

export async function renderUpload(container) {
    // Fetch current stats
    let stats = null;
    try {
        stats = await getClientStats();
    } catch { /* DB may be offline */ }

    const totalSirens = stats?.total_sirens || 0;
    const uploads = stats?.uploads || [];

    container.innerHTML = `
        ${breadcrumb([{ label: 'Base Client' }])}

        <h1 class="page-title">📤 Base de données client</h1>
        <p class="page-subtitle">
            Importez le fichier CSV de votre CRM pour éviter de re-scraper les entreprises que vous possédez déjà.
            Les SIRENs importés seront marqués <strong style="color:var(--info)">🔵 BLEU</strong> lors du triage et automatiquement ignorés.
        </p>

        <!-- Stats Card -->
        <div class="card" style="margin-bottom:var(--space-xl); display:flex; align-items:center; justify-content:space-between; gap:var(--space-xl)">
            <div>
                <div style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-xs)">
                    SIRENs importés
                </div>
                <div style="font-size:var(--font-3xl); font-weight:800; color:var(--text-primary)" id="upload-total">
                    ${totalSirens.toLocaleString('fr-FR')}
                </div>
                <div style="font-size:var(--font-sm); color:var(--text-secondary)">
                    entreprises ignorées automatiquement
                </div>
            </div>
            ${totalSirens > 0 ? `
                <button class="btn btn-secondary" id="btn-clear" style="white-space:nowrap">
                    🗑️ Réinitialiser
                </button>
            ` : ''}
        </div>

        <!-- Upload Zone -->
        <div class="card" style="margin-bottom:var(--space-xl)">
            <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                Importer un fichier CSV
            </h3>

            <div id="drop-zone" style="
                border: 2px dashed var(--border-color);
                border-radius: var(--radius-lg);
                padding: var(--space-3xl) var(--space-xl);
                text-align: center;
                cursor: pointer;
                transition: all 0.2s ease;
                background: var(--bg-secondary);
            ">
                <div style="font-size: 3rem; margin-bottom: var(--space-md)">📁</div>
                <div style="font-size: var(--font-lg); font-weight: 600; margin-bottom: var(--space-sm)">
                    Glissez votre fichier CSV ici
                </div>
                <div style="color: var(--text-muted); font-size: var(--font-sm); margin-bottom: var(--space-lg)">
                    ou cliquez pour sélectionner un fichier
                </div>
                <div style="color: var(--text-muted); font-size: var(--font-xs)">
                    Format: CSV avec une colonne <code>SIREN</code> • Encodage: UTF-8 ou Latin-1 • Délimiteur: virgule ou point-virgule
                </div>
                <input type="file" id="file-input" accept=".csv,.txt" style="display:none">
            </div>

            <!-- Upload result -->
            <div id="upload-result" style="margin-top:var(--space-lg); display:none"></div>
        </div>

        <!-- Upload History -->
        ${uploads.length > 0 ? `
            <div class="card">
                <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-lg)">
                    Historique des imports
                </h3>
                <table style="width:100%; border-collapse:collapse; font-size:var(--font-sm)">
                    <thead>
                        <tr style="border-bottom:1px solid var(--border-color)">
                            <th style="text-align:left; padding:var(--space-sm) var(--space-md); color:var(--text-muted)">Fichier</th>
                            <th style="text-align:right; padding:var(--space-sm) var(--space-md); color:var(--text-muted)">SIRENs</th>
                            <th style="text-align:right; padding:var(--space-sm) var(--space-md); color:var(--text-muted)">Date</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${uploads.map(u => `
                            <tr style="border-bottom:1px solid var(--border-subtle)">
                                <td style="padding:var(--space-sm) var(--space-md)">${escapeHtml(u.source_file || '—')}</td>
                                <td style="text-align:right; padding:var(--space-sm) var(--space-md); font-weight:600">${(u.siren_count || 0).toLocaleString('fr-FR')}</td>
                                <td style="text-align:right; padding:var(--space-sm) var(--space-md); color:var(--text-secondary)">${formatDateTime(u.uploaded_at)}</td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        ` : ''}
    `;

    // ── Wire up drag & drop + click ──────────────────────────
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');
    const resultDiv = document.getElementById('upload-result');

    dropZone.addEventListener('click', () => fileInput.click());

    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.style.borderColor = 'var(--primary)';
        dropZone.style.background = 'var(--bg-tertiary)';
    });

    dropZone.addEventListener('dragleave', () => {
        dropZone.style.borderColor = 'var(--border-color)';
        dropZone.style.background = 'var(--bg-secondary)';
    });

    dropZone.addEventListener('drop', async (e) => {
        e.preventDefault();
        dropZone.style.borderColor = 'var(--border-color)';
        dropZone.style.background = 'var(--bg-secondary)';
        const file = e.dataTransfer.files[0];
        if (file) await handleUpload(file, resultDiv);
    });

    fileInput.addEventListener('change', async () => {
        if (fileInput.files[0]) await handleUpload(fileInput.files[0], resultDiv);
    });

    // ── Clear button ─────────────────────────────────────────
    const btnClear = document.getElementById('btn-clear');
    if (btnClear) {
        btnClear.addEventListener('click', async () => {
            if (!confirm('Supprimer tous les SIRENs importés ? Cette action est irréversible.')) return;
            btnClear.disabled = true;
            btnClear.textContent = '⏳ Suppression...';
            const result = await clearClientSirens();
            if (result && result.status === 'ok') {
                // Reload the page
                await renderUpload(container);
            } else {
                btnClear.disabled = false;
                btnClear.textContent = '🗑️ Réinitialiser';
                alert('Erreur lors de la suppression.');
            }
        });
    }
}


async function handleUpload(file, resultDiv) {
    resultDiv.style.display = 'block';
    resultDiv.innerHTML = `
        <div style="display:flex; align-items:center; gap:var(--space-md); padding:var(--space-md); background:var(--bg-tertiary); border-radius:var(--radius-md)">
            <div class="spinner" style="width:20px; height:20px"></div>
            <span>Analyse de <strong>${escapeHtml(file.name)}</strong>...</span>
        </div>
    `;

    const result = await uploadClientCSV(file);

    if (result && result.status === 'ok') {
        resultDiv.innerHTML = `
            <div style="padding:var(--space-lg); background:var(--success-bg, rgba(16,185,129,0.1)); border:1px solid var(--success, #10b981); border-radius:var(--radius-md)">
                <div style="font-weight:700; font-size:var(--font-lg); margin-bottom:var(--space-sm); color:var(--success, #10b981)">
                    ✅ Import réussi
                </div>
                <div style="display:grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap:var(--space-md); margin-top:var(--space-md)">
                    <div>
                        <div style="font-size:var(--font-xs); color:var(--text-muted)">Fichier</div>
                        <div style="font-weight:600">${escapeHtml(result.filename)}</div>
                    </div>
                    <div>
                        <div style="font-size:var(--font-xs); color:var(--text-muted)">Lignes</div>
                        <div style="font-weight:600">${result.total_rows}</div>
                    </div>
                    <div>
                        <div style="font-size:var(--font-xs); color:var(--text-muted)">SIRENs valides</div>
                        <div style="font-weight:600">${result.valid_sirens}</div>
                    </div>
                    <div>
                        <div style="font-size:var(--font-xs); color:var(--text-muted)">Nouveaux</div>
                        <div style="font-weight:600; color:var(--success, #10b981)">${result.inserted}</div>
                    </div>
                    <div>
                        <div style="font-size:var(--font-xs); color:var(--text-muted)">Déjà importés</div>
                        <div style="font-weight:600; color:var(--text-muted)">${result.already_existed}</div>
                    </div>
                </div>
            </div>
        `;
        // Update total count
        const $total = document.getElementById('upload-total');
        if ($total) {
            const newStats = await getClientStats();
            if (newStats) $total.textContent = (newStats.total_sirens || 0).toLocaleString('fr-FR');
        }
    } else {
        const errorMsg = result?.error || 'Erreur inconnue';
        resultDiv.innerHTML = `
            <div style="padding:var(--space-lg); background:rgba(239,68,68,0.1); border:1px solid var(--danger, #ef4444); border-radius:var(--radius-md)">
                <div style="font-weight:700; color:var(--danger, #ef4444)">❌ Erreur d'import</div>
                <div style="margin-top:var(--space-sm); color:var(--text-secondary)">${escapeHtml(errorMsg)}</div>
            </div>
        `;
    }
}
