/**
 * Client Upload Page — drag & drop CSV for BLUE triage dedup
 *
 * Allows the client to upload their existing CRM database as a CSV.
 * Uploaded SIRENs are stored in client_sirens and used by triage
 * to classify companies the client already owns as BLUE (skip).
 */

import { uploadClientCSV, getClientStats, clearClientSirens } from '../api.js';
import { breadcrumb, formatDateTime, escapeHtml, showConfirmModal, showToast } from '../components.js';

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
    let pendingFile = null;

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
        if (file) showPreview(file, resultDiv, container);
    });

    fileInput.addEventListener('change', () => {
        if (fileInput.files[0]) showPreview(fileInput.files[0], resultDiv, container);
    });

    // ── Clear button ─────────────────────────────────────────
    const btnClear = document.getElementById('btn-clear');
    if (btnClear) {
        btnClear.addEventListener('click', () => {
            showConfirmModal({
                title: '🗑️ Réinitialiser la base client ?',
                body: `
                    <p><strong>${totalSirens.toLocaleString('fr-FR')}</strong> SIRENs seront supprimés.</p>
                    <p style="color:var(--warning)">⚠️ Cette action est irréversible.</p>
                    <p>Les entreprises correspondantes ne seront plus marquées 🔵 BLEU au triage.</p>
                `,
                confirmLabel: 'Supprimer tout',
                danger: true,
                onConfirm: async () => {
                    const result = await clearClientSirens();
                    if (result && result.status === 'ok') {
                        showToast('Base client réinitialisée', 'success');
                        await renderUpload(container);
                    } else {
                        showToast('Erreur lors de la suppression', 'error');
                    }
                },
            });
        });
    }
}


// ── CSV Preview — parse client-side and show table ───────────────
function showPreview(file, resultDiv, container) {
    resultDiv.style.display = 'block';
    resultDiv.innerHTML = `
        <div style="display:flex; align-items:center; gap:var(--space-md); padding:var(--space-md); background:var(--bg-tertiary); border-radius:var(--radius-md)">
            <div class="spinner" style="width:18px; height:18px"></div>
            <span>Lecture de <strong>${escapeHtml(file.name)}</strong>…</span>
        </div>
    `;

    const reader = new FileReader();
    reader.onload = () => {
        const text = reader.result;
        const parsed = parseCSV(text);

        if (!parsed || parsed.headers.length === 0) {
            resultDiv.innerHTML = `
                <div style="padding:var(--space-lg); background:rgba(239,68,68,0.1); border:1px solid var(--danger); border-radius:var(--radius-md)">
                    <div style="font-weight:700; color:var(--danger)">❌ Fichier illisible</div>
                    <div style="margin-top:var(--space-sm); color:var(--text-secondary)">Impossible de lire les colonnes du CSV.</div>
                </div>
            `;
            return;
        }

        // Find the SIREN column
        const sirenColIdx = parsed.headers.findIndex(h =>
            h.toUpperCase().replace(/[^A-Z]/g, '') === 'SIREN'
        );
        const hasSirenCol = sirenColIdx >= 0;

        // Count valid SIRENs (9-digit numbers)
        let sirenCount = 0;
        if (hasSirenCol) {
            for (const row of parsed.rows) {
                const val = (row[sirenColIdx] || '').replace(/\s/g, '');
                if (/^\d{9}$/.test(val)) sirenCount++;
            }
        }

        const previewRows = parsed.rows.slice(0, 5);
        const fileSize = file.size < 1024 ? `${file.size} o` :
            file.size < 1048576 ? `${(file.size / 1024).toFixed(1)} Ko` :
            `${(file.size / 1048576).toFixed(1)} Mo`;

        resultDiv.innerHTML = `
            <div style="padding:var(--space-lg); background:var(--bg-tertiary); border-radius:var(--radius-md); border:1px solid var(--border-subtle)">
                <!-- File info -->
                <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:var(--space-lg); flex-wrap:wrap; gap:var(--space-md)">
                    <div>
                        <div style="font-weight:700; font-size:var(--font-md)">📄 ${escapeHtml(file.name)}</div>
                        <div style="font-size:var(--font-xs); color:var(--text-muted); margin-top:2px">
                            ${fileSize} · ${parsed.rows.length.toLocaleString('fr-FR')} lignes · ${parsed.headers.length} colonnes
                            · Délimiteur: <code>${escapeHtml(parsed.delimiter)}</code>
                        </div>
                    </div>
                    <div style="display:flex; align-items:center; gap:var(--space-sm)">
                        ${hasSirenCol
                            ? `<span class="badge badge-success">✅ Colonne SIREN détectée</span>
                               <span class="badge badge-accent">${sirenCount.toLocaleString('fr-FR')} SIRENs valides</span>`
                            : `<span class="badge badge-danger">❌ Colonne SIREN introuvable</span>`
                        }
                    </div>
                </div>

                <!-- Column headers preview -->
                <div style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-sm)">
                    Aperçu (${Math.min(5, previewRows.length)} premières lignes)
                </div>

                <div style="overflow-x:auto; border-radius:var(--radius-sm); border:1px solid var(--border-subtle)">
                    <table style="width:100%; border-collapse:collapse; font-size:var(--font-xs); white-space:nowrap">
                        <thead>
                            <tr>
                                ${parsed.headers.map((h, i) => `
                                    <th style="
                                        padding:var(--space-sm) var(--space-md);
                                        text-align:left;
                                        border-bottom:2px solid var(--border-color);
                                        background:${i === sirenColIdx ? 'rgba(16,185,129,0.15)' : 'var(--bg-elevated)'};
                                        color:${i === sirenColIdx ? 'var(--success)' : 'var(--text-muted)'};
                                        font-weight:700;
                                    ">${escapeHtml(h)}${i === sirenColIdx ? ' ✓' : ''}</th>
                                `).join('')}
                            </tr>
                        </thead>
                        <tbody>
                            ${previewRows.map(row => `
                                <tr>
                                    ${parsed.headers.map((_, i) => `
                                        <td style="
                                            padding:var(--space-xs) var(--space-md);
                                            border-bottom:1px solid var(--border-subtle);
                                            background:${i === sirenColIdx ? 'rgba(16,185,129,0.05)' : 'transparent'};
                                            color:var(--text-secondary);
                                            max-width:200px;
                                            overflow:hidden;
                                            text-overflow:ellipsis;
                                        ">${escapeHtml(row[i] || '—')}</td>
                                    `).join('')}
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>

                ${parsed.rows.length > 5 ? `
                    <div style="text-align:center; font-size:var(--font-xs); color:var(--text-muted); margin-top:var(--space-sm)">
                        … et ${(parsed.rows.length - 5).toLocaleString('fr-FR')} autres lignes
                    </div>
                ` : ''}

                <!-- Action buttons -->
                <div style="display:flex; justify-content:flex-end; gap:var(--space-md); margin-top:var(--space-lg)">
                    <button class="btn btn-secondary" id="btn-cancel-preview">Annuler</button>
                    ${hasSirenCol ? `
                        <button class="btn btn-primary" id="btn-confirm-upload">
                            📤 Importer ${sirenCount.toLocaleString('fr-FR')} SIRENs
                        </button>
                    ` : `
                        <button class="btn btn-primary" disabled title="Aucune colonne SIREN détectée">
                            📤 Importer
                        </button>
                    `}
                </div>
            </div>
        `;

        // Cancel preview
        document.getElementById('btn-cancel-preview').addEventListener('click', () => {
            resultDiv.style.display = 'none';
            resultDiv.innerHTML = '';
        });

        // Confirm upload
        const confirmBtn = document.getElementById('btn-confirm-upload');
        if (confirmBtn) {
            confirmBtn.addEventListener('click', async () => {
                await doUpload(file, resultDiv, container);
            });
        }
    };
    reader.readAsText(file);
}


// ── Lightweight CSV parser ───────────────────────────────────────
function parseCSV(text) {
    if (!text || text.trim().length === 0) return null;

    // Detect delimiter
    const firstLine = text.split('\n')[0] || '';
    const semicolons = (firstLine.match(/;/g) || []).length;
    const commas = (firstLine.match(/,/g) || []).length;
    const tabs = (firstLine.match(/\t/g) || []).length;
    let delimiter = ',';
    if (semicolons > commas && semicolons > tabs) delimiter = ';';
    else if (tabs > commas && tabs > semicolons) delimiter = '\\t';

    const lines = text.split('\n').filter(l => l.trim().length > 0);
    if (lines.length === 0) return null;

    const split = (line) => {
        const d = delimiter === '\\t' ? '\t' : delimiter;
        // Simple split — handles most CSVs without quoted fields
        return line.split(d).map(cell => cell.replace(/^["']|["']$/g, '').trim());
    };

    const headers = split(lines[0]);
    const rows = lines.slice(1).map(l => split(l));

    return { headers, rows, delimiter };
}


// ── Actual upload after confirmation ─────────────────────────────
async function doUpload(file, resultDiv, container) {
    resultDiv.innerHTML = `
        <div style="padding:var(--space-md); background:var(--bg-tertiary); border-radius:var(--radius-md)">
            <div style="display:flex; align-items:center; gap:var(--space-md); margin-bottom:var(--space-md)">
                <div class="spinner" style="width:20px; height:20px"></div>
                <span>Import de <strong>${escapeHtml(file.name)}</strong>…</span>
            </div>
            <div class="progress-bar" style="height:6px">
                <div class="progress-bar-fill animated" style="width:80%"></div>
            </div>
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
