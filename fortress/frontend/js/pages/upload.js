/**
 * Import / Export Page — Smart Upload Engine
 *
 * 3-step flow:
 * 1. Drag & drop / select file
 * 2. Mapping preview: show recognized vs overflow columns
 * 3. Confirm: ingest with progress banner
 *
 * Each upload creates a batch_data entry (mode='upload') visible in Monitor.
 */

import { previewUpload, uploadClientFile, getClientStats } from '../api.js';
import { breadcrumb, formatDateTime, escapeHtml, showToast } from '../components.js';

export async function renderUpload(container) {
    // Fetch upload history
    let stats = null;
    try {
        stats = await getClientStats();
    } catch { /* DB may be offline */ }

    const uploads = stats?.uploads || [];

    container.innerHTML = `
        ${breadcrumb([{ label: 'Import / Export' }])}

        <h1 class="page-title">📤 Import / Export</h1>
        <p class="page-subtitle" style="max-width:600px">
            Importez un fichier CSV ou XLSX. Le système détecte automatiquement les colonnes
            et ingère les données dans les entreprises, contacts et dirigeants.
        </p>

        <!-- Upload Zone -->
        <div class="card" style="margin-bottom:var(--space-xl)">
            <div id="drop-zone" style="
                border: 2px dashed var(--border-color);
                border-radius: var(--radius-lg);
                padding: var(--space-3xl, 48px) var(--space-xl);
                text-align: center;
                cursor: pointer;
                transition: all 0.2s ease;
                background: var(--bg-secondary);
            ">
                <div style="font-size: 2.5rem; margin-bottom: var(--space-md)">📁</div>
                <div style="font-size: var(--font-md); font-weight: 600; margin-bottom: var(--space-xs)">
                    Glissez votre fichier CSV ou XLSX ici
                </div>
                <div style="color: var(--text-muted); font-size: var(--font-sm)">
                    ou cliquez pour sélectionner un fichier
                </div>
                <input type="file" id="file-input" accept=".csv,.txt,.xlsx,.xls" style="display:none">
            </div>

            <!-- Preview / Result area -->
            <div id="upload-result" style="margin-top:var(--space-lg); display:none"></div>
        </div>

        <!-- Upload History -->
        ${uploads.length > 0 ? _renderHistory(uploads) : ''}
    `;

    // ── Drop zone interactions ────────────────────────────────
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-input');
    const resultDiv = document.getElementById('upload-result');

    dropZone.addEventListener('click', () => fileInput.click());

    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.style.borderColor = 'var(--accent)';
        dropZone.style.background = 'var(--accent-subtle, rgba(59,130,246,0.08))';
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
        if (file) await showMappingPreview(file, resultDiv);
    });

    fileInput.addEventListener('change', async () => {
        if (fileInput.files[0]) await showMappingPreview(fileInput.files[0], resultDiv);
    });
}


// ── Step 2: Mapping Preview ──────────────────────────────────────

async function showMappingPreview(file, resultDiv) {
    resultDiv.style.display = 'block';
    resultDiv.innerHTML = `
        <div style="display:flex; align-items:center; gap:var(--space-md); padding:var(--space-md); background:var(--bg-tertiary, var(--bg-secondary)); border-radius:var(--radius-md)">
            <div class="spinner" style="width:18px; height:18px; flex-shrink:0"></div>
            <span>Analyse de <strong>${escapeHtml(file.name)}</strong>…</span>
        </div>
    `;

    const preview = await previewUpload(file);

    if (!preview._ok) {
        resultDiv.innerHTML = `
            <div style="padding:var(--space-lg); background:rgba(239,68,68,0.1); border:1px solid var(--danger, #ef4444); border-radius:var(--radius-md)">
                <div style="font-weight:700; color:var(--danger, #ef4444)">❌ Erreur d'analyse</div>
                <div style="margin-top:var(--space-sm); color:var(--text-secondary)">${escapeHtml(preview.error || 'Fichier illisible')}</div>
            </div>
        `;
        return;
    }

    const mapping = preview.mapping || {};
    const recognized = mapping.recognized || [];
    const overflow = mapping.overflow || [];

    resultDiv.innerHTML = `
        <div style="border-radius:var(--radius-md); border:1px solid var(--border-subtle); overflow:hidden">
            <!-- File info banner -->
            <div style="padding:var(--space-md) var(--space-lg); background:var(--bg-tertiary, var(--bg-secondary)); display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:var(--space-sm); border-bottom:1px solid var(--border-subtle)">
                <div>
                    <span style="font-weight:700">📄 ${escapeHtml(file.name)}</span>
                    <span style="color:var(--text-muted); font-size:var(--font-xs); margin-left:var(--space-sm)">
                        ${(preview.total_rows || 0).toLocaleString('fr-FR')} lignes · ${preview.total_columns || 0} colonnes
                    </span>
                </div>
                <div style="display:flex; gap:var(--space-xs); flex-wrap:wrap">
                    ${preview.has_siren_column
                        ? `<span class="badge badge-success" style="font-size:var(--font-xs)">✅ ${(preview.valid_sirens || 0).toLocaleString('fr-FR')} SIRENs</span>`
                        : `<span class="badge badge-danger" style="font-size:var(--font-xs)">❌ Pas de SIREN</span>`
                    }
                    ${preview.has_officer_data
                        ? `<span class="badge badge-info" style="font-size:var(--font-xs)">👤 Dirigeants</span>`
                        : ''
                    }
                </div>
            </div>

            <div style="padding:var(--space-lg)">
                <!-- Recognized columns -->
                <div style="margin-bottom:var(--space-lg)">
                    <div style="font-size:var(--font-xs); font-weight:700; color:var(--success, #10b981); text-transform:uppercase; letter-spacing:0.05em; margin-bottom:var(--space-sm)">
                        ✅ Colonnes reconnues (${mapping.recognized_count || 0})
                    </div>
                    <div style="display:grid; grid-template-columns:repeat(auto-fill, minmax(260px, 1fr)); gap:1px; background:var(--border-subtle); border-radius:var(--radius-sm); overflow:hidden; max-height:240px; overflow-y:auto">
                        ${recognized.filter(r => r.target).map(r => {
                            const parts = (r.target || '').split('.');
                            const table = parts[0] || '';
                            const field = parts[1] || '';
                            const icon = table === 'companies' ? '🏢' : table === 'contacts' ? '📞' : '👤';
                            return `<div style="padding:6px 10px; background:var(--bg-primary); display:flex; justify-content:space-between; align-items:center; font-size:var(--font-xs); gap:var(--space-sm)">
                                <span style="color:var(--text-secondary); overflow:hidden; text-overflow:ellipsis; white-space:nowrap">${escapeHtml(r.source)}</span>
                                <span style="color:var(--success, #10b981); font-weight:600; white-space:nowrap">${icon} ${field}</span>
                            </div>`;
                        }).join('')}
                    </div>
                </div>

                <!-- Overflow columns -->
                ${overflow.length > 0 ? `
                    <div style="margin-bottom:var(--space-lg)">
                        <div style="font-size:var(--font-xs); font-weight:700; color:var(--warning, #f59e0b); text-transform:uppercase; letter-spacing:0.05em; margin-bottom:var(--space-sm)">
                            📦 Données supplémentaires (${mapping.overflow_count || 0})
                        </div>
                        <div style="display:flex; flex-wrap:wrap; gap:4px">
                            ${overflow.map(o =>
                                `<span style="display:inline-block; padding:2px 8px; background:var(--bg-hover, var(--bg-secondary)); border-radius:var(--radius-sm); color:var(--text-muted); font-size:var(--font-xs)">${escapeHtml(o.source)}</span>`
                            ).join('')}
                        </div>
                        <div style="font-size:11px; color:var(--text-disabled); margin-top:4px">
                            Stockées dans la fiche entreprise et visibles sur les cartes de contact.
                        </div>
                    </div>
                ` : ''}

                <!-- Actions -->
                <div style="display:flex; gap:var(--space-md); justify-content:flex-end; padding-top:var(--space-md); border-top:1px solid var(--border-subtle)">
                    <button class="btn btn-secondary" id="btn-cancel-preview">Annuler</button>
                    ${preview.has_siren_column ? `
                        <button class="btn btn-primary" id="btn-confirm-upload">
                            📤 Importer ${(preview.valid_sirens || 0).toLocaleString('fr-FR')} entités
                        </button>
                    ` : `
                        <button class="btn btn-primary" disabled>📤 Importer</button>
                    `}
                </div>
            </div>
        </div>
    `;

    // Cancel
    document.getElementById('btn-cancel-preview')?.addEventListener('click', () => {
        resultDiv.style.display = 'none';
        resultDiv.innerHTML = '';
    });

    // Confirm upload
    document.getElementById('btn-confirm-upload')?.addEventListener('click', async () => {
        await doUpload(file, resultDiv);
    });
}


// ── Step 3: Upload with progress banner ──────────────────────────

async function doUpload(file, resultDiv) {
    // Show progress banner
    resultDiv.innerHTML = `
        <div style="border-radius:var(--radius-md); border:1px solid var(--accent, #3b82f6); overflow:hidden">
            <div style="padding:var(--space-md) var(--space-lg); background:rgba(59,130,246,0.08)">
                <div style="display:flex; align-items:center; gap:var(--space-md); margin-bottom:var(--space-md)">
                    <div class="spinner" style="width:20px; height:20px; flex-shrink:0"></div>
                    <div>
                        <div style="font-weight:700">📤 Import en cours…</div>
                        <div style="font-size:var(--font-xs); color:var(--text-muted)">${escapeHtml(file.name)}</div>
                    </div>
                </div>
                <div style="background:var(--bg-secondary); border-radius:99px; height:8px; overflow:hidden">
                    <div id="upload-progress-bar" style="
                        height:100%;
                        background:linear-gradient(90deg, var(--accent, #3b82f6), #06b6d4);
                        border-radius:99px;
                        width:10%;
                        transition:width 0.5s ease;
                        animation: pulse-bar 1.5s ease-in-out infinite;
                    "></div>
                </div>
                <div id="upload-status-text" style="font-size:var(--font-xs); color:var(--text-muted); margin-top:6px; text-align:center">
                    Lecture et analyse des colonnes…
                </div>
            </div>
        </div>
        <style>
            @keyframes pulse-bar {
                0%, 100% { opacity: 1; }
                50% { opacity: 0.7; }
            }
        </style>
    `;

    // Animate progress while waiting
    const bar = document.getElementById('upload-progress-bar');
    const statusText = document.getElementById('upload-status-text');
    let progress = 10;
    const progressInterval = setInterval(() => {
        if (progress < 85) {
            progress += Math.random() * 8;
            if (bar) bar.style.width = `${Math.min(progress, 85)}%`;
        }
        if (progress > 30 && statusText) {
            statusText.textContent = 'Ingestion des entreprises, contacts et dirigeants…';
        }
        if (progress > 60 && statusText) {
            statusText.textContent = 'Finalisation des tags et mise à jour…';
        }
    }, 600);

    const result = await uploadClientFile(file);
    clearInterval(progressInterval);

    if (bar) bar.style.width = '100%';

    if (result && result.status === 'ok') {
        const s = result.stats || {};
        const totalCompanies = (s.companies_inserted || 0) + (s.companies_updated || 0);

        resultDiv.innerHTML = `
            <div style="border-radius:var(--radius-md); border:1px solid var(--success, #10b981); overflow:hidden">
                <!-- Success banner -->
                <div style="padding:var(--space-md) var(--space-lg); background:rgba(16,185,129,0.08); display:flex; align-items:center; gap:var(--space-md); border-bottom:1px solid var(--border-subtle)">
                    <span style="font-size:1.5rem">✅</span>
                    <div>
                        <div style="font-weight:700; color:var(--success, #10b981)">Import réussi</div>
                        <div style="font-size:var(--font-xs); color:var(--text-muted)">${escapeHtml(result.filename || '')}</div>
                    </div>
                </div>

                <!-- Stats grid -->
                <div style="padding:var(--space-lg); display:grid; grid-template-columns: repeat(auto-fit, minmax(100px, 1fr)); gap:var(--space-lg); text-align:center">
                    <div>
                        <div style="font-size:var(--font-2xl, 1.5rem); font-weight:800; color:var(--text-primary)">${(result.total_rows || 0).toLocaleString('fr-FR')}</div>
                        <div style="font-size:var(--font-xs); color:var(--text-muted)">Lignes lues</div>
                    </div>
                    <div>
                        <div style="font-size:var(--font-2xl, 1.5rem); font-weight:800; color:var(--success, #10b981)">${totalCompanies.toLocaleString('fr-FR')}</div>
                        <div style="font-size:var(--font-xs); color:var(--text-muted)">🏢 Entreprises</div>
                        <div style="font-size:11px; color:var(--text-disabled)">
                            ${(s.companies_inserted || 0)} nouvelles · ${(s.companies_updated || 0)} enrichies
                        </div>
                    </div>
                    <div>
                        <div style="font-size:var(--font-2xl, 1.5rem); font-weight:800; color:var(--accent, #3b82f6)">${(s.contacts_upserted || 0).toLocaleString('fr-FR')}</div>
                        <div style="font-size:var(--font-xs); color:var(--text-muted)">📞 Contacts</div>
                    </div>
                    <div>
                        <div style="font-size:var(--font-2xl, 1.5rem); font-weight:800; color:var(--info, #6366f1)">${(s.officers_upserted || 0).toLocaleString('fr-FR')}</div>
                        <div style="font-size:var(--font-xs); color:var(--text-muted)">👤 Dirigeants</div>
                    </div>
                    ${s.siren_invalid > 0 ? `
                        <div>
                            <div style="font-size:var(--font-2xl, 1.5rem); font-weight:800; color:var(--warning, #f59e0b)">${s.siren_invalid}</div>
                            <div style="font-size:var(--font-xs); color:var(--text-muted)">⚠️ SIREN invalides</div>
                        </div>
                    ` : ''}
                </div>

                ${result.batch_id ? `
                    <div style="padding:var(--space-sm) var(--space-lg) var(--space-md); border-top:1px solid var(--border-subtle); text-align:center">
                        <a href="#/job/${encodeURIComponent(result.batch_id)}" style="color:var(--accent, #3b82f6); font-weight:600; font-size:var(--font-sm)">
                            📊 Voir le détail de l'import →
                        </a>
                    </div>
                ` : ''}
            </div>
        `;
        showToast(`✅ Import: ${totalCompanies} entreprises, ${s.contacts_upserted || 0} contacts, ${s.officers_upserted || 0} dirigeants`, 'success');
    } else {
        const errorMsg = result?.error || 'Erreur inconnue';
        resultDiv.innerHTML = `
            <div style="padding:var(--space-lg); background:rgba(239,68,68,0.08); border:1px solid var(--danger, #ef4444); border-radius:var(--radius-md)">
                <div style="font-weight:700; color:var(--danger, #ef4444)">❌ Erreur d'import</div>
                <div style="margin-top:var(--space-sm); color:var(--text-secondary); font-size:var(--font-sm)">${escapeHtml(errorMsg)}</div>
            </div>
        `;
    }
}


// ── Upload history ───────────────────────────────────────────────

function _renderHistory(uploads) {
    return `
        <div class="card">
            <h3 style="font-size:var(--font-xs); font-weight:700; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.08em; margin-bottom:var(--space-md)">
                Historique des imports
            </h3>
            <div style="overflow-x:auto">
                <table style="width:100%; border-collapse:collapse; font-size:var(--font-sm)">
                    <thead>
                        <tr style="text-align:left">
                            <th style="padding:var(--space-xs) var(--space-sm); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase">Date</th>
                            <th style="padding:var(--space-xs) var(--space-sm); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase">Fichier</th>
                            <th style="padding:var(--space-xs) var(--space-sm); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase; text-align:right">Entités</th>
                            <th style="padding:var(--space-xs) var(--space-sm); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase; text-align:center">Statut</th>
                            <th style="padding:var(--space-xs) var(--space-sm); border-bottom:2px solid var(--border-default); color:var(--text-muted); font-weight:700; font-size:var(--font-xs); text-transform:uppercase; text-align:center">Détail</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${uploads.map(u => `
                            <tr>
                                <td style="padding:var(--space-xs) var(--space-sm); border-bottom:1px solid var(--border-subtle); color:var(--text-secondary); white-space:nowrap">
                                    ${formatDateTime(u.created_at)}
                                </td>
                                <td style="padding:var(--space-xs) var(--space-sm); border-bottom:1px solid var(--border-subtle); color:var(--text-primary); font-weight:500; max-width:200px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap">
                                    📤 ${escapeHtml((u.batch_name || '').replace('Import: ', ''))}
                                </td>
                                <td style="padding:var(--space-xs) var(--space-sm); border-bottom:1px solid var(--border-subtle); color:var(--text-secondary); text-align:right">
                                    ${(u.batch_size || 0).toLocaleString('fr-FR')}
                                </td>
                                <td style="padding:var(--space-xs) var(--space-sm); border-bottom:1px solid var(--border-subtle); text-align:center">
                                    ${u.status === 'completed'
                                        ? '<span class="badge badge-success" style="font-size:var(--font-xs)">✅</span>'
                                        : u.status === 'in_progress'
                                        ? '<span class="badge badge-accent" style="font-size:var(--font-xs)">⏳</span>'
                                        : `<span class="badge" style="font-size:var(--font-xs)">${escapeHtml(u.status || '')}</span>`
                                    }
                                </td>
                                <td style="padding:var(--space-xs) var(--space-sm); border-bottom:1px solid var(--border-subtle); text-align:center">
                                    <a href="#/job/${encodeURIComponent(u.batch_id || '')}" style="color:var(--accent, #3b82f6)">→</a>
                                </td>
                            </tr>
                        `).join('')}
                    </tbody>
                </table>
            </div>
        </div>
    `;
}
