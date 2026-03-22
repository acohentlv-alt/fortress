import { startDeepEnrich } from './api.js';
import { GlobalSelection } from './state.js';
import { showToast } from './components.js';

export async function openLiveEnrichModal(sirens) {
    if (!sirens || sirens.length === 0) {
        showToast("Aucune entreprise sélectionnée.", "error");
        return;
    }
    if (sirens.length > 20) {
        showToast("Maximum 20 entreprises pour un enrichissement profond.", "warning");
        return;
    }

    // ── Build UI Overlay ──
    const overlay = document.createElement('div');
    overlay.className = 'modal-backdrop';
    overlay.style.display = 'flex';
    
    // We will render a mini pipeline for each company
    const renderCard = (siren) => `
        <div id="live-card-${siren}" style="background:var(--bg-tertiary); padding:var(--space-md); border-radius:var(--radius-md); margin-bottom:var(--space-sm); border-left: 3px solid var(--text-muted)">
            <div style="font-weight:600; font-size:var(--font-sm); margin-bottom:var(--space-sm)">SIREN: ${siren}</div>
            <div style="display:flex; gap:15px; font-size:var(--font-xs)">
                <div class="live-step" id="step-crawl-${siren}" style="color:var(--text-muted)">🕸️ Crawl...</div>
                <div class="live-step" id="step-officers-${siren}" style="color:var(--text-muted)">👔 Dirigeants...</div>
                <div class="live-step" id="step-financials-${siren}" style="color:var(--text-muted)">💶 Finances...</div>
            </div>
            <div id="detail-${siren}" style="font-size:10px; color:var(--text-secondary); margin-top:4px"></div>
        </div>
    `;

    overlay.innerHTML = `
        <div class="modal-content" style="max-width: 600px; width:100%">
            <h2 style="margin-bottom: var(--space-xs)">⚡ Enrichissement Profond en cours</h2>
            <p style="color:var(--text-secondary); margin-bottom:var(--space-lg); font-size:var(--font-sm)">
                Analyse de ${sirens.length} entreprise(s) en temps réel. Ne fermez pas cette fenêtre.
            </p>
            <div style="max-height: 400px; overflow-y:auto; padding-right:10px">
                ${sirens.map(s => renderCard(s)).join('')}
            </div>
            <div style="margin-top:var(--space-lg); display:flex; justify-content:flex-end">
                <button class="btn" id="close-live-modal" style="display:none">Fermer</button>
            </div>
        </div>
    `;
    document.body.appendChild(overlay);

    const closeBtn = document.getElementById('close-live-modal');
    closeBtn.addEventListener('click', () => {
        overlay.remove();
        // Clear selection to prevent accidental re-run
        GlobalSelection.clear();
        const exportBar = document.getElementById('alldata-bulk-bar');
        if (exportBar) exportBar.remove();
    });

    try {
        // Start job
        const res = await startDeepEnrich(sirens);
        const jobId = res.batch_id;
        
        // Connect WebSocket
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/api/ws/enrich/${jobId}`;
        const ws = new WebSocket(wsUrl);

        ws.onmessage = (event) => {
            const data = JSON.parse(event.data);
            
            if (data.status === 'completed') {
                closeBtn.style.display = 'block';
                closeBtn.classList.add('btn-primary');
                showToast("Enrichissement terminé !", "success");
                ws.close();
                return;
            }

            const siren = data.siren;
            const step = data.step;
            const status = data.status;
            
            if (siren && step) {
                const el = document.getElementById(`step-${step}-${siren}`);
                if (el) {
                    if (status === 'running') {
                        el.style.color = 'var(--accent)';
                        el.style.fontWeight = 'bold';
                    } else if (status === 'success') {
                        el.style.color = 'var(--success)';
                        el.style.fontWeight = 'normal';
                    } else if (status === 'error') {
                        el.style.color = 'var(--error)';
                        el.style.fontWeight = 'normal';
                    }
                }
                const detailEl = document.getElementById(`detail-${siren}`);
                if (detailEl && data.detail) {
                    detailEl.innerText = data.detail;
                }
                
                // Color border success if all done (financials usually last)
                if (step === 'financials' && status === 'success') {
                    const card = document.getElementById(`live-card-${siren}`);
                    if (card) card.style.borderLeftColor = 'var(--success)';
                }
            } else if (siren && status === 'started') {
                const detailEl = document.getElementById(`detail-${siren}`);
                if (detailEl) detailEl.innerText = 'Démarrage du processus...';
                const card = document.getElementById(`live-card-${siren}`);
                if (card) card.style.borderLeftColor = 'var(--accent)';
            }
        };
        
        ws.onerror = () => {
            showToast("La connexion directe a été perdue, mais le job continue en arrière-plan.", "warning");
            closeBtn.style.display = 'block';
        };

    } catch (e) {
        showToast("Erreur de lancement de l'enrichissement.", "error");
        closeBtn.style.display = 'block';
    }
}
