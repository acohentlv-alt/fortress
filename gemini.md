# 📒 Project Log: Gemini context

## Current Stage: Phase 6 (Completed)

### Major Architectural Wins (Last 24h)
1.  **Merge Conflict Engine**: New `data_conflicts` logic in `_merge_contacts(contacts)`. Provides an interactive A-vs-B comparison UI on the company card.
2.  **Siren Mismatch Detection**: Enrichment pipeline now flags `siren_match=false` if the website SIREN ≠ input SIREN.
3.  **Financial Enrichment**: `fetch_dirigeants` in `recherche_entreprises.py` now captures Revenue (CA) and Employee counts (Tranche effectif).
4.  **Optimized Crawling**: Wave cap reduced to 2× to prevent resource exhaustion.
5.  **Multi-Social Support**: Database/API support for WhatsApp & YouTube fields.

### Next Objectives
- [ ] Implement "Lier à l'autre entreprise" action for SIREN mismatch cases.
- [ ] Monitor hit-rate improvements with the new revenue capture.
- [ ] Deploy and verify the visual Merge UI on living data.

### 🔑 Context Keys
- **Test SIREN**: `399817626` (has known multi-source conflicts).
- **Primary Logic**: `module_d/enricher.py` (orchestrations) and `api/routes/companies.py` (merging).
