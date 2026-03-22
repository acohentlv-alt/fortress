# 📒 Project Log: Gemini context

## ⚠️ MANDATORY RULES FOR ALL AGENTS

### 1. Push & Deploy BEFORE asking user to verify
**NEVER** tell the user "check on the website" or "verify on Render" without FIRST:
1. `git add -A && git commit -m "description"`
2. `git push origin main`
3. Confirm Render deployment started
4. ONLY THEN tell the user to check

### 2. End-of-Session Summary
When the user says "finish working" or ends a session, the agent MUST:
1. Summarize all work done in the last 24h
2. Update this file with what was built
3. Write a to-do list for the next session
4. Update `catch_up.md` in the brain directory

### 3. Wave Size Enforcement
User batch size = EXACT limit. If user requests 20 entities, output ≤ 20. Never exceed.
Logic: `while len(contacts) < target_count` (strict `<`). Already enforced.

---

## Current Stage: Phase 6 (Completed)

### Major Architectural Wins (Last 24h)
1.  **Merge Conflict Engine**: `data_conflicts` in `_merge_contacts()`. A-vs-B comparison UI.
2.  **Siren Mismatch Detection**: `siren_match=false` flag + warning banner.
3.  **Financial Enrichment**: `chiffre_affaires` + `resultat_net` + `tranche_effectif` captured.
4.  **Optimized Crawling**: Wave cap 5× → 2×.
5.  **Social Expansion**: WhatsApp & YouTube fields (full stack).
6.  **Glass Badge UI**: `statutBadge` + `formeJuridiqueBadge` → glass-badge design system.

### Next Objectives
- [ ] Implement "Lier à l'autre entreprise" action for SIREN mismatch cases
- [ ] Verify Merge Dialog visually on live Render deployment
- [ ] Monitor hit-rate improvements with financial data capture

### 🔑 Context Keys
- **Test SIREN**: `399817626` (has known multi-source conflicts)
- **Primary Logic**: `module_d/enricher.py` (orchestrations), `api/routes/companies.py` (merging)
- **Design System**: `css/design-system.css` (glass-badge, btn-liquid, info-tip classes)
