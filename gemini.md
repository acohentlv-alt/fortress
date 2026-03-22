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

---

## 🧠 Model Routing Guidance

### Suggested Model Selection
| Task Type | Recommended Model | Why |
|-----------|-------------------|-----|
| **Architecture / Planning** | Claude Opus | Deep reasoning, multi-file analysis, DB schema design |
| **UI / CSS / Frontend** | Gemini 2.5 Pro | Visual debugging, layout reasoning, responsive design |
| **Step-by-step execution** | Sonnet / Gemini Flash | Following a defined plan, simple edits, file updates |

### ⚠️ Honest Counter-Argument (Agent's Professional Opinion)

> [!CAUTION]
> **The real cost is context loss, not tokens.**

The instinct to save tokens by switching models is understandable but has **hidden costs** that often exceed the savings:

1. **Context re-bootstrapping costs more than the model delta.** Every model switch means the new model must re-read files, re-understand the codebase, and re-discover edge cases. The 10-15 tool calls spent "warming up" a cheaper model often cost more than keeping the expensive one.

2. **UI work is NOT simple.** We just spent hours debugging CSS flex overflow, `opacity:0` tooltip failures, and responsive layout blowouts. A "low" model would have made more mistakes, requiring more iteration cycles. The cost of YOUR time debugging bad AI output exceeds any token savings.

3. **"Defined step-by-step plans" still hit surprises.** The `resultat_net` column error, the tooltip inline rendering, the flex blowout — none were in the plan. A flash model wouldn't have caught them proactively.

4. **The economic optimization is session discipline, not model switching:**
   - Keep focused conversations (one feature per session)
   - Use task.md and walkthrough.md for handoff (already doing this)
   - Break work into distinct sessions with clear scope

**Bottom line:** Model-switching saves ~15-20% on tokens but adds ~40% re-context overhead. Use it for truly mechanical tasks (renaming files, updating versions, copy-pasting boilerplate) but NOT for debugging or feature work.
