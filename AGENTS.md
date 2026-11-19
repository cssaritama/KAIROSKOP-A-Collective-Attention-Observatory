# Working in KAIROSKOP — Systems Studio Collective Attention Observatory

Read `README.md` first. Follow the active task in `CURRENT_STATE.md`.

## Founding Life Mission of Systems Studio

> *"Understand, identify, and detect human and institutional behavioral patterns across markets, organizations, sports, and collective consciousness—and turn those asymmetric insights into decisive action."*

---

## 1. Front-Page Context Firewall Table

To eliminate context contamination and maintain zero token waste, all agents operate under the **Context Firewall**:

| Operation Category | Permitted Files / Directories | Explicit Restrictions & Firewalls |
|---|---|---|
| **WHERE TO START READING**<br/>*(Then follow links as the task requires)* | 1. `AGENTS.md`<br/>2. `CURRENT_STATE.md`<br/>3. `README.md`<br/>4. Active contract in `data/attention_snapshots/` or `src/` | Start with these files, then follow links until scope, permissions and acceptance are clear. Read history, archives, large logs or other repositories only when the task needs that evidence. |
| **WHERE TO WRITE IDEAS & REFS**<br/>*(Local First, Global by Exception)* | **Local Project Repository (`KAIROSKOP`):**<br/>- Attention metrics, pipeline ideas stay in `IDEAS.md`<br/>- Sentiment references, external data sources stay in `REFERENCES.md`<br/>*Classified by type:*<br/>`[SIGNAL_OR_MODEL]`, `[ARCHITECTURE_OR_PERF]`,<br/>`[PRODUCT_OR_UX]`, `[COMMERCIAL_OR_MONETIZATION]`<br/><br/>**Global by Exception (Systems Studio):**<br/>- Tagged `[CROSS_PORTFOLIO_HQ]` in `systems-studio-founder/IDEAS.md` or `systems-studio-innovation/REFERENCES.md` only after verifying zero duplicates.<br/>- If offline/detached, stage in local `## Outbox: Pending HQ Sync`. | **NEVER** dump high-frequency event stream logs into Systems Studio repositories.<br/>**NEVER** create duplicate entries without scanning existing IDs. |
| **WHERE NOT TO READ**<br/>*(Context Contamination Firewall)* | **Strictly Forbidden:**<br/>`archive/`, `past_version/`, full raw stream dumps, or unrelated domain sibling repositories during routine execution. | Loading archived or inactive files causes prompt drift, hallucination, and massive token exhaustion. |

---

## 2. Proactive Strategic Co-Pilot & 6-Validator Decision Protocol

1. **Never a Passive Interrogator:** The system already maintains repository state (`CURRENT_STATE.md`, `README.md`). Agents must NEVER dump blank questionnaires or leave decision-making open-ended to Carlos.
2. **Proactive Option Synthesis:** When an attention pipeline choice, stream architecture, or model bottleneck arises:
   - Synthesize current context automatically from state files.
   - Frame the decision with the result statement and outcome framing of the Systems Studio Engineering Standard, and record a consequential choice in an ADR.
   - Present 2–3 concrete options scored by **`ITR` (Investment, Time, Return)**.
   - Proactively recommend **The Optimal Path & Why**, accompanied by an immediate physical action ready for one-word ("Sí") executive approval.
3. **The Flashlight Rule:** Internal decision engines utilize structural models (Lacan R-S-I, Peirce's Quadrants, 11-Step Lifecycle) to maintain rigorous analytical clarity, while all external deliverables, code, and communications remain in crisp, minimalist, high-impact C-Suite American English with 10-word inline micro-bridges.

---

## 3. Four-Layer Agentic Architecture (Grigorev + gstack Standard)

- **Layer 1 (Entry Map):** Concise root entry maps in `AGENTS.md` (<60 lines or tightly modular). Provider adapters (`CLAUDE.md`, `GEMINI.md`) contain single-line pointers (`@AGENTS.md`).
- **Layer 2 (Empirical Skills):** Reusable workflows in `.agents/skills/<name>/SKILL.md` (mirrored in `skills/`), captured empirically only after executing, testing, and refining a real task.
- **Layer 3 (3-Role Fresh-Session Execution Loop):**
  - **Task #1 Baseline Rule:** The first backlog task of any new project is always an empty project skeleton with 1 passing test.
  - **PM Role:** Grooms tasks with clear `Goal`, binary `Acceptance Criteria`, `Out of Scope` linked to future task IDs, and `Constraints`.
  - **Engineer Role:** Implements code and runs tests without closing the task prematurely.
  - **Fresh-Session QA Role:** Executes in a clean session unbiased by implementation context; verifies running code and tests; outputs `## QA: PASS` or `## QA: FAIL`.
  - **Checkable Stop Conditions & Worktrees:** Loops enforce binary machine-checkable criteria. Concurrent work tracks use isolated `git worktree` directories.
  - **"Fix the Documents, Not Only the Code":** Every human correction triggers an immediate update to the governing standard/specification upon committing.
- **Layer 4 (MCP Governance):** Load only domain-required MCP servers; default to read-only mode; strictly forbid autonomous execution of destructive commands (`rm -rf`, `force push`, DB drops, unreviewed cloud provisioning).

---

## 4. 3-Tier Agentic Governance Authority Matrix

- **Tier A (Autonomous Execution):** Local code exploration, pipeline authoring, refactoring, linting, running unit tests (`pytest`). Autonomous execution without interrupting the owner for routine reversible steps.
- **Tier B (Shadow-Mode & Empirical Receipt Gate):** Schema changes, snapshot specification updates, CI workflow modifications. Requires immutable empirical proof (SHA-256 hash, test suite execution logs, CI green run).
- **Tier C (Human-in-the-Loop Gate):** Public deployment, production release, licensing changes, external publication, or capital spend (>0 EUR). Strict signed authorization from Carlos Saritama required.

---

## 5. Routing to Systems Studio repositories

Any universal engineering standard, C-Suite ontology update, or cross-project idea routes to the Systems Studio repository that owns it:
- Cross-portfolio ideas → `systems-studio-founder/IDEAS.md` (tagged `[CROSS_PORTFOLIO_HQ]`)
- Shared engineering baselines → `systems-studio-standard/standard/SHARED_RULES_CANDIDATE.md`, changed only by pull request
- Reusable agent roles & skills → `systems-studio-standard/standard/AGENT_ROLES_AND_SKILLS_CATALOGUE.md` or `.agents/skills/`
- External references → `systems-studio-innovation/REFERENCES.md`

---

## 6. Operating Rules & Guardrails

- **Zero-Bluff Context Honesty:** If any agent lacks context, has not read a required file, encounters an unindexed dependency, or hits a memory/context boundary, it MUST state that gap directly in sentence 1 of its response rather than guessing, bluffing, or wasting Carlos's time. Radical transparency is absolute.
- **Zero AI Footprint:** Strictly forbid any AI attribution signatures (`Co-authored-by:`, `Generated with...`). All commits and code authored by `Carlos Saritama <cssaritama@gmail.com>`.
- **Strict Verifiability:** Nothing is claimed done without running observable checks (`pytest`).
- **Rule #10 (Mandatory Backlog Auto-Registration & Cross-Repo Sync):** Log approved features and improvements with sequential IDs and update tracking upon completion.
- **Rule #11 (CI Workflow Parity & Remote Run Verification):** Verify CI environment parity and inspect remote run status when CLI access is available.
- **Rule #12 (Dimensional Invariance, Anti-Tautology & Finite-Sample Statistical Rigor):**
  - Never divide dimensionless Z-scores by unnormalized currency ATR (always use percentage ATR = `ATR / Spot` or native standard deviations).
  - Never enforce risk-reward gates via tautological target formulas (`take_profit = entry + 2.6 * risk`); structural support/resistance walls must genuinely provide $R:R \ge 2.5$ before gating entry.
  - Use Student's $t$ distribution (not Gaussian `erfc`) for finite-sample tests ($n < 30$), enforce unconditional bootstrap CI lower-bound checks (`ci_lower <= 0.0`), use logit $\ln(p/(1-p))$ for probabilities, and combine multi-sensor p-values via Simes or Cauchy combination (never raw $\min(P_i)$) before Benjamini-Hochberg FDR.
- **Rule #13 (Automated Code-Rule Checker & Root Pytest Isolation):**
  - Every repository with Python tests MUST configure `pyproject.toml` or `pytest.ini` with `testpaths = ["tests"]` and `norecursedirs = ["archive", "past_version", ".*"]` so bare `pytest` from the repository root never collects archived legacy files.
- **Language & Intuitive Pronunciation Rule (Strictly No IPA Symbols):** Write all artifacts in American English; communicate in Spanish. All C-Suite English coaching must strictly avoid IPA symbols and use **Intuitive Spanish-Friendly Respelling** (e.g., `ei-suh-MÉ-trik SÍG-nul ÁR-buh-trazh`) + 1 native linking tip.
