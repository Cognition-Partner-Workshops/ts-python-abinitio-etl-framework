---
name: attribute-lineage-analysis
description: Column-level lineage + impact analysis for ANY mart/staging attribute in this Ab Initio estate (ksh wrappers -> .mp graphs referenced but not checked in -> PSETs -> DML -> Python execution patterns). Produces LINEAGE.json/.md, IMPACT_ANALYSIS.md, BUSINESS_DOCUMENTATION.md and a self-contained interactive viz, gated by an answer-key/self-check and a real screen-recorded in-browser click-through. Use when asked "where does <column> come from", "what breaks if we change <column>", or "map the estate for <column>".
---

# Attribute lineage analysis (Ab Initio estate, static, read-only)

## Parameters

| Param | Required | Example | Notes |
|---|---|---|---|
| `TARGET_ATTRIBUTE` | yes | `ORACLE_STG.STAGING.ORDERS.ORDER_STATUS` | `SYSTEM.SCHEMA.TABLE.COLUMN` or `TABLE.COLUMN`; normalize to upper-case, `attribute_lower` = `order_status` |
| `PROPOSED_CHANGE` | no | "rename ORDER_STATUS -> ORDER_STATE", "widen decimal(10.2) -> (12.2)", "add channel to CDC compare_columns" | drives IMPACT_ANALYSIS.md; if absent, write the impact of a *type widening* as the default scenario and say so |
| `OUTPUT_DIR` | no | `analysis/` (default) | never write outside it, except nothing else |
| `ANSWER_KEY` | no | `expected/<attribute_lower>_lineage.json` | auto-detected; if absent GATE 1 runs in self-check mode. `expected/order_status_lineage.json` ships with the repo |

Everything is derived **statically from the repo**. Never invoke `air`, never connect to Oracle/Teradata/Databricks, never modify `graphs/`, `dml/`, `psets/`, `scripts/`, `data/` or `expected/`.

## Estate file conventions (this repo)

| Artifact | Where | Grammar / what to read | Gotcha |
|---|---|---|---|
| Wrappers | `scripts/run_*.ksh` | `air sandbox run $AI_SANDBOX/graphs/<pipe>/<graph>.mp -pset $AI_SANDBOX/psets/<pset>.pset -PARAM $VAL` — one line per step, **execution order = lineage order** | `run_daily_orders.ksh` pipes through `\| tee` without `set -o pipefail`, so step failures are swallowed; `set -e` alone is not enough |
| Env | `scripts/setenv.ksh` | `AI_SANDBOX`, `AI_SERIAL`, `AI_MFS`, `BATCH_DATE` defaults, DB env names | paths are runtime metadata, not value lineage |
| Graphs | `graphs/*.py` | **No `.mp` exports are checked in.** `parallel_loader.py` (partitioned `air_run`, `PartitionManager`) and `cdc_processor.py` (`CDCProcessor(key_columns, compare_columns)`, MD5 row hash, `inserts/updates/deletes`) are execution *patterns*; graph names come from the wrapper lines | grade anything that depends on a `.mp` body as `inferred` or `external` |
| PSETs | `psets/pset_templates/*.pset` | `define KEY VALUE` (also `KEY=VALUE`); `${ENV}` expanded by `psets/pset_manager.py`; env override at `psets/environments/<env>/<name>.pset` | only 3 of 8 referenced PSETs exist; `orders_pipeline.pset` is referenced by nothing; only `orders_staging.pset` binds a DML (`DML_FILE=`) |
| DML | `dml/*.dml` | `record ... end;` / `type NAME = record ... end;`, `include "x.dml";`, `string(",") f;`, `decimal(",", 10.2)`, `date("YYYY-MM-DD")(",")`, `string(",", null("")) f;`, vectors `type(...)[n] f;` / `record[n] ... end f;`, `if (expr) record ... end f;`, `void(",") pad;`, `packed_decimal(5)` / `zoned_decimal(4)` / `string(20)` fixed width, `ebcdic string` | `utils/dml_parser.py` expects `record NAME {` and parses **0 fields** from every real file — never use it as a schema source; use `templates/estate_inventory.py` |
| Samples | `data/sample/*.dat` | pipe-delimited, header row | `orders.dat` (7 flat cols) does **not** match `order_items.dml` (5 fields incl. vectors); `customers.dat` matches neither `customer.dml` nor `customer_address.dml` — sample ≠ DML is a finding, not a reason to pick one silently |
| Tests | `tests/` (pytest) | source of truth for `pset_manager` behaviour | never edit tests to make GATE 1 pass |

Evidence grades used everywhere (JSON, viz, MD): `explicit` = a checked-in artifact names it; `inferred` = derived from wrapper order / naming / sample data; `external` = exists only on the Ab Initio host (missing `.mp`/`.pset`). Never upgrade a grade without a new artifact.

## Procedure

Work through every step; record the command/output you used in the `LINEAGE.json.metadata.evidence_log`.

### 0. Orient (read-only)
1. `cat README.md`; note the migration framing (graphs→notebooks, DML→schemas, PSET→job params, CDC→Delta MERGE).
2. Run the scanner and keep its output as the estate inventory:
   ```bash
   python .agents/skills/attribute-lineage-analysis/templates/estate_inventory.py \
       --json "$OUTPUT_DIR/estate_inventory.json" > "$OUTPUT_DIR/estate_inventory.md"
   ```
   It cross-references ksh → `.mp`/`.pset` → `DML_FILE` → `dml/` (with a parser that handles the real grammar), lists missing/unreferenced artifacts, flags `tee`-without-`pipefail`, and classifies each step set-based vs procedural.
3. Locate `TARGET_ATTRIBUTE`: `grep -ri <column>` across `psets/ dml/ scripts/ graphs/ data/ tests/ deployment/`. Note which artifact names the *table* (`TARGET_TABLE=` in a PSET) and which names the *column* (a DML field, a sample header, or nothing).
4. Decide the owning pipeline from the wrapper whose steps write the table. If nothing writes it, stop and report: lineage cannot be asserted.

### 1. Walk backward from the target, hop by hop
Start at the target column and move upstream one wrapper step at a time. For each hop capture: `graph` (`.mp` name from the wrapper line), `artifact` (repo path you actually read), `output` stage column, `inputs[]`, `hop_type`, `evidence`, and `missing[]`.

Hop types seen in this estate: `extract` (external graph → landing file), `snapshot`, `cdc (hash compare, multi-output)`, `record-format bind` (PSET `DML_FILE` → DML field, positional), `load (MERGE|INSERT|UPSERT)`, `rollover/swap` (prod partition swap), `audit-trail append`, `direct carry`, `arithmetic derivation`, `aggregation`, `conditional map`.

Rules:
- Preserve every intermediate field (`raw_orders.status` → `orders_delta.status` → `order_items.order_status` → target). Never collapse to a source→target shortcut.
- DML → column mapping is positional: field order in the DML **is** the column order in the landing/staging file. Flatten nested records as `parent.child`; vectors as `field[n]`; conditional sub-records carry their `if (...)` predicate as a rule.
- CDC steps do not change values but do change **which rows** reach the target; record the key (`key_columns`) and `compare_columns` (if the attribute is not a compare column, changes to it will not be detected — this is a lineage fact).
- Runtime parameters (`BATCH_DATE`, `PARTITION_COUNT`, `BATCH_SIZE`, `MAX_ERRORS`, `CHECKPOINT_DIR`, `REJECT_PATH`, `SLA_MINUTES`, DB names) go in `runtime_parameters`, drawn as `[P]` nodes, never as sources.
- Steps *downstream* of the target (e.g. `prod_rollover_orders` when the target is `STAGING.ORDERS.*`) are listed under `downstream_consumers`, not in `hops`.
- A reference that resolves to nothing in the repo is a **finding** (`F-GRAPH-n`, `F-PSET-n`, `F-DML-n`, `F-WRAP-n`), never a guess.

### 2. Orchestration and reliability
From the wrapper(s): job name, cadence (comment header / AutoSys hint), step order, parameters per step, checkpoint/reject paths, SLA, and error handling. Call out `| tee` without `set -o pipefail`, unquoted paths, missing `exit` on failure. Map each step to set-based (dbt) vs procedural/multi-output (PySpark/DLT) — the scanner does this; confirm by reading the step.

### 3. Deliverables (all under `OUTPUT_DIR`)

| File | Must contain |
|---|---|
| `estate_inventory.md` / `.json` | scanner output from step 0 (unmodified) |
| `LINEAGE.json` | `metadata{attribute, generated_at, repo_commit, mode: answer-key\|self-check, evidence_log[]}`, `target{system,table,column}`, `formula`, `systems[]`, `nodes[]` (source columns), `hops[]` (in order, each with graph/artifact/output/inputs/hop_type/evidence/missing), `runtime_parameters[]`, `orchestration{wrapper,job,cadence,steps[]}`, `dml_layouts{}` for every DML touched, `downstream_consumers[]`, `findings[]`, `limitations[]`. Shape must be accepted by `verify_against_answer_key.py` (see `answer_key_template.json`). |
| `LINEAGE.md` | narrative source→target; inventory table; column-mapping table (source col → stage col → DML field → target col, with grade); Mermaid `flowchart TD` with `[S]/[I]/[fx]/[T]/[P]` tags and dashed edges for inferred/external; legend; findings table; **GATE 1 result verbatim**; **GATE 2 summary with recording path** |
| `IMPACT_ANALYSIS.md` | `PROPOSED_CHANGE` restated; direct impacts (DML field, PSET, wrapper params); transitive impacts hop by hop *downstream* of the change; explicit non-impacts; whether the change is value-invariant (rename/widen) vs logic-changing (formula/key/compare_columns); recommended validation (row counts, hash of compare columns before/after, reject-file inspection, dbt test / DLT expectation equivalents); if `expected/impact_analysis_example.md` exists, cross-check style and hop count |
| `BUSINESS_DOCUMENTATION.md` | plain-language: what the table is, what the attribute means, how it is produced and how often, what "as of" means (BATCH_DATE), synthetic-data caveat, technical appendix mapping business words → artifact names |
| `lineage_viz.html` | copy `templates/lineage_viz_template.html`, replace the block between `EDIT BELOW/ABOVE`; **every** node and hop from `LINEAGE.json` present; `ev` grade on every node/edge; `WALK` lists all sources, then hops in order, then parameters; inline everything, no CDN |
| `BROWSER_TEST.md` | copy `templates/BROWSER_TEST_template.md`; fill every row |
| `browser_test/` | `lineage_viz_walkthrough.mp4` (or animated `.webp`) **real screen recording** + ordered `01_*.png ... NN_*.png` screenshots |

### 4. GATE 1 — answer key / structural self-check (mandatory, before viz)
```bash
python .agents/skills/attribute-lineage-analysis/templates/verify_against_answer_key.py \
    --generated "$OUTPUT_DIR/LINEAGE.json" \
    --answer-key "expected/${attribute_lower}_lineage.json"      # omit if none exists
```
- Answer-key mode: target identity, every expected source node `(column, graph, stage_column)`, every expected hop `(graph, output)`, no evidence-grade upgrades. The helper **fails if the key has uncommitted changes** — the key must be committed before the analysis and is never edited by it. A mismatch is a finding to investigate (re-read the artifacts), never something to paper over.
- Self-check mode (no key for this attribute): every hop has graph + artifact + evidence grade; `explicit` artifacts exist on disk; every input resolves to a source stage column or a prior hop output; final hop output == target. Record `mode: self-check` in `LINEAGE.json` and say "no answer key" in `LINEAGE.md` limitations.
- Paste the verdict block verbatim into `LINEAGE.md`. Do not proceed to GATE 2 on FAIL.

### 5. GATE 2 — in-browser viz test with a real screen recording (mandatory)
1. Maximize the visible browser (`wmctrl -r :ACTIVE: -b add,maximized_vert,maximized_horz`).
2. **Start recording first**: prefer the platform recorder (`recording_start`); fallback `templates/record_browser_test.sh start "$OUTPUT_DIR/browser_test"`. Add `setup` / `test_start` / `assertion` annotations if the recorder supports them.
3. Open `file://$PWD/$OUTPUT_DIR/lineage_viz.html` in that browser (not headless, not off-screen). Confirm zero console errors.
4. Click **every** entry of the ordered walkthrough (all sources, all hops in order, all parameters) and at least one edge per hop. After each click, compare the details panel to `LINEAGE.json` (graph, artifact, stage column, inputs, rule/formula, evidence grade, missing[]). Screenshot each step into `browser_test/NN_<node>.png`.
5. Scroll horizontally to prove the right-most (target) column is reachable; click the target node last.
6. Stop the recording (`recording_stop` / `record_browser_test.sh stop ...`) and move/copy the file to `browser_test/lineage_viz_walkthrough.mp4`.
7. Fill `BROWSER_TEST.md`: what was clicked, what was verified, defects found and fixed, ordered screenshots, recording path + duration, explicit **PASS/FAIL**.
8. **Any viz fix ⇒ re-run steps 2–7 and re-record.** A screenshot montage, a video synthesized from screenshots, or a headless capture does **not** satisfy the gate; GATE 2 = FAIL without a genuine recording, even if all screenshots exist.

### 6. Wrap up
- `git status` must show only `OUTPUT_DIR/**` (and nothing under `expected/`, `dml/`, `psets/`, `scripts/`, `graphs/`, `tests/`).
- Open the PR from a `devin/<ts>-lineage-<attribute_lower>` branch; body: target, one-line formula, GATE 1 verdict, GATE 2 verdict + recording path, findings count, limitations. No requester-identifying info.
- Final message to the user: verdict, PR link, open decisions only.

## Pitfalls hit in this estate (check each one every run)

1. **Zero `.mp` graphs** while wrappers reference 8. Graph names are real (from the wrapper), graph *bodies* are not; grade `external` and list under `missing[]`. Do not reconstruct XFR logic from imagination.
2. **6 of 8 PSETs missing**; the 3 present are *templates*, not the runtime sets the wrapper names. `orders_pipeline.pset` is referenced by nothing — say "unreferenced", do not attach it to a step.
3. **Only one explicit DML binding** (`orders_staging.pset` → `order_items.dml`). All other DML→graph links are inferred; four DMLs (`transaction_detail`, `packed_account`, `account_balance`, `account_status`) are orphans until a wrapper/PSET names them.
4. **`utils/dml_parser.py` is grammar-incompatible** (0 fields on every file). Use `templates/estate_inventory.py`; if you extend it, re-run it on all 8 DMLs and eyeball `customer_address.dml` (include + user type), `transaction_detail.dml` (vector + conditional + `null("...")`), `packed_account.dml` (fixed width), `account_status.dml` (`void`).
5. **Sample ≠ DML**: `orders.dat` is 7 flat pipe-delimited columns, `order_items.dml` is 5 comma-delimited fields with vectors; `customers.dat` has `address|city|state|zip|created_date` matching neither customer DML. Report as `F-DML-n` and pick the DML for *staging* lineage and the sample for *raw extract* shape, stating both.
6. **`set -e` + `| tee` without `pipefail`** in `run_daily_orders.ksh` means a failed graph can still return 0; the customer CDC wrapper does not have the pipe. Record under orchestration findings; it changes the "recommended validation" in the impact analysis.
7. **Runtime parameters are not lineage.** `BATCH_DATE`, partition counts, checkpoint/reject paths, DB env names select or route rows; they never produce the attribute's value. Draw them as `[P]`.
8. **Do not collapse hops.** The CDC step is a real hop even though it carries the value unchanged — it decides row visibility and depends on `compare_columns`.
9. **Nested/vector/conditional DML fields**: flatten to `parent.child`, keep `vector_of` and `condition`; a target column inside a conditional record inherits the predicate as a rule.
10. **Answer key hygiene**: the helper refuses to PASS on an uncommitted key. If a key is missing, the run is self-check only — never claim an answer-key match.
11. **Viz must work from `file://`**: no `fetch()`, no CDN, no ES modules. Wide chains need horizontal scroll — that is fine as long as the ordered walkthrough reaches every node; test it.
12. **Shape/stroke/tag, not color**: node types via shape + `[S]/[I]/[fx]/[T]/[P]`; evidence via stroke (solid/dashed/dotted) + `[explicit]/[inferred]/[external]` text.
13. **Playwright can't click transparent edge hit-paths** on straight edges (zero-height bbox); use `page.mouse.click(x,y)` on a point along the path or click nodes. Real users are unaffected. GATE 2 is a *human-style* click-through on the visible desktop anyway.
14. **`graphs/cdc_processor.py` fails its own tests** (`tests/test_pset_manager.py::TestCDCProcessor::test_inserts_detected` / `test_deletes_detected`, `TypeError: unhashable type: 'dict'` inside `process()`). Tests are the source of truth (TDD): describe CDC semantics from the tests' expectations, cite the failing tests as a finding, and do not "fix" the pattern as part of a lineage run.
15. **Recording is the evidence.** Start it before the first click, keep the cursor visible, stop it after the last assertion; if any fix touches the viz, record again from scratch.

## Templates in this skill

- `templates/estate_inventory.py` — read-only scanner: ksh/PSET/DML cross-reference, real-grammar DML parser, set-based vs procedural classification, missing-artifact report (Markdown + `--json`).
- `templates/verify_against_answer_key.py` — GATE 1 helper (answer-key and self-check modes, dirty-key guard, evidence-grade guard).
- `templates/answer_key_template.json` — key format with a worked `STAGING.ORDERS.ORDER_STATUS` example and `_why` annotations.
- `templates/lineage_viz_template.html` — self-contained SVG viz with evidence grades, `missing[]` warning, ordered walkthrough (worked example: `ORDER_STATUS`).
- `templates/BROWSER_TEST_template.md` — GATE 2 report skeleton.
- `templates/record_browser_test.sh` — ffmpeg x11grab fallback recorder (`start`/`stop`).
