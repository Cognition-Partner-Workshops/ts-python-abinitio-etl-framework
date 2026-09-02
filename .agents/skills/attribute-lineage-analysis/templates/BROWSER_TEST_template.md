# BROWSER_TEST — `<TARGET_ATTRIBUTE>` lineage viz (GATE 2)

**Result: PASS | FAIL**  (FAIL if any row below is unverified, or if no genuine screen recording exists)

| Item | Value |
|---|---|
| File under test | `<OUTPUT_DIR>/lineage_viz.html` opened as `file://.../lineage_viz.html` |
| Browser / display | e.g. Chrome (visible window, maximized) on `:0` 1600x1200 |
| Recording | `browser_test/lineage_viz_walkthrough.mp4` — `<duration>` s, `<size>`; started **before** first click, stopped after last assertion; cursor visible |
| Console errors on load | 0 (or list) |
| Nodes rendered / expected | `<n>` / `<n>` (from `LINEAGE.json`: sources + hops + parameters) |
| Edges rendered / expected | `<n>` / `<n>` |
| Walkthrough entries | `<n>` — all clicked |
| GATE 1 mode this run | answer-key | self-check |

## Click log (in walkthrough order)

| # | Clicked | Panel showed (graph · artifact · stage column · evidence) | Matches LINEAGE.json? | Screenshot |
|---|---|---|---|---|
| 1 | Source · `<SYSTEM.TABLE.COLUMN>` | `<graph>` · `<artifact>` · `<stage_column>` · `[inferred]` + missing: `<...>` | yes | `browser_test/01_source_<col>.png` |
| 2 | Hop 1 · `<graph> -> <output>` | ... | yes | `browser_test/02_hop1_<...>.png` |
| … | … | … | … | … |
| n | Target · `<TARGET_ATTRIBUTE>` (right-most column, reached after horizontal scroll) | ... | yes | `browser_test/NN_target.png` |
| e1 | Edge `<from> -> <to>` | relationship `<k>` · graph `<g>` · `[grade]` | yes | `browser_test/e1_edge_<...>.png` |

## Legend / accessibility checks

- [ ] Node types distinguishable by shape + `[S]/[I]/[fx]/[T]/[P]` tag (not color alone)
- [ ] Evidence grades distinguishable by stroke (solid/dashed/dotted) + `[explicit]/[inferred]/[external]` text
- [ ] `Reset highlight` clears selection
- [ ] Page works with no network (no CDN / fetch)

## Defects found & fixed

| # | Defect | Fix | Re-recorded? |
|---|---|---|---|
| — | none | — | — |

## Not tested / limitations

- e.g. "no answer key for this attribute; panel values verified against self-check chain only"
