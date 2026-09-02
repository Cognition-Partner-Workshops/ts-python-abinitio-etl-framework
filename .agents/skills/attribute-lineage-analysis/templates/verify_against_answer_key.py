#!/usr/bin/env python3
"""Read-only GATE 1 helper for the attribute-lineage-analysis Skill.

Two modes, both exit 0 on PASS and 1 on FAIL and never write anything:

  answer-key mode (default when --answer-key exists)
      Diffs the generated attribute chain in OUTPUT_DIR/LINEAGE.json against a
      hand-authored key under expected/. Also refuses to pass if the answer key
      has uncommitted modifications (guards against "editing the key to match").

  self-check mode (--self-check, or automatically when no answer key exists)
      Structural check when no key exists for TARGET_ATTRIBUTE: every node/hop
      names a graph and an artifact; explicit-evidence artifacts exist in the
      repo; every hop input resolves to a source stage_column, a previous hop
      output, or the target; the target is the output of the final hop; no
      hop is missing an `evidence` grade. Prints a loud banner so BROWSER_TEST /
      LINEAGE.md cannot mistake it for an answer-key match.

Usage (from repo root):
    python .agents/skills/attribute-lineage-analysis/templates/verify_against_answer_key.py \
        --generated analysis/LINEAGE.json \
        [--answer-key expected/order_status_lineage.json] \
        [--attribute-key order_status_lineage] [--self-check]

The generated LINEAGE.json may hold the chain at top level or under
`attribute_lineage[<attribute-key>]`; both shapes are accepted.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

EVIDENCE = {"explicit", "inferred", "external"}


def load(path: str) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def strip_private(o):
    if isinstance(o, dict):
        return {k: strip_private(v) for k, v in o.items() if not k.startswith("_")}
    if isinstance(o, list):
        return [strip_private(x) for x in o]
    return o


def section(doc: dict, attribute_key: str | None) -> dict:
    doc = strip_private(doc)
    if attribute_key and attribute_key in doc:
        return doc[attribute_key]
    for container in ("attribute_lineage", "attribute_lineages", "lineages"):
        node = doc.get(container)
        if isinstance(node, dict):
            if attribute_key and attribute_key in node:
                return node[attribute_key]
            if len(node) == 1:
                return next(iter(node.values()))
    if "target" in doc and ("nodes" in doc or "hops" in doc or "source_column_nodes" in doc):
        return doc
    raise KeyError(f"could not locate attribute section '{attribute_key}'")


def nodes_of(sec: dict) -> list[dict]:
    return sec.get("nodes") or sec.get("source_column_nodes") or []


def hops_of(sec: dict) -> list[dict]:
    return sec.get("hops") or sec.get("column_edges") or []


def target_str(sec: dict) -> str:
    t = sec.get("target", {})
    if isinstance(t, str):
        return t
    return ".".join(x for x in (t.get("system"), t.get("table"), t.get("column")) if x)


def norm_nodes(sec):
    return {(n.get("column"), n.get("graph"), n.get("stage_column")) for n in nodes_of(sec)}


def norm_hops(sec):
    return {(h.get("graph"), h.get("output") or h.get("to")) for h in hops_of(sec)}


def key_is_dirty(path: str) -> bool:
    try:
        r = subprocess.run(["git", "status", "--porcelain", "--", path], capture_output=True, text=True, check=True)
        return bool(r.stdout.strip())
    except (OSError, subprocess.CalledProcessError):
        return False


# ------------------------------------------------------------------ answer-key
def answer_key_mode(gen: dict, key_path: str, attribute_key: str | None) -> bool:
    key = section(load(key_path), attribute_key)
    ok = True
    print(f"GATE 1 — answer-key mode: {key_path}")
    if key_is_dirty(key_path):
        ok = False
        print("  FAIL: answer key has uncommitted modifications — never edit the key to force a match")

    gt, kt = target_str(gen), target_str(key)
    print(f"  target {'OK' if gt == kt else 'MISMATCH'}: generated={gt} key={kt}")
    ok &= gt == kt

    gn, kn = norm_nodes(gen), norm_nodes(key)
    print(f"  source nodes: {len(kn)} expected, {len(kn & gn)} matched")
    for m in sorted(kn - gn, key=str):
        ok = False; print(f"    MISSING node: {m}")
    for e in sorted(gn - kn, key=str):
        print(f"    extra node (not in key, review): {e}")

    gh, kh = norm_hops(gen), norm_hops(key)
    for h in [(x.get("graph"), x.get("output") or x.get("to")) for x in hops_of(key)]:
        if h in gh:
            print(f"  hop OK      {h[0]} -> {h[1]}")
        else:
            ok = False; print(f"  hop MISSING {h[0]} -> {h[1]}")
    for e in sorted(gh - kh, key=str):
        print(f"  extra hop (not in key, review): {e[0]} -> {e[1]}")

    # evidence grades must not be upgraded relative to the key
    key_ev = {(h.get("graph"), h.get("output")): h.get("evidence") for h in hops_of(key)}
    for h in hops_of(gen):
        k = (h.get("graph"), h.get("output") or h.get("to"))
        if key_ev.get(k) not in (None, "explicit") and h.get("evidence") == "explicit":
            ok = False
            print(f"  FAIL: hop {k} graded 'explicit' but key says '{key_ev[k]}' — do not upgrade evidence without a new artifact")
    return ok


# ------------------------------------------------------------------ self-check
def self_check_mode(gen: dict, repo: Path) -> bool:
    ok = True
    print("GATE 1 — SELF-CHECK MODE (no answer key for this attribute). Structural only; this is NOT an answer-key match.")
    nodes, hops = nodes_of(gen), hops_of(gen)
    if not nodes or not hops:
        print("  FAIL: chain needs at least one source node and one hop"); return False

    known = {n.get("stage_column") for n in nodes} | {n.get("column") for n in nodes}
    for i, h in enumerate(hops, 1):
        g, out, art, ev = h.get("graph"), h.get("output") or h.get("to"), h.get("artifact") or h.get("transform"), h.get("evidence")
        if not g or not out:
            ok = False; print(f"  FAIL hop {i}: missing graph/output")
        if ev not in EVIDENCE:
            ok = False; print(f"  FAIL hop {i} ({g} -> {out}): evidence must be one of {sorted(EVIDENCE)}, got {ev!r}")
        if not art:
            ok = False; print(f"  FAIL hop {i} ({g} -> {out}): no artifact/transform cited")
        elif ev == "explicit" and not (repo / art).exists():
            ok = False; print(f"  FAIL hop {i} ({g} -> {out}): explicit evidence but artifact not in repo: {art}")
        inputs = h.get("inputs") or ([h["input"]] if h.get("input") else []) or ([h["from"]] if h.get("from") else [])
        if not inputs:
            ok = False; print(f"  FAIL hop {i} ({g} -> {out}): no inputs — every hop must consume something")
        for inp in inputs:
            if inp not in known:
                ok = False; print(f"  FAIL hop {i} ({g} -> {out}): input '{inp}' is not a source stage_column or prior hop output (dangling)")
        known.add(out)
        print(f"  hop {i} {g} -> {out} [{ev}]")

    for n in nodes:
        if n.get("evidence") not in EVIDENCE:
            ok = False; print(f"  FAIL node {n.get('column')}: missing/invalid evidence grade")
        art = n.get("artifact") or n.get("transform")
        if n.get("evidence") == "explicit" and art and not (repo / art).exists():
            ok = False; print(f"  FAIL node {n.get('column')}: explicit evidence but artifact not in repo: {art}")

    tgt = target_str(gen)
    last = hops[-1].get("output") or hops[-1].get("to")
    if last != tgt and not tgt.endswith(last or "\x00"):
        ok = False; print(f"  FAIL: final hop output '{last}' is not the target '{tgt}'")
    else:
        print(f"  target reached: {tgt}")
    return ok


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--generated", default="analysis/LINEAGE.json")
    ap.add_argument("--answer-key", default=None, help="expected/<attribute_lower>_lineage.json")
    ap.add_argument("--attribute-key", default=None)
    ap.add_argument("--self-check", action="store_true")
    ap.add_argument("--repo", default=".")
    a = ap.parse_args()

    gen = section(load(a.generated), a.attribute_key)
    if a.self_check or not a.answer_key or not Path(a.answer_key).exists():
        if a.answer_key and not Path(a.answer_key).exists():
            print(f"answer key not found: {a.answer_key} -> falling back to self-check")
        ok = self_check_mode(gen, Path(a.repo).resolve())
        verdict = "GATE 1 RESULT: PASS (structural self-check only — record 'no answer key' as a limitation)" if ok \
            else "GATE 1 RESULT: FAIL — fix the chain before opening a PR"
    else:
        ok = answer_key_mode(gen, a.answer_key, a.attribute_key)
        verdict = "GATE 1 RESULT: PASS — generated lineage matches the answer key" if ok \
            else "GATE 1 RESULT: FAIL — mismatch is a finding to investigate, not to paper over"
    print(); print(verdict)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
