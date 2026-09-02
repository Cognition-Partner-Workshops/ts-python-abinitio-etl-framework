#!/usr/bin/env python3
"""Read-only estate inventory for the attribute-lineage-analysis Skill (Phase A).

Cross-references the four artifact families of this Ab Initio estate and reports
what is present, what is referenced-but-missing, and which DML each pipeline
step binds:

  scripts/*.ksh      -> ordered `air sandbox run` steps (graph, pset, params)
  psets/**/*.pset    -> `define KEY VALUE` parameters (SOURCE_PATH, TARGET_TABLE, DML_FILE, ...)
  dml/*.dml          -> record layouts (nested records, vectors, conditionals, includes)
  graphs/            -> Python execution-pattern modules and any *.mp exports

It never writes into the source tree; it prints Markdown to stdout and, with
--json, writes a machine-readable inventory to the given path (put it under
OUTPUT_DIR, e.g. analysis/estate_inventory.json).

Usage (from repo root):
    python .agents/skills/attribute-lineage-analysis/templates/estate_inventory.py \
        [--repo .] [--json analysis/estate_inventory.json]

Exit code is 0 even when gaps are found: gaps are findings for ESTATE_MAP.md,
not errors. The shipped utils/dml_parser.py does NOT understand this estate's
DML syntax (verified: 0 fields on every file) — use the parser below instead.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

# --------------------------------------------------------------------------- DML
BASE_TYPES = "decimal|integer|long|double|string|date|datetime|packed_decimal|zoned_decimal|void|ebcdic|utf8"
ARGS = r"(?:[^()]|\([^()]*\))*"                # one level of nested parens, e.g. null("")
FIELD_RE = re.compile(
    rf"^(?P<type>{BASE_TYPES})"
    rf"(?:\((?P<args>{ARGS})\))?"               # (",") | ("8.2", ",") | (5) | ("YYYY-MM-DD") | (",", null(""))
    rf"(?:\((?P<extra>{ARGS})\))?"              # second arg group: date("fmt")(";")
    r"(?:\[(?P<vec>[^\]]+)\])?"                 # [item_count]
    r"\s+(?P<name>\w+)\s*;$"
)
ARG_TOKEN_RE = re.compile(r'"((?:[^"\\]|\\.)*)"|null\("((?:[^"\\]|\\.)*)"\)|(\d+(?:\.\d+)?)')


def split_args(s: str) -> tuple[list[str], str | None]:
    """Tokenize a DML type-argument list into positional args and an optional null default."""
    args, null_default = [], None
    for m in ARG_TOKEN_RE.finditer(s or ""):
        if m.lastindex == 2:      # null("...")
            null_default = m.group(2)
        else:                     # quoted string or bare number
            args.append(m.group(m.lastindex))
    return args, null_default


def parse_dml(path: Path, seen: set[str] | None = None) -> dict:
    """Parse an Ab Initio DML record layout into a nested field list.

    Handles: `record ... end;`, `type NAME = record ... end;`, nested sub-records
    (`record ... end name;`), vector sub-records (`record[n] ... end name;`),
    conditional sub-records (`if (expr)` immediately before `record`), vectors
    (`type(...)[n] name;`), `void` skip fields, `null("x")` defaults, and
    `include "file.dml";`.
    """
    seen = seen or set()
    text = path.read_text(encoding="utf-8")
    includes, types, fields, findings = [], {}, [], []
    stack: list[list] = [fields]
    meta_stack: list[dict] = [{}]
    pending_cond = None

    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith(("//", "#")):
            continue
        m = re.match(r'^include\s+"([^"]+)"\s*;', line)
        if m:
            inc = path.parent / m.group(1)
            includes.append(m.group(1))
            if inc.exists() and str(inc) not in seen:
                seen.add(str(inc))
                sub = parse_dml(inc, seen)
                types.update(sub["types"])
            elif not inc.exists():
                findings.append(f"include not found: {m.group(1)}")
            continue
        m = re.match(r"^type\s+(\w+)\s*=\s*record\s*$", line)
        if m:
            stack.append([]); meta_stack.append({"kind": "type", "name": m.group(1)})
            continue
        m = re.match(r"^if\s*\((.+)\)\s*$", line)
        if m:
            pending_cond = m.group(1)
            continue
        m = re.match(r"^record(?:\[(\w+)\])?\s*$", line)
        if m:
            meta = {"kind": "record", "vector_of": m.group(1), "condition": pending_cond}
            pending_cond = None
            if len(stack) == 1 and not fields and meta_stack[-1] == {}:
                meta_stack[-1] = meta  # top-level record
                continue
            stack.append([]); meta_stack.append(meta)
            continue
        m = re.match(r"^end\s*(\w+)?\s*;$", line)
        if m:
            name = m.group(1)
            meta = meta_stack.pop(); body = stack.pop()
            if meta.get("kind") == "type":
                types[meta["name"]] = body
            elif name:  # nested sub-record
                stack[-1].append({
                    "name": name, "type": "record", "fields": body,
                    "vector_of": meta.get("vector_of"), "condition": meta.get("condition"),
                })
            # else: top-level `end;` -> nothing to do
            continue
        m = FIELD_RE.match(line)
        if m:
            g = m.groupdict()
            args, null_default = split_args(g["args"])
            extra, _ = split_args(g["extra"])
            f = {"name": g["name"], "type": g["type"]}
            unquoted_len = bool(g["args"]) and '"' not in g["args"]
            if g["type"] in ("packed_decimal", "zoned_decimal") or (g["type"] == "string" and unquoted_len):
                f["fixed_width_or_precision"] = args[0] if args else None   # fixed-width / mainframe
            elif g["type"] in ("date", "datetime"):
                f["format"] = args[0] if args else None                     # date("fmt")("delim")
                f["delimiter"] = extra[0] if extra else None
            else:
                if len(args) > 1 and re.match(r"^\d+(\.\d+)?$", args[0]):
                    f["precision"] = args[0]                                 # decimal("8.2", ",")
                f["delimiter"] = args[-1] if args else None
            if null_default is not None:
                f["null_default"] = null_default
            if g["vec"]:
                f["vector_of"] = g["vec"]
            if pending_cond:
                f["condition"] = pending_cond; pending_cond = None
            stack[-1].append(f)
            continue
        m = re.match(r"^(\w+)\s+(\w+)\s*;$", line)  # typed field: address_t address;
        if m:
            stack[-1].append({"name": m.group(2), "type": m.group(1), "user_type": True})
            continue
        findings.append(f"unparsed line: {line}")

    def flatten(fs, prefix=""):
        out = []
        for f in fs:
            if f.get("type") == "record":
                out += flatten(f["fields"], prefix + f["name"] + ".")
            elif f.get("user_type") and f["type"] in types:
                out += flatten(types[f["type"]], prefix + f["name"] + ".")
            elif f["type"] != "void":
                out.append(prefix + f["name"])
        return out

    delims = sorted({f.get("delimiter") for f in _walk(fields) if f.get("delimiter")})
    return {
        "file": str(path), "includes": includes, "types": types, "fields": fields,
        "flat_columns": flatten(fields), "delimiters": delims,
        "has_vectors": any(f.get("vector_of") for f in _walk(fields)),
        "has_conditionals": any(f.get("condition") for f in _walk(fields)),
        "has_nested_records": any(f.get("type") == "record" or f.get("user_type") for f in fields),
        "is_fixed_width": bool(fields) and not delims,
        "findings": findings,
    }


def _walk(fs):
    for f in fs:
        yield f
        if f.get("type") == "record":
            yield from _walk(f["fields"])


# -------------------------------------------------------------------------- PSET
def parse_pset(path: Path) -> dict:
    params = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "//")):
            continue
        if line.startswith("define "):
            k, _, v = line[7:].partition(" ")
            params[k.strip()] = v.strip()
        elif "=" in line:
            k, _, v = line.partition("=")
            params[k.strip()] = v.strip()
    return params


# --------------------------------------------------------------------------- KSH
RUN_RE = re.compile(r"air\s+sandbox\s+run\s*\\?\s*\n?(?P<body>(?:.*\\\s*\n)*.*)", re.MULTILINE)


def parse_ksh(path: Path) -> dict:
    text = path.read_text(encoding="utf-8")
    vars_ = dict(re.findall(r'^\s*(?:export\s+)?([A-Z_]+)="?([^"\n]+?)"?\s*$', text, re.MULTILINE))
    joined = re.sub(r"\\\s*\n", " ", text)  # join continuation lines
    steps = []
    for m in re.finditer(r"air\s+sandbox\s+run\s+(.+)", joined):
        seg = m.group(1)
        toks = re.findall(r'"([^"]+)"|(\S+)', seg)
        toks = [a or b for a, b in toks]
        step = {"graph": toks[0] if toks else None, "pset": None, "partition": None, "params": []}
        i = 1
        while i < len(toks):
            t = toks[i]
            if t == "-pset": step["pset"] = toks[i + 1]; i += 2
            elif t == "-partition": step["partition"] = toks[i + 1]; i += 2
            elif t == "-param": step["params"].append(toks[i + 1]); i += 2
            elif t == "-log": i += 2
            elif t in ("2>&1", "|", "tee", "-a") or t.startswith(("$", "2>")): i += 1
            else: i += 1
        steps.append(step)
    job = re.search(r"(JOB_[A-Z0-9_]+)", text)
    header = "\n".join(text.splitlines()[:6])
    cadence = re.search(r"(every \d+ \w+|daily|hourly|weekly|monthly)", header, re.IGNORECASE)
    pipefail = "pipefail" in text
    uses_tee = "| tee" in text
    return {
        "file": str(path), "job": job.group(1) if job else None,
        "cadence": cadence.group(1) if cadence else None,
        "vars": vars_, "steps": steps, "set_e": "set -e" in text,
        "pipefail": pipefail, "uses_tee_pipe": uses_tee,
        "rc_swallowed_by_pipe": uses_tee and not pipefail,
    }


def expand(value: str, vars_: dict, env: dict) -> str:
    def rep(m):
        k = m.group(1)
        return vars_.get(k, env.get(k, m.group(0)))
    prev = None
    while prev != value:
        prev, value = value, re.sub(r"\$\{(\w+)\}", rep, value)
    return value


def to_repo_rel(p: str, project_dir: str) -> str:
    return p[len(project_dir) + 1:] if project_dir and p.startswith(project_dir + "/") else p


# -------------------------------------------------------------------------- MAIN
def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=".")
    ap.add_argument("--json", default=None, help="write inventory JSON here (under OUTPUT_DIR)")
    args = ap.parse_args()
    repo = Path(args.repo).resolve()

    setenv = repo / "scripts" / "setenv.ksh"
    env = dict(re.findall(r"^\s*export\s+([A-Z_]+)=(\S+)", setenv.read_text(), re.MULTILINE)) if setenv.exists() else {}
    project_dir = env.get("AI_PROJECT_DIR", "")

    dmls = {p.name: parse_dml(p) for p in sorted((repo / "dml").glob("*.dml"))}
    psets = {str(p.relative_to(repo)): parse_pset(p) for p in sorted((repo / "psets").rglob("*.pset"))}
    pset_by_stem = {}
    for rel in psets:
        pset_by_stem.setdefault(Path(rel).stem, []).append(rel)
    mp_graphs = sorted(str(p.relative_to(repo)) for p in (repo / "graphs").rglob("*.mp"))
    py_graphs = sorted(str(p.relative_to(repo)) for p in (repo / "graphs").glob("*.py") if p.name != "__init__.py")

    pipelines = []
    referenced_psets, referenced_graphs = set(), set()
    for ksh in sorted((repo / "scripts").glob("*.ksh")):
        if ksh.name == "setenv.ksh":
            continue
        k = parse_ksh(ksh)
        for s in k["steps"]:
            g = to_repo_rel(expand(s["graph"] or "", k["vars"], env), project_dir)
            p = to_repo_rel(expand(s["pset"] or "", k["vars"], env), project_dir)
            stem = Path(p).stem
            found = pset_by_stem.get(stem, [])
            s.update({
                "graph_rel": g, "graph_in_repo": (repo / g).exists(),
                "pset_rel": p, "pset_in_repo": found,
                "pset_params": psets[found[0]] if found else None,
            })
            dml_ref = (psets[found[0]].get("DML_FILE") if found else None)
            s["dml_binding"] = {
                "explicit": to_repo_rel(dml_ref, project_dir) if dml_ref else None,
                "explicit_in_repo": (repo / to_repo_rel(dml_ref, project_dir)).exists() if dml_ref else None,
            }
            referenced_graphs.add(g); referenced_psets.add(stem)
        pipelines.append(k)

    unreferenced_psets = sorted(s for s in pset_by_stem if s not in referenced_psets)
    bound_dmls = {Path(s["dml_binding"]["explicit"]).name for k in pipelines for s in k["steps"] if s["dml_binding"]["explicit"]}
    unbound_dmls = sorted(d for d in dmls if d not in bound_dmls and not d.startswith("common_"))

    inv = {
        "repo": str(repo), "setenv": env,
        "graphs": {"mp_exports_in_repo": mp_graphs, "python_patterns": py_graphs,
                   "referenced_by_wrappers": sorted(referenced_graphs),
                   "referenced_but_missing": sorted(g for g in referenced_graphs if not (repo / g).exists())},
        "psets": {"present": sorted(psets), "referenced_stems": sorted(referenced_psets),
                  "referenced_but_missing": sorted(s for s in referenced_psets if s not in pset_by_stem),
                  "present_but_unreferenced": unreferenced_psets, "params": psets},
        "dml": {name: {k: v for k, v in d.items() if k != "types"} for name, d in dmls.items()},
        "dml_unbound": unbound_dmls,
        "pipelines": pipelines,
    }

    # ---- Markdown report
    out = []
    out.append(f"# Estate inventory — `{repo.name}`\n")
    out.append(f"- `.mp` graphs in repo: **{len(mp_graphs)}**; referenced by wrappers: **{len(referenced_graphs)}**; "
               f"missing: **{len(inv['graphs']['referenced_but_missing'])}**")
    out.append(f"- Python execution patterns in graphs/: {', '.join('`'+g+'`' for g in py_graphs) or 'none'}")
    out.append(f"- PSETs present: **{len(psets)}**; referenced: **{len(referenced_psets)}**; "
               f"missing: **{len(inv['psets']['referenced_but_missing'])}**; unreferenced: {', '.join('`'+u+'`' for u in unreferenced_psets) or 'none'}")
    out.append(f"- DML files: **{len(dmls)}**; explicitly bound: {', '.join('`'+b+'`' for b in sorted(bound_dmls)) or 'none'}; "
               f"unbound: {', '.join('`'+u+'`' for u in unbound_dmls) or 'none'}\n")
    for k in pipelines:
        out.append(f"## `{Path(k['file']).name}` — {k['job'] or 'job unknown'}"
                   f"{' · ' + k['cadence'] if k['cadence'] else ''}")
        if k["rc_swallowed_by_pipe"]:
            out.append("> FINDING: `| tee` without `set -o pipefail` — `set -e`/`$?` see tee's exit code, graph failures are swallowed.")
        out.append("\n| # | graph | in repo | pset | in repo | explicit DML | partition | params |\n|---|---|---|---|---|---|---|---|")
        for i, s in enumerate(k["steps"], 1):
            out.append(f"| {i} | `{s['graph_rel']}` | {'yes' if s['graph_in_repo'] else '**no**'} | `{s['pset_rel']}` | "
                       f"{'`'+s['pset_in_repo'][0]+'`' if s['pset_in_repo'] else '**no**'} | "
                       f"{'`'+s['dml_binding']['explicit']+'`' if s['dml_binding']['explicit'] else '—'} | "
                       f"{s['partition'] or '—'} | {', '.join(s['params']) or '—'} |")
        out.append("")
    out.append("## DML layouts\n\n| DML | columns (flattened) | delimiters | nested | vectors | conditional | fixed-width | parse findings |\n|---|---|---|---|---|---|---|---|")
    for name, d in dmls.items():
        out.append(f"| `{name}` | {', '.join(d['flat_columns']) or '(type-only)'} | {' '.join(repr(x) for x in d['delimiters']) or '—'} | "
                   f"{'yes' if d['has_nested_records'] else ''} | {'yes' if d['has_vectors'] else ''} | {'yes' if d['has_conditionals'] else ''} | "
                   f"{'yes' if d['is_fixed_width'] else ''} | {'; '.join(d['findings']) or '—'} |")
    print("\n".join(out))

    if args.json:
        Path(args.json).parent.mkdir(parents=True, exist_ok=True)
        Path(args.json).write_text(json.dumps(inv, indent=2, default=str), encoding="utf-8")
        print(f"\n(wrote {args.json})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
