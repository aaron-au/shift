#!/usr/bin/env python3
"""Render NOTICE from a newline-separated module list on stdin.

Classifies each module by reading its own LICENSE/COPYING text — never by a
hardcoded table, which would silently go stale when a dependency relicenses.
An unrecognised licence is emitted as "see upstream" so it shows up as work to
do rather than passing quietly.
"""

import os
import re
import subprocess
import sys

ORDER = ["Apache-2.0", "MIT", "BSD-3-Clause", "BSD-2-Clause", "ISC"]

NOTES = {
    "Apache-2.0": (
        "Apache License 2.0 — https://www.apache.org/licenses/LICENSE-2.0\n"
        "Section 4(d) requires that the NOTICE files of these components travel with\n"
        "any distribution; where a component ships its own NOTICE it is marked below."
    ),
    "MIT": (
        "MIT License — the copyright notice and permission notice of each component\n"
        "are retained in the component's own source distribution."
    ),
    "BSD-3-Clause": (
        "BSD 3-Clause License — redistribution requires the copyright notice, this\n"
        "list of conditions and the disclaimer; the name of the copyright holder may\n"
        "not be used to endorse products derived from the software."
    ),
    "BSD-2-Clause": (
        "BSD 2-Clause License — redistribution requires the copyright notice, this\n"
        "list of conditions and the disclaimer."
    ),
    "ISC": (
        "ISC License — the copyright notice and permission notice are retained in\n"
        "the component's own source distribution."
    ),
}

HEADER = """SHIFT — third-party notices
===========================

SHIFT is proprietary software (see LICENSE). This file exists to satisfy the
attribution terms of the open-source components it links against, and is
distributed with every SHIFT binary and container image.

The list covers the modules that actually ship in the hubd, runnerd and
connector binaries (Go's build graph, `go list -deps`), not the wider module
graph. All are permissive licences; none imposes a source-disclosure or
reciprocity obligation on SHIFT itself.

Regenerate with `make notice` after changing dependencies.
"""

FOOTER = """Runtime components (separate processes, not linked)
--------------------------------------------------

The "just runs" bundle (deploy/) starts these as their own containers. They are
not linked into any SHIFT binary and impose no obligation on it:

  PostgreSQL      PostgreSQL License (permissive, BSD-like)
  Dex             Apache License 2.0
"""

MODULE_DIRS = ("hub", "runner", "connectors", "engine", "sdk", "pkg")


def module_dir(mod: str) -> str:
    for wd in MODULE_DIRS:
        out = subprocess.run(
            ["go", "list", "-m", "-f", "{{.Dir}}", mod],
            capture_output=True, text=True, cwd=wd,
        ).stdout.strip()
        if out and os.path.isdir(out):
            return out
    return ""


def classify(d: str):
    """Return (licence, ships_own_notice) for a module directory."""
    if not d:
        return "", False
    files = os.listdir(d)
    ships_notice = any(re.match(r"(?i)^notice", f) for f in files)
    for f in sorted(files):
        if not re.match(r"(?i)^(licen[cs]e|copying)", f):
            continue
        head = open(os.path.join(d, f), errors="replace").read(2000)
        if "Apache License" in head:
            return "Apache-2.0", ships_notice
        if re.search(r"MIT License", head, re.I) or "Permission is hereby granted, free of charge" in head:
            return "MIT", ships_notice
        if "Redistribution and use in source and binary" in head:
            return ("BSD-3-Clause" if "Neither the name" in head else "BSD-2-Clause"), ships_notice
        if "Permission to use, copy, modify" in head:
            return "ISC", ships_notice
        break
    return "", ships_notice


def main() -> int:
    mods = [m.strip() for m in sys.stdin if m.strip()]
    by_licence: dict[str, list[tuple[str, bool]]] = {}
    for m in mods:
        lic, ships = classify(module_dir(m))
        by_licence.setdefault(lic or "see upstream", []).append((m, ships))

    parts = [HEADER]
    seen = set()
    for lic in ORDER + sorted(k for k in by_licence if k not in ORDER):
        if lic not in by_licence or lic in seen:
            continue
        seen.add(lic)
        parts.append(f"{lic}\n{'-' * len(lic)}\n")
        if lic in NOTES:
            parts.append(NOTES[lic] + "\n")
        for mod, ships in sorted(by_licence[lic]):
            parts.append(f"  {mod}" + ("   (ships a NOTICE file)" if ships else ""))
        parts.append("")
    parts.append(FOOTER)

    with open("NOTICE", "w") as fh:
        fh.write("\n".join(parts))

    if "see upstream" in by_licence:
        names = ", ".join(m for m, _ in by_licence["see upstream"])
        print(f"notice: WARNING unclassified licence for: {names}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
