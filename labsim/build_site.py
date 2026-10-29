"""Build the static site for GitHub Pages: the web app plus the labsim
sources, which the browser runs with Pyodide (see web/pyworker.js).

  python -m labsim build-site --out _site
"""

import json
import shutil
from pathlib import Path

PACKAGE = Path(__file__).parent
WEB = PACKAGE / "web"
FINDINGS = PACKAGE.parent / "FINDINGS.md"


def build(out):
    out = Path(out)
    if out.exists():
        shutil.rmtree(out)
    (out / "static").mkdir(parents=True)
    (out / "py" / "labsim").mkdir(parents=True)

    shutil.copy(WEB / "index.html", out / "index.html")
    for f in WEB.iterdir():
        if f.is_file() and f.name != "index.html":
            shutil.copy(f, out / "static" / f.name)

    files = []
    for f in sorted(PACKAGE.glob("*.py")):
        shutil.copy(f, out / "py" / "labsim" / f.name)
        files.append(f"labsim/{f.name}")
    if FINDINGS.exists():
        shutil.copy(FINDINGS, out / "py" / "FINDINGS.md")
        files.append("FINDINGS.md")
    (out / "py" / "manifest.json").write_text(json.dumps({"files": files}, indent=1), encoding="utf-8")
    (out / ".nojekyll").write_text("", encoding="utf-8")   # serve files as-is, no Jekyll processing
    return out
