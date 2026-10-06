"""Check maintained Markdown links and public documentation privacy boundaries."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote, urlparse

ROOT = Path(__file__).resolve().parent.parent
DOCUMENTS = [
    ROOT / name for name in ["README.md", "PRD.md", "AGENTS.md", "CHANGELOG.md"]
]
DOCUMENTS += sorted((ROOT / "docs").rglob("*.md"))
DOCUMENTS += [
    ROOT / "data/README.md",
    ROOT / "powerbi/README.md",
    ROOT / "diagnostics/README.md",
]
LINK = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
PRIVATE = re.compile(
    r"(?i:[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})"
    r"|/home/[^/\s]+|/Users/[^/\s]+"
    r"|llm-api-\d+|\b(?:user|serviceAccount):"
    r"|\b(?:export|env)\s+[A-Z_][A-Z0-9_]*="
    r"|\b[A-Z_][A-Z0-9_]*=(?:[^=]|$)",
)


def check() -> list[str]:
    findings = []
    for document in DOCUMENTS:
        for number, line in enumerate(document.read_text().splitlines(), 1):
            if PRIVATE.search(line):
                findings.append(
                    f"{document.relative_to(ROOT)}:{number}: private configuration text"
                )
            for target in LINK.findall(line):
                target = target.strip().strip("<>")
                if urlparse(target).scheme or target.startswith("#"):
                    continue
                path = unquote(target.split("#", 1)[0])
                if path and not (document.parent / path).exists():
                    findings.append(
                        f"{document.relative_to(ROOT)}:{number}: broken local link"
                    )
    return findings


def main() -> int:
    findings = check()
    if findings:
        print("\n".join(findings))
        return 1
    print("Documentation links and privacy boundaries passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
