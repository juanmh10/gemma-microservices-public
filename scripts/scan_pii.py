"""Flag likely personal identifiers in repository text files."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SKIP_PARTS = {".git", ".terraform", ".venv", "results"}
SKIP_SUFFIXES = {".png", ".jpg", ".jpeg", ".pdf", ".parquet", ".bin"}
EMAIL = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
LOCAL_USER_PATH = re.compile(
    r"(?:/(?:home|Users)/[A-Za-z0-9._-]+/|[A-Z]:\\Users\\[A-Za-z0-9._-]+\\)", re.I
)
CPF = re.compile(r"(?<!\d)\d{3}[. ]?\d{3}[. ]?\d{3}[- ]?\d{2}(?!\d)")


def valid_cpf(value: str) -> bool:
    digits = [int(char) for char in value if char.isdigit()]
    if len(digits) != 11 or len(set(digits)) == 1:
        return False
    for size in (9, 10):
        check = (
            sum(digit * (size + 1 - index) for index, digit in enumerate(digits[:size]))
            * 10
        ) % 11
        if (0 if check == 10 else check) != digits[size]:
            return False
    return True


def main() -> int:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=ROOT,
        capture_output=True,
        check=True,
    )
    findings = []
    for raw_name in result.stdout.split(b"\0"):
        if not raw_name:
            continue
        path = Path(raw_name.decode("utf-8", errors="replace"))
        if (
            any(part in SKIP_PARTS for part in path.parts)
            or path.suffix.lower() in SKIP_SUFFIXES
        ):
            continue
        absolute = ROOT / path
        if not absolute.is_file():
            continue
        try:
            with absolute.open(encoding="utf-8") as stream:
                for number, line in enumerate(stream, 1):
                    if (
                        EMAIL.search(line)
                        or LOCAL_USER_PATH.search(line)
                        or any(valid_cpf(match.group()) for match in CPF.finditer(line))
                    ):
                        findings.append(f"{path}:{number}")
        except UnicodeDecodeError:
            continue
    if findings:
        print("Possible personal identifiers found at:")
        print("\n".join(findings))
        return 1
    print("PII pattern scan passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
