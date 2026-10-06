"""Reject private/generated artifacts and verify the approved synthetic data pins."""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
MANIFEST = ROOT / "repository-assets.json"
APPROVED_PATHS = {
    "synthetic_data": {
        "data/source/gym_v3_dataset_clean.jsonl",
        "data/source/gym_v3_metadata.jsonl",
        "data/prepared/gym-sales-v1.jsonl",
        "data/ground-truth/gym-sales-v1.jsonl",
    },
    "reviewed_images": {"powerbi/power-bi.png"},
}
PRIVATE_PARTS = {".git", ".terraform", ".venv", "results", "models", "__pycache__"}
PRIVATE_SUFFIXES = {
    ".pbix",
    ".pbit",
    ".parquet",
    ".arrow",
    ".safetensors",
    ".gguf",
    ".bin",
    ".tfplan",
    ".pyc",
    ".log",
    ".pem",
    ".key",
    ".p12",
    ".pfx",
    ".zip",
}


def repository_paths(root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z"],
        cwd=root,
        capture_output=True,
        check=True,
    )
    return sorted({Path(p.decode()) for p in result.stdout.split(b"\0") if p})


def check_manifest(manifest: dict) -> list[str]:
    if set(manifest) != set(APPROVED_PATHS):
        return [
            "repository-assets.json: asset categories differ from the approved policy"
        ]
    for category, paths in APPROVED_PATHS.items():
        entries = manifest[category]
        if not isinstance(entries, dict) or set(entries) != paths:
            return [
                "repository-assets.json: asset paths differ from the approved policy"
            ]
        if any(
            not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{64}", value)
            for value in entries.values()
        ):
            return [
                "repository-assets.json: each approved asset requires a SHA-256 pin"
            ]
    return []


def check(root: Path, paths: list[Path], manifest: dict) -> list[str]:
    findings = []
    pins = manifest["synthetic_data"] | manifest["reviewed_images"]
    for path in paths:
        file = root / path
        name = path.as_posix()
        if file.is_symlink():
            findings.append(f"{name}: symlinks require an explicit publication policy")
            continue
        private = (
            bool(PRIVATE_PARTS.intersection(path.parts))
            or path.suffix.lower() in PRIVATE_SUFFIXES
            or ".tfstate" in path.name
            or (path.name.startswith(".env") and path.name != ".env.example")
            or (path.name.endswith((".tfvars", ".tfvars.json")))
            or path.name in {"credentials.json", "application_default_credentials.json"}
            or name
            in {"pipeline", "analysis", "analytics", "messaging", "dataset-preparer"}
        )
        if private:
            findings.append(
                f"{name}: private or generated artifact cannot be published"
            )
            continue
        if not file.is_file():
            continue  # Source deletions are not working-tree publication candidates.
        data = file.read_bytes()
        if name in pins:
            if hashlib.sha256(data).hexdigest() != pins[name]:
                findings.append(
                    f"{name}: approved asset hash changed; review its provenance/content"
                )
            continue
        if path.parts[0] == "data" and path.suffix != ".md":
            findings.append(
                f"{name}: dataset is outside the pinned synthetic exception"
            )
            continue
        try:
            data.decode("utf-8")
        except UnicodeDecodeError:
            findings.append(
                f"{name}: binary asset has no reviewed publication exception"
            )
    for name in pins:
        if not (root / name).is_file() or Path(name) not in paths:
            findings.append(
                f"{name}: approved asset missing from publication candidates"
            )
    return findings


def main() -> int:
    manifest = json.loads(MANIFEST.read_text())
    findings = check_manifest(manifest)
    if not findings:
        findings = check(ROOT, repository_paths(ROOT), manifest)
    if findings:
        print("\n".join(findings))
        return 1
    print("Repository artifact policy and synthetic asset pins passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
