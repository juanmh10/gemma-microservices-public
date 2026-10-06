"""Create a checksum manifest for an already downloaded model snapshot."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("--model-id", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--quantization", required=True)
    parser.add_argument("--vllm-version", required=True)
    args = parser.parse_args()
    root = args.directory.resolve()
    files = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.name == "model-manifest.json":
            continue
        if path.is_symlink() or ".cache" in path.relative_to(root).parts:
            continue
        with path.open("rb") as source:
            checksum = hashlib.file_digest(source, "sha256").hexdigest()
        files.append({"path": path.relative_to(root).as_posix(), "sha256": checksum})
    if not files:
        raise ValueError("No model files found")
    manifest = {
        "model_id": args.model_id,
        "revision": args.revision,
        "quantization": args.quantization,
        "vllm_version": args.vllm_version,
        "files": files,
    }
    target = root / "model-manifest.json"
    target.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {target} with {len(files)} files")


if __name__ == "__main__":
    main()
