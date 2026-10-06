"""Bounded export and compatibility checks for image-baked vLLM artifacts."""

from __future__ import annotations

import hashlib
import importlib.metadata
import io
import json
import os
from pathlib import Path
import platform
import subprocess
import tarfile
import time

CACHE_ROOT = Path("/root/.cache/vllm/torch_compile_cache")
MANIFEST_PATH = Path("/app/compile-cache-manifest.json")
MAX_CACHE_BYTES = 2 * 1024**3
MAX_CACHE_FILES = 10000


class DeadlineReader(io.BufferedReader):
    """Check the soft deadline between resumable upload chunks."""

    def __init__(self, path: Path, deadline: float):
        super().__init__(io.FileIO(path, "r"))
        self.deadline = deadline

    def read(self, size=-1):
        if time.monotonic() >= self.deadline:
            raise TimeoutError("Compilation cache upload deadline exceeded")
        return super().read(size)


def file_sha256(path: Path) -> str:
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024**2), b""):
            value.update(block)
    return value.hexdigest()


def compatibility(model_info: dict, engine_args: dict, profile: str) -> dict:
    """Capture inputs without initializing CUDA in the worker parent process."""
    gpu = subprocess.run(
        [
            "nvidia-smi",
            "--query-gpu=name,driver_version,compute_cap",
            "--format=csv,noheader,nounits",
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=5,
    ).stdout.strip()
    if not gpu or "\n" in gpu or profile != "rtx6000":
        raise ValueError("Compilation cache requires exactly one RTX GPU")
    return {
        "model_manifest_sha256": hashlib.sha256(
            json.dumps(model_info, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "engine_args": json.loads(json.dumps(engine_args, sort_keys=True)),
        "gpu_profile": profile,
        "gpu": gpu,
        "python": platform.python_version(),
        "packages": {
            name: importlib.metadata.version(name)
            for name in ("vllm", "torch", "triton")
        },
        "worker_sha256": file_sha256(Path(__file__).with_name("worker.py")),
        "domain_module_sha256": file_sha256(Path(__file__).with_name("domain.py")),
        "cache_module_sha256": file_sha256(Path(__file__)),
        "base_image": os.environ.get("WORKER_VLLM_BASE_IMAGE", ""),
        "requirements_sha256": file_sha256(Path("/app/requirements.lock")),
        "vllm_v2_runner": os.environ.get("VLLM_USE_V2_MODEL_RUNNER", ""),
        "cache_root": str(CACHE_ROOT),
    }


def inventory(root: Path, deadline: float) -> list[dict]:
    files = []
    total = 0
    if root.is_symlink() or not root.is_dir():
        raise ValueError("Compilation cache directory is missing")
    for path in sorted(root.rglob("*")):
        if time.monotonic() >= deadline:
            raise TimeoutError("Compilation cache deadline exceeded")
        if path.is_symlink():
            raise ValueError("Compilation cache must not contain symlinks")
        if path.is_dir():
            continue
        if not path.is_file():
            raise ValueError("Compilation cache must contain regular files only")
        size = path.stat().st_size
        total += size
        if total > MAX_CACHE_BYTES or len(files) >= MAX_CACHE_FILES:
            raise ValueError("Compilation cache exceeds its size or file limit")
        files.append(
            {
                "path": path.relative_to(root).as_posix(),
                "size": size,
                "sha256": file_sha256(path),
            }
        )
    if not any(
        item["path"].startswith("torch_aot_compile/")
        and item["path"].endswith("/model")
        and item["size"] > 0
        for item in files
    ):
        raise ValueError("Compilation cache has no AOT model artifact")
    return files


def validate(
    expected: dict,
    deadline: float,
    manifest_path: Path = MANIFEST_PATH,
    root: Path = CACHE_ROOT,
) -> dict:
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("schema_version") != 1 or manifest.get("compatibility") != expected:
        raise ValueError("Baked compilation cache is incompatible with this task")
    if manifest.get("files") != inventory(root, deadline):
        raise ValueError("Baked compilation cache failed integrity verification")
    if os.environ.get("VLLM_DISABLE_COMPILE_CACHE", "0") != "0":
        raise ValueError("Baked compilation cache cannot be disabled")
    if os.environ.get("VLLM_CACHE_ROOT", "/root/.cache/vllm") != "/root/.cache/vllm":
        raise ValueError("Baked compilation cache path cannot be relocated")
    os.environ["VLLM_USE_AOT_COMPILE"] = "1"
    os.environ["VLLM_FORCE_AOT_LOAD"] = "1"
    return {
        "mode": "baked_aot_required",
        "files": len(manifest["files"]),
        "bytes": sum(item["size"] for item in manifest["files"]),
    }


def export(client, prefix: str, expected: dict, deadline: float) -> dict:
    """Publish a create-only bundle; existing predictions remain independently durable."""
    files = inventory(CACHE_ROOT, deadline)
    archive = Path("/tmp/compile-cache.tar")
    try:
        with tarfile.open(archive, "w") as bundle:
            for item in files:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Compilation cache export deadline exceeded")
                source = CACHE_ROOT / item["path"]
                if file_sha256(source) != item["sha256"]:
                    raise ValueError("Compilation cache changed during export")
                bundle.add(source, arcname=item["path"], recursive=False)
        manifest = {
            "schema_version": 1,
            "compatibility": expected,
            "files": files,
            "archive_sha256": file_sha256(archive),
        }
        from urllib.parse import urlparse

        parsed = urlparse(prefix)
        if parsed.scheme != "gs" or not parsed.netloc:
            raise ValueError("Cache export requires a GCS results prefix")
        bucket = client.bucket(parsed.netloc)
        key = parsed.path.lstrip("/") + "/compile-cache"
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Compilation cache upload deadline exceeded")
        with DeadlineReader(archive, deadline) as source:
            bucket.blob(key + ".tar").upload_from_file(
                source,
                size=archive.stat().st_size,
                if_generation_match=0,
                timeout=min(60, remaining),
                retry=None,
                content_type="application/x-tar",
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Compilation cache manifest deadline exceeded")
        bucket.blob(key + "-manifest.json").upload_from_string(
            json.dumps(manifest, sort_keys=True).encode(),
            if_generation_match=0,
            timeout=min(30, remaining),
            retry=None,
            content_type="application/json",
        )
        return {
            "mode": "exported",
            "archive_uri": prefix + "/compile-cache.tar",
            "manifest_uri": prefix + "/compile-cache-manifest.json",
            "archive_sha256": manifest["archive_sha256"],
            "files": len(files),
            "bytes": sum(item["size"] for item in files),
        }
    finally:
        archive.unlink(missing_ok=True)


def unpack(archive: Path, manifest_path: Path, destination: Path) -> None:
    """Prepare a Docker build context from a trusted, checksummed GPU export."""
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("schema_version") != 1:
        raise ValueError("Unsupported compilation cache manifest")
    if file_sha256(archive) != manifest["archive_sha256"]:
        raise ValueError("Compilation cache archive checksum mismatch")
    if destination.exists():
        raise ValueError("Use a new cache build context directory")
    if archive.stat().st_size > MAX_CACHE_BYTES + MAX_CACHE_FILES * 2048:
        raise ValueError("Compilation cache archive exceeds its limits")
    with tarfile.open(archive, "r:") as bundle:
        members = []
        names = set()
        total = 0
        for member in bundle:
            total += member.size
            if (
                len(members) >= MAX_CACHE_FILES
                or total > MAX_CACHE_BYTES
                or member.name in names
            ):
                raise ValueError("Compilation cache archive exceeds its limits")
            path = Path(member.name)
            if not member.isfile() or path.is_absolute() or ".." in path.parts:
                raise ValueError("Unsafe compilation cache archive member")
            members.append(member)
            names.add(member.name)
        destination.mkdir(parents=True)
        root = destination / "cache"
        bundle.extractall(root, members=members, filter="data")
    if inventory(root, time.monotonic() + 120) != manifest["files"]:
        raise ValueError("Extracted compilation cache failed integrity verification")
    (destination / "compile-cache-manifest.json").write_text(
        json.dumps(manifest, sort_keys=True)
    )


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path)
    args = parser.parse_args()
    unpack(args.archive, args.manifest, args.destination)
