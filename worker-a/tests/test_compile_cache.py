import io
import json
import tarfile
import time
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import compile_cache


def test_fingerprint_keeps_original_engine_arguments(monkeypatch):
    monkeypatch.setattr(
        compile_cache.subprocess,
        "run",
        lambda *a, **kw: Mock(stdout="RTX PRO 6000, driver, 12.0"),
    )
    monkeypatch.setattr(
        compile_cache.importlib.metadata, "version", lambda name: "pinned"
    )
    monkeypatch.setattr(compile_cache, "file_sha256", lambda path: "synthetic-sha")
    args = {"hf_overrides": {"diffusion_sampler": "entropy_bound"}}
    expected = compile_cache.compatibility({"revision": "pinned"}, args, "rtx6000")
    args["hf_overrides"]["diffusion_sampler"] = "changed-by-engine"
    assert (
        expected["engine_args"]["hf_overrides"]["diffusion_sampler"] == "entropy_bound"
    )


def cache_fixture(tmp_path):
    root = tmp_path / "cache"
    artifact = root / "torch_aot_compile/key/rank_0_0/model"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"synthetic-aot")
    expected = {"model": "pinned-model", "gpu": "synthetic-rtx"}
    manifest = {
        "schema_version": 1,
        "compatibility": expected,
        "files": compile_cache.inventory(root, time.monotonic() + 10),
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    return root, path, expected


def test_valid_cache_requires_aot_loading(tmp_path, monkeypatch):
    root, path, expected = cache_fixture(tmp_path)
    monkeypatch.delenv("VLLM_CACHE_ROOT", raising=False)
    monkeypatch.delenv("VLLM_DISABLE_COMPILE_CACHE", raising=False)
    monkeypatch.setenv("VLLM_FORCE_AOT_LOAD", "0")
    monkeypatch.setenv("VLLM_USE_AOT_COMPILE", "0")
    result = compile_cache.validate(expected, time.monotonic() + 10, path, root)
    assert result["mode"] == "baked_aot_required"
    assert result["bytes"] == len(b"synthetic-aot")
    import os

    assert os.environ["VLLM_FORCE_AOT_LOAD"] == "1"
    assert os.environ["VLLM_USE_AOT_COMPILE"] == "1"


def test_changed_model_or_gpu_is_rejected(tmp_path):
    root, path, expected = cache_fixture(tmp_path)
    with pytest.raises(ValueError, match="incompatible"):
        compile_cache.validate(
            {**expected, "gpu": "other"}, time.monotonic() + 10, path, root
        )


def test_corruption_is_rejected(tmp_path):
    root, path, expected = cache_fixture(tmp_path)
    next(root.rglob("model")).write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="integrity"):
        compile_cache.validate(expected, time.monotonic() + 10, path, root)


def test_symlinks_and_oversized_cache_are_rejected(tmp_path, monkeypatch):
    root, _, _ = cache_fixture(tmp_path)
    (root / "link").symlink_to("/etc/passwd")
    with pytest.raises(ValueError, match="symlinks"):
        compile_cache.inventory(root, time.monotonic() + 10)
    (root / "link").unlink()
    monkeypatch.setattr(compile_cache, "MAX_CACHE_BYTES", 1)
    with pytest.raises(ValueError, match="limit"):
        compile_cache.inventory(root, time.monotonic() + 10)


def test_deadline_and_missing_aot_are_rejected(tmp_path):
    root, _, _ = cache_fixture(tmp_path)
    with pytest.raises(TimeoutError):
        compile_cache.inventory(root, time.monotonic() - 1)
    next(root.rglob("model")).unlink()
    with pytest.raises(ValueError, match="no AOT"):
        compile_cache.inventory(root, time.monotonic() + 10)


def test_export_is_create_only_and_unpack_round_trip(tmp_path, monkeypatch):
    root, _, expected = cache_fixture(tmp_path)
    monkeypatch.setattr(compile_cache, "CACHE_ROOT", root)
    client = Mock()
    saved = {}

    def blob(name):
        result = Mock()

        def upload_file(source, **kwargs):
            assert kwargs["if_generation_match"] == 0
            assert kwargs["retry"] is None
            saved["archive"] = source.read()

        def upload_string(data, **kwargs):
            assert kwargs["if_generation_match"] == 0
            assert kwargs["retry"] is None
            saved["manifest"] = data

        result.upload_from_file.side_effect = upload_file
        result.upload_from_string.side_effect = upload_string
        return result

    client.bucket.return_value.blob.side_effect = blob
    result = compile_cache.export(
        client, "gs://results/isolated/task-00000", expected, time.monotonic() + 10
    )
    assert result["files"] == 1
    archive = tmp_path / "bundle.tar"
    archive.write_bytes(saved["archive"])
    manifest = tmp_path / "export.json"
    manifest.write_bytes(saved["manifest"])
    destination = tmp_path / "build"
    compile_cache.unpack(archive, manifest, destination)
    assert (
        destination / "cache/torch_aot_compile/key/rank_0_0/model"
    ).read_bytes() == b"synthetic-aot"


@pytest.mark.parametrize("name", ["../escaped", "/tmp/escaped"])
def test_unpack_rejects_path_traversal(tmp_path, name):
    archive = tmp_path / "unsafe.tar"
    with tarfile.open(archive, "w") as bundle:
        member = tarfile.TarInfo(name)
        member.size = 1
        bundle.addfile(member, io.BytesIO(b"x"))
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {"schema_version": 1, "archive_sha256": compile_cache.file_sha256(archive)}
        )
    )
    with pytest.raises(ValueError, match="Unsafe"):
        compile_cache.unpack(archive, manifest, tmp_path / "build")
    assert not (tmp_path / "build").exists()


def test_unpack_rejects_archive_corruption(tmp_path):
    archive = tmp_path / "bad.tar"
    archive.write_bytes(b"not-an-archive")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"schema_version": 1, "archive_sha256": "0" * 64}))
    with pytest.raises(ValueError, match="checksum"):
        compile_cache.unpack(archive, manifest, tmp_path / "build")


def test_upload_failure_cleans_temporary_archive(tmp_path, monkeypatch):
    root, _, expected = cache_fixture(tmp_path)
    monkeypatch.setattr(compile_cache, "CACHE_ROOT", root)
    client = Mock()
    client.bucket.return_value.blob.return_value.upload_from_file.side_effect = (
        RuntimeError("upload failed")
    )
    with pytest.raises(RuntimeError, match="upload failed"):
        compile_cache.export(
            client, "gs://results/task", expected, time.monotonic() + 10
        )
    assert not Path("/tmp/compile-cache.tar").exists()
