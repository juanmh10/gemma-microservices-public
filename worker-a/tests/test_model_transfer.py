"""Exercise the production staging function against a loopback Storage endpoint."""

import base64
import hashlib
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import multiprocessing
from pathlib import Path
import sys
from threading import Thread
from urllib.parse import parse_qs, urlparse

from google.api_core.exceptions import PreconditionFailed
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage
from google.cloud.storage.exceptions import DataCorruption
import google_crc32c
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from worker import MODEL_STAGE_LIMIT_BYTES, stage_rtx_model


@pytest.fixture
def local_gcs(request, monkeypatch):
    # Avoid forking the test's HTTP server thread; retain real process downloads.
    monkeypatch.setattr(
        "concurrent.futures.ProcessPoolExecutor",
        partial(ProcessPoolExecutor, mp_context=multiprocessing.get_context("spawn")),
    )
    # Cross the production 32 MiB chunk boundary without allocating model weights.
    payload = b"0123456789abcdef" * (2 * 1024 * 1024) + b"final partial chunk"
    mode = request.param
    ranges = []
    checksum = base64.b64encode(google_crc32c.Checksum(payload).digest()).decode(
        "ascii"
    )

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _format, *_args):
            pass

        def do_GET(self):
            parsed = urlparse(self.path)
            if parsed.path.startswith("/storage/v1/b/local-models/o/"):
                metadata = {
                    "name": "rtx/model.safetensors",
                    "size": str(
                        MODEL_STAGE_LIMIT_BYTES + 1
                        if mode == "oversized"
                        else len(payload)
                    ),
                    "generation": "7",
                    "crc32c": checksum,
                    "mediaLink": f"{endpoint}/weights",
                }
                self.respond(200, json.dumps(metadata).encode(), "application/json")
                return
            if parsed.path != "/weights":
                self.respond(404, b"Unknown local test endpoint")
                return
            generation = parse_qs(parsed.query).get("ifGenerationMatch")
            ranges.append((self.headers.get("Range"), generation))
            if generation != ["7"] or mode == "generation_changed":
                self.respond(
                    412,
                    b'{"error":{"code":412,"message":"Generation changed"}}',
                    "application/json",
                )
                return
            start, end = map(
                int, self.headers["Range"].removeprefix("bytes=").split("-")
            )
            body = payload[start : end + 1]
            if mode == "corrupted" and start == 0:
                body = b"X" + body[1:]
            self.respond(
                206,
                body,
                content_range=f"bytes {start}-{end}/{len(payload)}",
            )

        def respond(
            self,
            status,
            body,
            content_type="application/octet-stream",
            content_range=None,
        ):
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            if content_range:
                self.send_header("Content-Range", content_range)
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    endpoint = f"http://127.0.0.1:{server.server_port}"
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    client = storage.Client(
        project="local-test",
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": endpoint},
        use_auth_w_custom_endpoint=False,
    )
    try:
        yield client, payload, ranges, mode
    finally:
        client.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


@pytest.mark.parametrize(
    "local_gcs",
    ["success", "corrupted", "manifest_mismatch", "generation_changed", "oversized"],
    indirect=True,
)
def test_production_staging_with_local_storage(local_gcs, tmp_path):
    client, payload, ranges, mode = local_gcs
    mounted = tmp_path / "mounted"
    mounted.mkdir()
    (mounted / "config.json").write_text('{"model_type":"gemma"}')
    staged = tmp_path / "staged"
    staged.mkdir()
    manifest = {
        "files": [
            {"path": "config.json"},
            {
                "path": "model.safetensors",
                "sha256": "0" * 64
                if mode == "manifest_mismatch"
                else hashlib.sha256(payload).hexdigest(),
            },
        ]
    }
    arguments = (
        client,
        "gs://local-models/rtx/model-manifest.json",
        manifest,
        mounted,
        staged,
    )
    if mode == "success":
        model_dir, stats = stage_rtx_model(*arguments)
        assert (model_dir / "model.safetensors").read_bytes() == payload
        assert (model_dir / "config.json").read_bytes() == (
            mounted / "config.json"
        ).read_bytes()
        assert (
            stats["staged_bytes"]
            == len(payload) + (mounted / "config.json").stat().st_size
        )
        assert stats["weight_files"] == 1
    else:
        exception, match = {
            "corrupted": (DataCorruption, "checksum"),
            "manifest_mismatch": (ValueError, "checksum mismatch"),
            "generation_changed": (PreconditionFailed, "Generation changed"),
            "oversized": (ValueError, "volume limit"),
        }[mode]
        with pytest.raises(exception, match=match):
            stage_rtx_model(*arguments)

    if mode == "oversized":
        assert ranges == []
        assert not (staged / "model.safetensors").exists()
    else:
        assert sorted(header for header, _ in ranges) == [
            "bytes=0-33554431",
            f"bytes=33554432-{len(payload) - 1}",
        ]
        assert all(generation == ["7"] for _, generation in ranges)
