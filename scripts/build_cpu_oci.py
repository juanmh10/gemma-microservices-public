#!/usr/bin/env python3
"""Package a prebuilt static CPU binary into a local OCI layout; never push or deploy."""

import argparse
import gzip
import hashlib
import io
import json
import subprocess
import tarfile
from pathlib import Path


def digest(data):
    return hashlib.sha256(data).hexdigest()


def source_digest():
    files = [Path("go.mod"), Path("go.sum")]
    for directory in [
        "worker-b",
        "cmd/dataset-preparer",
        "internal/gcs",
        "internal/preparation",
        "internal/benchmark",
        "prompts/worker-b",
        "schemas/analysis",
        "schemas/analytics",
        "schemas/messaging",
        "schemas/observability",
    ]:
        files.extend(
            p
            for p in Path(directory).rglob("*")
            if p.is_file()
            and "diagnostics" not in p.parts
            and p.suffix in {".go", ".mod", ".sum", ".txt", ".json"}
        )
    data = b"".join(
        str(p).encode() + b"\0" + p.read_bytes() + b"\0" for p in sorted(set(files))
    )
    return digest(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument(
        "--image-name",
        choices=["analysis", "analytics", "messaging", "dataset-preparer"],
        default="analysis",
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--certificates", type=Path, default=Path("/etc/ssl/certs/ca-certificates.crt")
    )
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    binary = args.binary.read_bytes()
    certificates = args.certificates.read_bytes()
    if binary[:4] != b"\x7fELF":
        parser.error("binary must be a static linux/amd64 ELF build")
    source_hash = source_digest()
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name in ["etc", "etc/ssl", "etc/ssl/certs", "tmp"]:
            entry = tarfile.TarInfo(name)
            entry.type = tarfile.DIRTYPE
            entry.mode = 0o1777 if name == "tmp" else 0o755
            archive.addfile(entry)
        for name, payload, mode in [
            ("analysis", binary, 0o555),
            ("etc/ssl/certs/ca-certificates.crt", certificates, 0o444),
        ]:
            entry = tarfile.TarInfo(name)
            entry.size = len(payload)
            entry.mode = mode
            archive.addfile(entry, io.BytesIO(payload))
    layer = buffer.getvalue()
    packed = gzip.compress(layer, mtime=0)
    config = {
        "architecture": "amd64",
        "os": "linux",
        "config": {
            "User": "65532:65532",
            "Entrypoint": ["/analysis"],
            "Env": ["SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt"],
            "WorkingDir": "/",
            "Labels": {
                "org.opencontainers.image.revision": revision,
                "gemma.source.sha256": source_hash,
            },
        },
        "rootfs": {"type": "layers", "diff_ids": ["sha256:" + digest(layer)]},
    }

    def blob(payload, media_type):
        path = args.output / "blobs/sha256" / digest(payload)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
        return {
            "mediaType": media_type,
            "digest": "sha256:" + digest(payload),
            "size": len(payload),
        }

    def encoded(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()

    manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": blob(encoded(config), "application/vnd.oci.image.config.v1+json"),
        "layers": [blob(packed, "application/vnd.oci.image.layer.v1.tar+gzip")],
    }
    descriptor = blob(encoded(manifest), manifest["mediaType"])
    (args.output / "index.json").write_bytes(
        encoded({"schemaVersion": 2, "manifests": [descriptor]})
    )
    (args.output / "oci-layout").write_text('{"imageLayoutVersion":"1.0.0"}\n')
    record = {
        "image": "us-central1-docker.pkg.dev/your-gcp-project-id/pipeline/"
        + args.image_name
        + "@"
        + descriptor["digest"],
        "source_revision": revision,
        "source_sha256": source_hash,
        "binary_sha256": digest(binary),
        "ca_bundle_sha256": digest(certificates),
        "publication_status": "local-only",
    }
    (args.output / "artifact.json").write_text(json.dumps(record, indent=2) + "\n")
    print(f"image={record['image']} publication_status=local-only")


if __name__ == "__main__":
    main()
