"""Publication policy checks use only temporary synthetic fixtures."""

import hashlib
import importlib.util
import tempfile
import unittest
from pathlib import Path

MODULE = Path(__file__).resolve().parents[1] / "check_repository_artifacts.py"
spec = importlib.util.spec_from_file_location("artifacts", MODULE)
artifacts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(artifacts)


class ArtifactPolicyTests(unittest.TestCase):
    def test_manifest_cannot_expand_the_dataset_exception(self):
        manifest = {
            category: {name: "a" * 64 for name in paths}
            for category, paths in artifacts.APPROVED_PATHS.items()
        }
        self.assertEqual(artifacts.check_manifest(manifest), [])
        manifest["synthetic_data"]["data/customer.jsonl"] = "b" * 64
        self.assertTrue(artifacts.check_manifest(manifest))

    def test_private_and_unknown_dataset_artifacts_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            paths = [
                Path(p)
                for p in [
                    "report.pbix",
                    "private.tfstate.backup",
                    "settings.tfvars",
                    "results/report.json",
                    "data/customer.jsonl",
                    "image.png",
                ]
            ]
            for p in paths:
                (root / p).parent.mkdir(parents=True, exist_ok=True)
                (root / p).write_bytes(b"\xff\x00" if p.suffix == ".png" else b"{}")
            findings = artifacts.check(
                root, paths, {"synthetic_data": {}, "reviewed_images": {}}
            )
            self.assertEqual(len(findings), len(paths))

    def test_exact_pin_is_required_even_for_synthetic_data(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            p = Path("data/approved.jsonl")
            (root / p).parent.mkdir()
            (root / p).write_bytes(b'{"record_id":"synthetic"}\n')
            manifest = {
                "synthetic_data": {
                    p.as_posix(): hashlib.sha256((root / p).read_bytes()).hexdigest()
                },
                "reviewed_images": {},
            }
            self.assertEqual(artifacts.check(root, [p], manifest), [])
            (root / p).write_bytes(b'{"record_id":"different"}\n')
            self.assertTrue(artifacts.check(root, [p], manifest))
            self.assertTrue(artifacts.check(root, [], manifest))

    def test_symlink_cannot_publish_an_ignored_private_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            private = root / "results/private.json"
            private.parent.mkdir()
            private.write_text("{}")
            (root / "config.json").symlink_to(private)
            self.assertTrue(
                artifacts.check(
                    root,
                    [Path("config.json")],
                    {"synthetic_data": {}, "reviewed_images": {}},
                )
            )


if __name__ == "__main__":
    unittest.main()
