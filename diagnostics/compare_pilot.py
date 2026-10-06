"""Compare completed Worker A profiles against isolated ground truth."""

from __future__ import annotations

import argparse
import io
import json
from collections import defaultdict
from urllib.parse import urlparse

import pyarrow.parquet as pq
from google.cloud import storage


RATES = {
    "l4": 4 * 0.000011244 + 16 * 0.000001235 + 0.0001867,
    "rtx6000": 20 * 0.000011244 + 80 * 0.000001235 + 0.00036522,
}


def split_gcs(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "gs" or not parsed.netloc:
        raise ValueError(f"Expected gs:// URI: {uri}")
    return parsed.netloc, parsed.path.lstrip("/")


def read_truth(client: storage.Client, uri: str) -> dict:
    bucket, name = split_gcs(uri)
    data = client.bucket(bucket).blob(name).download_as_bytes()
    table = pq.read_table(io.BytesIO(data))
    return {
        row["record_id"]: {
            "outcome": row["outcome"].lower(),
            "causes": set(json.loads(row["metadata_json"])["hidden_objection_ids"]),
        }
        for row in table.to_pylist()
    }


def read_profile(
    client: storage.Client, prefix: str, profile: str
) -> tuple[list[dict], list[dict]]:
    bucket, name = split_gcs(prefix.rstrip("/") + "/" + profile + "/")
    rows, markers = [], []
    for blob in client.list_blobs(bucket, prefix=name):
        if blob.name.endswith(".parquet"):
            rows.extend(pq.read_table(io.BytesIO(blob.download_as_bytes())).to_pylist())
        elif blob.name.endswith("/_SUCCESS.json") or blob.name.endswith(
            "/_ABORTED.json"
        ):
            markers.append(json.loads(blob.download_as_bytes()))
    return rows, markers


def summarize(rows: list[dict], markers: list[dict], truth: dict, profile: str) -> dict:
    if not markers:
        raise ValueError(f"No task markers for {profile}")
    if len({item["task_index"] for item in markers}) != len(markers):
        raise ValueError(f"Duplicate task markers for {profile}")
    if any(item["status"] != "success" for item in markers):
        raise ValueError(f"Incomplete tasks for {profile}")
    if len({item["manifest_sha256"] for item in markers}) != 1:
        raise ValueError(f"Tasks used different manifests for {profile}")
    expected = sum(item["records_total"] for item in markers)
    if len(rows) != expected:
        raise ValueError(f"{profile}: expected {expected} rows, received {len(rows)}")
    if len({row["conversation_id"] for row in rows}) != len(rows):
        raise ValueError(f"Duplicate predictions for {profile}")
    if any(row["conversation_id"] not in truth for row in rows):
        raise ValueError(f"Unknown ground truth ID for {profile}")

    valid = [row for row in rows if row["status"] == "ok"]
    correct = sum(
        row["outcome"] == truth[row["conversation_id"]]["outcome"] for row in valid
    )
    true_positive = false_positive = false_negative = 0
    for row in valid:
        predicted = {cause["code"] for cause in row["causes"]}
        actual = truth[row["conversation_id"]]["causes"]
        true_positive += len(predicted & actual)
        false_positive += len(predicted - actual)
        false_negative += len(actual - predicted)

    precision = (
        true_positive / (true_positive + false_positive)
        if true_positive + false_positive
        else 0.0
    )
    recall = (
        true_positive / (true_positive + false_negative)
        if true_positive + false_negative
        else 0.0
    )
    billable_seconds_estimate = sum(
        max(60, marker["duration_ms"] / 1000) for marker in markers
    )
    estimated_cost = billable_seconds_estimate * RATES[profile]
    status_counts = defaultdict(int)
    for row in rows:
        status_counts[row["status"]] += 1
    return {
        "profile": profile,
        "run_id": markers[0]["run_id"],
        "manifest_sha256": markers[0]["manifest_sha256"],
        "model_revision": markers[0]["model_revision"],
        "quantization": markers[0]["quantization"],
        "vllm_version": markers[0]["vllm_version"],
        "records": len(rows),
        "status_counts": dict(status_counts),
        "schema_valid_rate": len(valid) / len(rows),
        "outcome_accuracy_on_valid": correct / len(valid) if valid else 0.0,
        "correct_outcomes": correct,
        "cause_precision": precision,
        "cause_recall": recall,
        "cause_f1": 2 * precision * recall / (precision + recall)
        if precision + recall
        else 0.0,
        "model_load_ms_max": max(marker["model_load_ms"] for marker in markers),
        "job_wall_ms_estimate": max(marker["duration_ms"] for marker in markers),
        "billable_instance_seconds_estimate": billable_seconds_estimate,
        "cost_usd_estimate": round(estimated_cost, 4),
        "cost_usd_per_1000_correct_outcomes_estimate": round(
            estimated_cost * 1000 / correct, 4
        )
        if correct
        else None,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--results-prefix", required=True, help="gs://.../runs/RUN_ID/worker-a"
    )
    parser.add_argument("--ground-truth", required=True, help="gs://.../labels.parquet")
    parser.add_argument("--output", required=True, help="local JSON report path")
    args = parser.parse_args()
    client = storage.Client()
    truth = read_truth(client, args.ground_truth)
    report = {}
    for profile in ("l4", "rtx6000"):
        rows, markers = read_profile(client, args.results_prefix, profile)
        report[profile] = summarize(rows, markers, truth, profile)
    if report["l4"]["manifest_sha256"] != report["rtx6000"]["manifest_sha256"]:
        raise ValueError("Profiles used different manifests")
    from pathlib import Path

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8"
    )
    print(f"Wrote comparison to {output}")


if __name__ == "__main__":
    main()
