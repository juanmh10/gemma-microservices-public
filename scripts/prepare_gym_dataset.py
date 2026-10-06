"""Split the pinned Gym Salesman source into Worker A input and ground truth."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent / "data"
SOURCE = ROOT / "source"
PREPARED = ROOT / "prepared" / "gym-sales-v1.jsonl"
GROUND_TRUTH = ROOT / "ground-truth" / "gym-sales-v1.jsonl"


def rows(path: Path):
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if line.strip():
                try:
                    yield json.loads(line)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"Invalid JSON at {path}:{line_number}") from exc


def write_row(stream, value: dict) -> None:
    stream.write(json.dumps(value, ensure_ascii=False, separators=(",", ":")) + "\n")


def main() -> None:
    metadata = {}
    for row in rows(SOURCE / "gym_v3_metadata.jsonl"):
        sample_id = row.pop("sample_id")
        if sample_id in metadata:
            raise ValueError(f"Duplicate metadata ID: {sample_id}")
        metadata[sample_id] = row

    PREPARED.parent.mkdir(parents=True, exist_ok=True)
    GROUND_TRUTH.parent.mkdir(parents=True, exist_ok=True)
    seen = set()
    with (
        PREPARED.open("w", encoding="utf-8") as prepared,
        GROUND_TRUTH.open("w", encoding="utf-8") as ground_truth,
    ):
        for row in rows(SOURCE / "gym_v3_dataset_clean.jsonl"):
            sample_id = row["sample_id"]
            if sample_id in seen or sample_id not in metadata:
                raise ValueError(f"Duplicate or unmatched conversation ID: {sample_id}")
            seen.add(sample_id)
            record_id = f"conv_{sample_id:06d}"
            messages = [
                {
                    "turn_id": index,
                    "role": "seller" if turn["role"] == "sales" else turn["role"],
                    "text": turn["text"],
                }
                for index, turn in enumerate(row["dialogue"], 1)
            ]
            if not messages or any(
                turn["role"] not in {"seller", "customer"} for turn in messages
            ):
                raise ValueError(f"Invalid dialogue roles: {sample_id}")
            write_row(prepared, {"record_id": record_id, "messages": messages})
            write_row(
                ground_truth,
                {
                    "record_id": record_id,
                    "outcome": row["outcome"],
                    "decision_turn_index": row["decision_turn_index"],
                    **metadata[sample_id],
                },
            )

    orphan_metadata = metadata.keys() - seen
    print(
        f"Prepared {len(seen)} conversations and matching ground truth records; "
        f"ignored {len(orphan_metadata)} metadata records without conversations."
    )


if __name__ == "__main__":
    main()
