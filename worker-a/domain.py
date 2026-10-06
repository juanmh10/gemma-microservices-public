"""Versioned task definitions shared by every input conversation in a run."""

from __future__ import annotations

import hashlib
import json
import re

MAX_DOMAIN_BYTES = 65536


def load_domain(raw: bytes, manifest: dict) -> dict:
    if len(raw) > MAX_DOMAIN_BYTES:
        raise ValueError("Domain configuration exceeds its size limit")
    if hashlib.sha256(raw).hexdigest() != manifest.get("domain_sha256"):
        raise ValueError("Domain checksum mismatch")
    config = json.loads(raw)
    required = {"version", "task", "outcomes", "causes", "evidence_role"}
    if not isinstance(config, dict) or set(config) != required:
        raise ValueError("Invalid domain configuration fields")
    if config["version"] != manifest.get("domain_version"):
        raise ValueError("Domain version mismatch")
    if not isinstance(config["version"], str) or not re.fullmatch(
        r"[a-z][a-z0-9-]{2,63}", config["version"]
    ):
        raise ValueError("Invalid domain version")
    if not isinstance(config["task"], str) or not 1 <= len(config["task"]) <= 4000:
        raise ValueError("Invalid domain task")
    if config["evidence_role"] not in ("customer", "seller"):
        raise ValueError("Invalid evidence role")
    outcomes, causes = config["outcomes"], config["causes"]
    if not isinstance(outcomes, dict) or set(outcomes) != {"success", "failure"}:
        raise ValueError("Domain must define success and failure")
    if not isinstance(causes, dict) or not 1 <= len(causes) <= 64:
        raise ValueError("Invalid domain causes")
    for code in causes:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", code):
            raise ValueError("Invalid domain cause code")
    if any(
        not isinstance(value, str) or not 1 <= len(value) <= 2000
        for value in [*outcomes.values(), *causes.values()]
    ):
        raise ValueError("Invalid domain definition")
    return config


def render_instruction(template: str, config: dict, row: dict) -> str:
    dialogue = "\n".join(
        f"{turn['turn_id']}. {turn['role']}: {turn['text']}" for turn in row["messages"]
    )
    return template.format(
        record_id=row["record_id"],
        domain_definition=json.dumps(config, ensure_ascii=False, sort_keys=True),
        dialogue=dialogue,
    )
