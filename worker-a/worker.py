"""Bounded, stateless Worker A task for one prepared shard."""

from __future__ import annotations

import hashlib
import importlib.metadata
import io
import json
import os
import shutil
import signal
import sys
import subprocess
import threading
from contextlib import contextmanager
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

import compile_cache
import domain

import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
from google.cloud.storage import transfer_manager
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


MAX_MODEL_LEN = 4096
MAX_OUTPUT_TOKENS = 512
BATCH_SIZE = 1
FLUSH_RECORDS = 250
FLUSH_SECONDS = 60
SOFT_DEADLINE_SECONDS = 18 * 60
PROMPT_VERSION = "worker-a-v3"
MODEL_STAGE_DIR = Path("/mnt/model-cache")
MODEL_STAGE_LIMIT_BYTES = 24 * 1024**3
STOP = False


def on_term(_signum, _frame):
    global STOP
    STOP = True


signal.signal(signal.SIGTERM, on_term)


class Cause(BaseModel):
    model_config = ConfigDict(extra="ignore")
    code: str
    confidence: float = Field(ge=0, le=1)
    evidence_turns: list[int] = Field(min_length=1)


class Prediction(BaseModel):
    model_config = ConfigDict(extra="ignore")
    conversation_id: str
    outcome: str
    causes: list[Cause]

    @model_validator(mode="before")
    @classmethod
    def normalize_inputs(cls, data):
        if isinstance(data, dict):
            if "outcome" in data and isinstance(data["outcome"], str):
                outcome = data["outcome"].strip().lower()
                data["outcome"] = {"won": "success", "lost": "failure"}.get(
                    outcome, outcome
                )
        return data

    @model_validator(mode="after")
    def valid_outcome(self):
        if self.outcome not in {"success", "failure"}:
            raise ValueError("invalid outcome")
        return self


def clean_json_text(raw: str) -> str:
    cleaned = raw.strip()
    if "```json" in cleaned:
        cleaned = cleaned.split("```json", 1)[1].split("```", 1)[0].strip()
    elif "```" in cleaned:
        cleaned = cleaned.split("```", 1)[1].split("```", 1)[0].strip()
    else:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end != -1 and end > start:
            cleaned = cleaned[start : end + 1]
    return cleaned


def blob_for(client: storage.Client, uri: str):
    parsed = urlparse(uri)
    if parsed.scheme != "gs" or not parsed.netloc or not parsed.path.lstrip("/"):
        raise ValueError(f"Expected a GCS object URI: {uri}")
    return client.bucket(parsed.netloc).blob(parsed.path.lstrip("/"))


def read_bytes(client: storage.Client, uri: str) -> bytes:
    return blob_for(client, uri).download_as_bytes()


def write_bytes(client: storage.Client, uri: str, data: bytes) -> None:
    blob_for(client, uri).upload_from_string(data)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def verify_mounted_model(
    client: storage.Client, manifest_uri: str, root: Path
) -> tuple[dict, Path]:
    model_manifest = json.loads(read_bytes(client, manifest_uri))
    parsed = urlparse(manifest_uri)
    model_dir = root / Path(parsed.path.lstrip("/")).parent

    def verify(entry: dict) -> None:
        relative = Path(entry["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Unsafe model file path")
        target = model_dir / relative
        if not target.exists():
            raise FileNotFoundError(f"Model file missing: {relative}")
        stat = target.stat()
        if stat.st_size == 0:
            raise ValueError(f"Model file is empty: {relative}")
        # Hash small files locally. Large weight files are checked for presence
        # and size only; GCS transfer checksums do not prove manifest SHA256.
        if stat.st_size < 50 * 1024 * 1024:
            with target.open("rb") as source:
                actual = hashlib.file_digest(source, "sha256").hexdigest()
            if actual != entry["sha256"]:
                raise ValueError(f"Model checksum mismatch: {relative}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(verify, model_manifest["files"]))
    return model_manifest, model_dir


def stage_rtx_model(
    client: storage.Client,
    manifest_uri: str,
    model_manifest: dict,
    mounted_dir: Path,
    staging_dir: Path,
) -> tuple[Path, dict]:
    parsed = urlparse(manifest_uri)
    if parsed.scheme != "gs" or not parsed.netloc:
        raise ValueError("Expected a GCS model manifest URI")
    model_prefix = Path(parsed.path.lstrip("/")).parent
    if not staging_dir.is_dir():
        raise FileNotFoundError("RTX model cache volume is not mounted")
    download_ms = 0
    checksum_ms = 0
    weight_files = 0
    staged_bytes = 0

    for entry in model_manifest["files"]:
        relative = Path(entry["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Unsafe model file path")
        target = staging_dir / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if relative.suffix != ".safetensors":
            shutil.copyfile(mounted_dir / relative, target)
            staged_bytes += target.stat().st_size
            continue

        blob = client.bucket(parsed.netloc).blob((model_prefix / relative).as_posix())
        blob.reload()
        if not blob.size or not blob.generation:
            raise ValueError("Model weight object is missing size or generation")
        if staged_bytes + blob.size > MODEL_STAGE_LIMIT_BYTES:
            raise ValueError("Model exceeds RTX cache volume limit")
        log_event(event="model_weights_download_started", bytes=blob.size)
        started = time.monotonic()
        transfer_manager.download_chunks_concurrently(
            blob,
            str(target),
            chunk_size=32 * 1024 * 1024,
            max_workers=8,
            worker_type=transfer_manager.PROCESS,
            download_kwargs={"if_generation_match": blob.generation},
            crc32c_checksum=True,
        )
        download_ms += int((time.monotonic() - started) * 1000)
        if target.stat().st_size != blob.size:
            raise ValueError("Downloaded model weight size mismatch")
        started = time.monotonic()
        with target.open("rb") as source:
            actual_sha256 = hashlib.file_digest(source, "sha256").hexdigest()
        checksum_ms += int((time.monotonic() - started) * 1000)
        if actual_sha256 != entry["sha256"]:
            raise ValueError("Downloaded model weight checksum mismatch")
        staged_bytes += blob.size
        weight_files += 1

    if weight_files == 0:
        raise ValueError("Model manifest has no safetensors weights")
    return staging_dir, {
        "weights_download_ms": download_ms,
        "weights_sha256_ms": checksum_ms,
        "weight_files": weight_files,
        "staged_bytes": staged_bytes,
    }


def rows_from_shard(data: bytes):
    import zstandard

    with zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)) as stream:
        with io.TextIOWrapper(stream, encoding="utf-8") as text:
            for line in text:
                yield json.loads(line)


RESULT_SCHEMA = pa.schema(
    [
        ("conversation_id", pa.string()),
        ("status", pa.string()),
        ("outcome", pa.string()),
        (
            "causes",
            pa.list_(
                pa.struct(
                    [
                        ("code", pa.string()),
                        ("confidence", pa.float32()),
                        ("evidence_turns", pa.list_(pa.int32())),
                    ]
                )
            ),
        ),
        ("input_tokens", pa.int32()),
        ("output_tokens", pa.int32()),
        ("duration_ms", pa.int64()),
    ]
)


def flush(
    client: storage.Client, prefix: str, chunk_index: int, buffered: list[dict]
) -> int:
    if not buffered:
        return chunk_index
    table = pa.Table.from_pylist(buffered, schema=RESULT_SCHEMA)
    output = io.BytesIO()
    pq.write_table(table, output, compression="zstd")
    write_bytes(client, f"{prefix}/chunk-{chunk_index:06d}.parquet", output.getvalue())
    buffered.clear()
    return chunk_index + 1


def log_event(**fields):
    print(json.dumps(fields, separators=(",", ":")), flush=True)


def cgroup_memory_bytes(name: str, root: Path = Path("/sys/fs/cgroup")) -> int | None:
    legacy = {
        "memory.current": "memory.usage_in_bytes",
        "memory.peak": "memory.max_usage_in_bytes",
    }
    for path in (root / name, root / "memory" / legacy[name], root / legacy[name]):
        try:
            return int(path.read_text().strip())
        except (FileNotFoundError, PermissionError, ValueError):
            continue
    return None


def cgroup_cpu_usage_usec(root: Path = Path("/sys/fs/cgroup")) -> int | None:
    """Read total container CPU time, including engine child processes."""
    try:
        values = dict(
            line.split() for line in (root / "cpu.stat").read_text().splitlines()
        )
        return int(values["usage_usec"])
    except (OSError, ValueError, KeyError):
        pass
    for path in (root / "cpuacct" / "cpuacct.usage", root / "cpuacct.usage"):
        try:
            return int(path.read_text().strip()) // 1000
        except (OSError, ValueError):
            continue
    return None


def gpu_snapshot() -> dict:
    """Query numeric device counters without importing CUDA in the parent process."""
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=utilization.gpu,memory.used,memory.total",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=2,
            check=True,
        )
        utilization, used, total = [
            int(value.strip()) for value in result.stdout.strip().split(",")
        ]
        return {
            "gpu_utilization_percent": utilization,
            "gpu_memory_used_bytes": used * 1024**2,
            "gpu_memory_total_bytes": total * 1024**2,
        }
    except (OSError, subprocess.SubprocessError, ValueError):
        return {
            "gpu_utilization_percent": None,
            "gpu_memory_used_bytes": None,
            "gpu_memory_total_bytes": None,
        }


@contextmanager
def initialization_phase(phase: str, timings: dict):
    """Emit bounded resource samples and preserve failures from initialization."""
    started = time.monotonic()
    stopped = threading.Event()
    previous_time = started
    previous_cpu = cgroup_cpu_usage_usec()

    def sample():
        nonlocal previous_time, previous_cpu
        gpu = gpu_snapshot()
        now = time.monotonic()
        cpu = cgroup_cpu_usage_usec()
        cores = None
        if cpu is not None and previous_cpu is not None and now > previous_time:
            cores = max(0, cpu - previous_cpu) / ((now - previous_time) * 1_000_000)
        previous_time, previous_cpu = now, cpu
        log_event(
            event="initialization_resources",
            phase=phase,
            elapsed_ms=int((now - started) * 1000),
            cpu_cores_used=cores,
            memory_current_bytes=cgroup_memory_bytes("memory.current"),
            memory_peak_bytes=cgroup_memory_bytes("memory.peak"),
            **gpu,
        )

    def monitor():
        while not stopped.wait(5):
            sample()

    log_event(event="initialization_phase_started", phase=phase)
    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    status = "failed"
    try:
        yield
        status = "completed"
    finally:
        stopped.set()
        thread.join()
        elapsed_ms = int((time.monotonic() - started) * 1000)
        timings[phase + "_ms"] = elapsed_ms
        sample()
        log_event(
            event="initialization_phase_finished",
            phase=phase,
            elapsed_ms=elapsed_ms,
            status=status,
        )


def configured_soft_deadline() -> int:
    soft = int(os.environ.get("SOFT_DEADLINE_SECONDS", SOFT_DEADLINE_SECONDS))
    hard = int(os.environ.get("TASK_TIMEOUT_SECONDS", 1200))
    if not 0 < soft < hard:
        raise ValueError("Soft deadline must be positive and below the task timeout")
    return soft


class PredictionRejected(ValueError):
    def __init__(self, category: str, message: str):
        super().__init__(message)
        self.category = category


def rejection_category(error: Exception) -> str:
    if isinstance(error, PredictionRejected):
        return error.category
    if isinstance(error, json.JSONDecodeError):
        return "malformed_json"
    if isinstance(error, ValidationError):
        errors = error.errors(include_input=False, include_context=False)
        if any(item["type"] == "json_invalid" for item in errors):
            return "malformed_json"
        return "schema_validation"
    return "validation_error"


def generation_finish_reason(output) -> str:
    reason = getattr(output, "finish_reason", None)
    return reason if reason in ("stop", "length", "abort") else "unknown"


def validate_prediction(
    prediction: Prediction,
    row: dict,
    allowed_codes: set[str],
    evidence_role: str = "customer",
) -> None:
    if prediction.conversation_id != row["record_id"]:
        raise PredictionRejected("conversation_id_mismatch", "Conversation ID mismatch")
    customer_ids = {
        turn["turn_id"] for turn in row["messages"] if turn["role"] == evidence_role
    }
    seen = set()
    for cause in prediction.causes:
        if cause.code not in allowed_codes:
            raise PredictionRejected(
                "unknown_cause_code", "Unknown or duplicate cause code"
            )
        if cause.code in seen:
            raise PredictionRejected(
                "duplicate_cause_code", "Unknown or duplicate cause code"
            )
        if not set(cause.evidence_turns) <= customer_ids:
            raise PredictionRejected(
                "invalid_evidence_turn",
                f"Evidence must reference supplied {evidence_role} turn IDs",
            )
        seen.add(cause.code)
        cause.evidence_turns = sorted(set(cause.evidence_turns))


def main() -> int:
    soft_deadline_seconds = configured_soft_deadline()
    manifest_uri = os.environ["MANIFEST_URI"]
    result_prefix = os.environ["RESULTS_PREFIX"].rstrip("/")
    model_manifest_uri = os.environ["MODEL_MANIFEST_URI"]
    profile = os.environ["GPU_PROFILE"]
    task_index = int(os.environ["CLOUD_RUN_TASK_INDEX"])
    client = storage.Client()
    start = time.monotonic()
    manifest_bytes = read_bytes(client, manifest_uri)
    manifest = json.loads(manifest_bytes)
    if manifest.get("schema_version") != "conversation-analysis-v1":
        raise ValueError("Manifest schema does not match the worker image")
    if manifest["prompt_version"] != PROMPT_VERSION:
        raise ValueError("Manifest prompt version does not match the worker image")
    domain_uri = manifest.get("domain_uri", "")
    if domain_uri != manifest_uri.rsplit("/", 1)[0] + "/domain.json":
        raise ValueError("Domain must be stored alongside the prepared manifest")
    domain_blob = blob_for(client, domain_uri)
    domain_blob.reload()
    if not domain_blob.size or domain_blob.size > domain.MAX_DOMAIN_BYTES:
        raise ValueError("Invalid domain configuration size")
    task_domain = domain.load_domain(
        domain_blob.download_as_bytes(if_generation_match=domain_blob.generation),
        manifest,
    )
    prompt_template = Path("/app/prompt.txt").read_text(encoding="utf-8")
    prompt_sha256 = digest(prompt_template.encode("utf-8"))
    allowed_codes = set(task_domain["causes"])
    if task_index < 0 or task_index >= len(manifest["shards"]):
        raise ValueError("Task index does not match manifest")
    shard = manifest["shards"][task_index]
    if shard["index"] != task_index:
        raise ValueError("Shard index mismatch")
    shard_data = read_bytes(client, shard["uri"])
    if digest(shard_data) != shard["sha256"]:
        raise ValueError("Shard checksum mismatch")
    prefix = f"{result_prefix}/runs/{manifest['run_id']}/worker-a/{profile}/task-{task_index:05d}"
    verify_start = time.monotonic()
    model_info, model_dir = verify_mounted_model(
        client, model_manifest_uri, Path("/mnt/models")
    )
    verify_ms = int((time.monotonic() - verify_start) * 1000)
    load_start = time.monotonic()
    stage_stats = {}
    if profile == "rtx6000":
        model_dir, stage_stats = stage_rtx_model(
            client, model_manifest_uri, model_info, model_dir, MODEL_STAGE_DIR
        )
        log_event(
            event="model_weights_staged",
            **stage_stats,
            memory_current_bytes=cgroup_memory_bytes("memory.current"),
        )
    engine_args = {
        "model": str(model_dir),
        "enable_flashinfer_autotune": False,
        "max_model_len": MAX_MODEL_LEN,
        "max_num_seqs": BATCH_SIZE,
        "gpu_memory_utilization": 0.75,
        "generation_config": "vllm",
        "hf_overrides": {
            "diffusion_sampler": "entropy_bound",
            "diffusion_entropy_bound": 0.1,
        },
        "diffusion_config": {"canvas_length": 256},
        "trust_remote_code": True,
    }
    if profile == "rtx6000":
        engine_args["load_format"] = "safetensors"
    cache_status = {"mode": "disabled"}
    cache_compatibility = None
    export_cache = os.environ.get("EXPORT_COMPILE_CACHE", "0") == "1"
    require_cache = (
        os.environ.get("REQUIRE_COMPILE_CACHE", "0") == "1"
        or compile_cache.MANIFEST_PATH.exists()
    )
    if export_cache and require_cache:
        raise ValueError("Cache generation and required cache loading are exclusive")
    if export_cache or require_cache:
        cache_compatibility = compile_cache.compatibility(
            model_info, engine_args, profile
        )
        if export_cache:
            if task_index != 0 or os.environ.get("CLOUD_RUN_TASK_COUNT") != "1":
                raise ValueError("Cache generation requires a single-task canary")
            os.environ["VLLM_USE_AOT_COMPILE"] = "1"
            os.environ["VLLM_FORCE_AOT_LOAD"] = "0"
            cache_status = {"mode": "generation"}
        else:
            cache_status = compile_cache.validate(
                cache_compatibility, start + soft_deadline_seconds
            )
        log_event(event="compilation_cache_ready", **cache_status)
    engine_start = time.monotonic()
    initialization_timings = {}
    with initialization_phase("vllm_import", initialization_timings):
        from vllm import LLM, SamplingParams

    if importlib.metadata.version("vllm") != model_info["vllm_version"]:
        raise ValueError("vLLM version does not match model manifest")

    with initialization_phase("engine_construction", initialization_timings):
        engine = LLM(**engine_args)
    with initialization_phase("tokenizer_access", initialization_timings):
        tokenizer = engine.get_tokenizer()
    load_ms = int((time.monotonic() - load_start) * 1000)
    engine_init_ms = int((time.monotonic() - engine_start) * 1000)
    load_breakdown = {
        "verify_mounted_ms": verify_ms,
        **stage_stats,
        "engine_init_ms": engine_init_ms,
        "initialization_phases": initialization_timings,
        "total_load_ms": load_ms,
    }
    log_event(event="model_load_breakdown", **load_breakdown)
    settings = SamplingParams(temperature=0, max_tokens=MAX_OUTPUT_TOKENS)
    buffered = []
    counts = {"ok": 0, "invalid_output": 0, "oversized_input": 0}
    chunk_index = 0
    last_flush = time.monotonic()
    batch = []

    def process_batch() -> None:
        nonlocal chunk_index, last_flush
        if not batch:
            return
        prompts = [item[1] for item in batch]
        started = time.monotonic()
        outputs = engine.generate(prompts, settings)
        elapsed_ms = int((time.monotonic() - started) * 1000)
        for (row, _prompt, input_tokens), output in zip(batch, outputs, strict=True):
            finish_reason = generation_finish_reason(output.outputs[0])
            log_event(
                event="prediction_generation_finished",
                record_id=row["record_id"],
                finish_reason=finish_reason,
            )
            result = {
                "conversation_id": row["record_id"],
                "status": "invalid_output",
                "outcome": None,
                "causes": [],
                "input_tokens": input_tokens,
                "output_tokens": len(output.outputs[0].token_ids),
                "duration_ms": elapsed_ms,
            }
            try:
                raw_output = output.outputs[0].text
                cleaned_text = clean_json_text(raw_output)
                prediction = Prediction.model_validate_json(cleaned_text)
                validate_prediction(
                    prediction, row, allowed_codes, task_domain["evidence_role"]
                )
                result.update(
                    status="ok",
                    outcome=prediction.outcome,
                    causes=[cause.model_dump() for cause in prediction.causes],
                )
            except (ValidationError, ValueError, json.JSONDecodeError) as err:
                log_event(
                    event="prediction_rejected",
                    record_id=row["record_id"],
                    error_type=type(err).__name__,
                    rejection_category=rejection_category(err),
                    finish_reason=finish_reason,
                )
            buffered.append(result)
            counts[result["status"]] += 1
        batch.clear()
        if (
            len(buffered) >= FLUSH_RECORDS
            or time.monotonic() - last_flush >= FLUSH_SECONDS
        ):
            chunk_index = flush(client, prefix, chunk_index, buffered)
            last_flush = time.monotonic()
        log_event(
            event="batch_completed",
            run_id=manifest["run_id"],
            task_index=task_index,
            gpu_profile=profile,
            records=len(outputs),
            elapsed_ms=elapsed_ms,
        )

    for row in rows_from_shard(shard_data):
        if STOP or time.monotonic() - start >= soft_deadline_seconds:
            break
        instruction = domain.render_instruction(prompt_template, task_domain, row)
        prompt = tokenizer.apply_chat_template(
            [{"role": "user", "content": instruction}],
            tokenize=False,
            add_generation_prompt=True,
        )
        input_tokens = len(tokenizer.encode(prompt))
        if input_tokens + MAX_OUTPUT_TOKENS > MAX_MODEL_LEN:
            buffered.append(
                {
                    "conversation_id": row["record_id"],
                    "status": "oversized_input",
                    "outcome": None,
                    "causes": [],
                    "input_tokens": input_tokens,
                    "output_tokens": 0,
                    "duration_ms": 0,
                }
            )
            counts["oversized_input"] += 1
        else:
            batch.append((row, prompt, input_tokens))
            if len(batch) == BATCH_SIZE:
                process_batch()
        if (
            len(buffered) >= FLUSH_RECORDS
            or time.monotonic() - last_flush >= FLUSH_SECONDS
        ):
            chunk_index = flush(client, prefix, chunk_index, buffered)
            last_flush = time.monotonic()
    if not STOP and time.monotonic() - start < soft_deadline_seconds:
        process_batch()
    chunk_index = flush(client, prefix, chunk_index, buffered)
    processed = sum(counts.values())
    metadata = {
        "run_id": manifest["run_id"],
        "task_index": task_index,
        "gpu_profile": profile,
        "git_sha": os.environ.get("GIT_SHA", ""),
        "image_digest": os.environ.get("IMAGE_DIGEST", ""),
        "manifest_sha256": digest(manifest_bytes),
        "model_id": model_info["model_id"],
        "model_revision": model_info["revision"],
        "quantization": model_info["quantization"],
        "vllm_version": model_info["vllm_version"],
        "model_load_format": "local_safetensors" if profile == "rtx6000" else "auto",
        "prompt_version": manifest["prompt_version"],
        "soft_deadline_seconds": soft_deadline_seconds,
        "prompt_sha256": prompt_sha256,
        "schema_version": manifest["schema_version"],
        "domain_version": task_domain["version"],
        "domain_sha256": manifest["domain_sha256"],
        "compilation_cache": cache_status,
        "model_load_ms": load_ms,
        "model_load_breakdown": load_breakdown,
        "memory_peak_bytes": cgroup_memory_bytes("memory.peak"),
        "duration_ms": int((time.monotonic() - start) * 1000),
        "records_total": shard["records"],
        "records_processed": processed,
        **counts,
    }
    success = (
        not STOP
        and processed == shard["records"]
        and time.monotonic() - start < soft_deadline_seconds
    )
    if success and export_cache:
        try:
            cache_status = compile_cache.export(
                client, prefix, cache_compatibility, start + soft_deadline_seconds
            )
            metadata["compilation_cache"] = cache_status
            log_event(event="compilation_cache_exported", **cache_status)
            success = not STOP and time.monotonic() - start < soft_deadline_seconds
        except Exception as exc:
            success = False
            metadata["compilation_cache"] = {
                "mode": "export_failed",
                "error_type": type(exc).__name__,
            }
            log_event(
                event="compilation_cache_export_failed", error_type=type(exc).__name__
            )
    metadata["duration_ms"] = int((time.monotonic() - start) * 1000)
    metadata["status"] = "success" if success else "aborted"
    write_bytes(
        client,
        prefix + ("/_SUCCESS.json" if success else "/_ABORTED.json"),
        json.dumps(metadata, sort_keys=True).encode(),
    )
    log_event(
        event="task_finished",
        run_id=manifest["run_id"],
        task_index=task_index,
        gpu_profile=profile,
        status=metadata["status"],
        records=processed,
    )
    return 0 if success else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        log_event(event="task_error", error_type=type(exc).__name__)
        raise
