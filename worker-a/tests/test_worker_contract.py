import hashlib
import json
from pathlib import Path
import pytest
from pydantic import ValidationError

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from worker import Cause, Prediction, clean_json_text

ALLOWED_CODES = {
    "boredom",
    "cancellation_maze",
    "changing_room",
    "commute_friction",
    "contract_lock",
    "contract_trap",
    "equipment_confusion",
    "fitness_level",
    "hidden_fees",
    "judgment_fear",
    "motivation_dropoff",
    "no_plan",
    "no_time",
    "peak_congestion",
    "price_gap",
    "wasted_money",
}


def test_clean_json_text_markdown_block():
    raw = '```json\n{"conversation_id": "conv_000001", "outcome": "success", "causes": []}\n```'
    cleaned = clean_json_text(raw)
    assert json.loads(cleaned)["conversation_id"] == "conv_000001"


def test_clean_json_text_with_commentary():
    raw = 'Here is the result:\n{"conversation_id": "conv_000002", "outcome": "failure", "causes": []}\nHope this helps!'
    cleaned = clean_json_text(raw)
    parsed = json.loads(cleaned)
    assert parsed["conversation_id"] == "conv_000002"
    assert parsed["outcome"] == "failure"


def test_clean_json_text_plain():
    raw = '{"conversation_id": "conv_000003", "outcome": "success", "causes": []}'
    assert clean_json_text(raw) == raw


def test_prediction_outcome_normalization():
    p1 = Prediction.model_validate_json(
        json.dumps({"conversation_id": "conv_000001", "outcome": "WON", "causes": []})
    )
    assert p1.outcome == "success"

    p2 = Prediction.model_validate_json(
        json.dumps({"conversation_id": "conv_000002", "outcome": "Lost", "causes": []})
    )
    assert p2.outcome == "failure"

    p3 = Prediction.model_validate_json(
        json.dumps(
            {"conversation_id": "conv_000003", "outcome": "  SUCCESS  ", "causes": []}
        )
    )
    assert p3.outcome == "success"


def test_prediction_invalid_outcome():
    with pytest.raises(ValidationError):
        Prediction.model_validate_json(
            json.dumps(
                {"conversation_id": "conv_000001", "outcome": "maybe", "causes": []}
            )
        )


def test_prediction_with_valid_causes():
    data = {
        "conversation_id": "conv_000001",
        "outcome": "failure",
        "causes": [
            {
                "code": "boredom",
                "confidence": 0.85,
                "evidence_turns": [2, 4],
            },
            {
                "code": "contract_trap",
                "confidence": 0.95,
                "evidence_turns": [5],
            },
        ],
    }
    p = Prediction.model_validate_json(json.dumps(data))
    assert len(p.causes) == 2
    assert p.causes[0].code == "boredom"
    assert p.causes[0].confidence == 0.85
    assert p.causes[0].evidence_turns == [2, 4]


def test_cause_confidence_bounds():
    with pytest.raises(ValidationError):
        Cause(code="boredom", confidence=1.5, evidence_turns=[1])
    with pytest.raises(ValidationError):
        Cause(code="boredom", confidence=-0.1, evidence_turns=[1])


def test_cause_evidence_turns_min_length():
    with pytest.raises(ValidationError):
        Cause(code="boredom", confidence=0.8, evidence_turns=[])


def test_taxonomy_codes_verification():
    valid_code = "cancellation_maze"
    invalid_code = "non_existent_objection"
    assert valid_code in ALLOWED_CODES
    assert invalid_code not in ALLOWED_CODES


def test_stage_rtx_model_downloads_and_verifies_weights(tmp_path, monkeypatch):
    from worker import stage_rtx_model

    mounted = tmp_path / "mounted"
    mounted.mkdir()
    (tmp_path / "staged").mkdir()
    (mounted / "config.json").write_text('{"model_type":"gemma"}')
    weight_bytes = b"synthetic safetensors bytes"

    class FakeBlob:
        size = len(weight_bytes)
        generation = 7

        def reload(self):
            pass

    class FakeBucket:
        def blob(self, name):
            assert name == "models/rtx6000/model.safetensors"
            return FakeBlob()

    class FakeClient:
        def bucket(self, name):
            assert name == "models-bucket"
            return FakeBucket()

    def fake_download(blob, filename, **kwargs):
        assert blob.generation == 7
        assert kwargs["crc32c_checksum"] is True
        assert kwargs["download_kwargs"] == {"if_generation_match": 7}
        Path(filename).write_bytes(weight_bytes)

    monkeypatch.setattr(
        "worker.transfer_manager.download_chunks_concurrently", fake_download
    )
    manifest = {
        "files": [
            {"path": "config.json", "sha256": "unused"},
            {
                "path": "model.safetensors",
                "sha256": hashlib.sha256(weight_bytes).hexdigest(),
            },
        ]
    }
    staged, stats = stage_rtx_model(
        FakeClient(),
        "gs://models-bucket/models/rtx6000/model-manifest.json",
        manifest,
        mounted,
        tmp_path / "staged",
    )
    assert (staged / "config.json").read_bytes() == (
        mounted / "config.json"
    ).read_bytes()
    assert (staged / "model.safetensors").read_bytes() == weight_bytes
    assert stats["weight_files"] == 1
    assert stats["weights_download_ms"] >= 0


def test_stage_rtx_model_rejects_corrupted_download(tmp_path, monkeypatch):
    from worker import stage_rtx_model

    class FakeBlob:
        size = 3
        generation = 1

        def reload(self):
            pass

    class FakeClient:
        def bucket(self, _name):
            return self

        def blob(self, _name):
            return FakeBlob()

    def fake_download(_blob, filename, **_kwargs):
        Path(filename).write_bytes(b"bad")

    monkeypatch.setattr(
        "worker.transfer_manager.download_chunks_concurrently", fake_download
    )
    (tmp_path / "staged").mkdir()
    with pytest.raises(ValueError, match="checksum mismatch"):
        stage_rtx_model(
            FakeClient(),
            "gs://models-bucket/models/rtx6000/model-manifest.json",
            {"files": [{"path": "model.safetensors", "sha256": "0" * 64}]},
            tmp_path,
            tmp_path / "staged",
        )


@pytest.mark.parametrize("layout", ["v2", "v1", "flat-v1"])
def test_cgroup_memory_reads_container_counters(tmp_path, layout):
    from worker import cgroup_memory_bytes

    root = tmp_path / "memory" if layout == "v1" else tmp_path
    root.mkdir(exist_ok=True)
    current = "memory.current" if layout == "v2" else "memory.usage_in_bytes"
    peak = "memory.peak" if layout == "v2" else "memory.max_usage_in_bytes"
    (root / current).write_text("1234\n")
    (root / peak).write_text("5678\n")
    assert cgroup_memory_bytes("memory.current", tmp_path) == 1234
    assert cgroup_memory_bytes("memory.peak", tmp_path) == 5678


def test_cgroup_memory_unavailable_is_not_zero(tmp_path):
    from worker import cgroup_memory_bytes

    assert cgroup_memory_bytes("memory.peak", tmp_path) is None


@pytest.mark.parametrize("soft,hard", [("0", "900"), ("900", "900"), ("1080", "900")])
def test_soft_deadline_rejects_unbounded_configuration(monkeypatch, soft, hard):
    from worker import configured_soft_deadline

    monkeypatch.setenv("SOFT_DEADLINE_SECONDS", soft)
    monkeypatch.setenv("TASK_TIMEOUT_SECONDS", hard)
    with pytest.raises(ValueError, match="below the task timeout"):
        configured_soft_deadline()


def test_canary_soft_deadline_leaves_flush_margin(monkeypatch):
    from worker import configured_soft_deadline

    monkeypatch.setenv("SOFT_DEADLINE_SECONDS", "600")
    monkeypatch.setenv("TASK_TIMEOUT_SECONDS", "900")
    assert configured_soft_deadline() == 600


@pytest.mark.parametrize(
    "turns,accepted", [([2, 2], True), ([1], False), ([0], False), ([99], False)]
)
def test_prediction_requires_actual_customer_evidence(turns, accepted):
    from worker import validate_prediction

    row = {
        "record_id": "conv_test",
        "messages": [
            {"turn_id": 1, "role": "seller"},
            {"turn_id": 2, "role": "customer"},
        ],
    }
    prediction = Prediction(
        conversation_id="conv_test",
        outcome="failure",
        causes=[Cause(code="boredom", confidence=0.8, evidence_turns=turns)],
    )
    if accepted:
        validate_prediction(prediction, row, {"boredom"})
        assert prediction.causes[0].evidence_turns == [2]
    else:
        with pytest.raises(ValueError, match="customer turn"):
            validate_prediction(prediction, row, {"boredom"})


def test_prediction_rejects_duplicate_cause():
    from worker import validate_prediction

    prediction = Prediction(
        conversation_id="conv_test",
        outcome="failure",
        causes=[Cause(code="boredom", confidence=0.8, evidence_turns=[2])] * 2,
    )
    with pytest.raises(ValueError, match="duplicate cause"):
        validate_prediction(
            prediction,
            {
                "record_id": "conv_test",
                "messages": [{"turn_id": 2, "role": "customer"}],
            },
            {"boredom"},
        )


@pytest.mark.parametrize("layout", ["v2", "v1", "flat-v1"])
def test_initialization_cpu_includes_container_children(tmp_path, layout):
    from worker import cgroup_cpu_usage_usec

    if layout == "v2":
        (tmp_path / "cpu.stat").write_text("usage_usec 2500000\nuser_usec 2000000\n")
    else:
        root = tmp_path / "cpuacct" if layout == "v1" else tmp_path
        root.mkdir(exist_ok=True)
        (root / "cpuacct.usage").write_text("2500000000\n")
    assert cgroup_cpu_usage_usec(tmp_path) == 2500000


def test_initialization_gpu_missing_remains_unavailable(monkeypatch):
    from worker import gpu_snapshot

    def missing(*args, **kwargs):
        raise FileNotFoundError()

    monkeypatch.setattr("worker.subprocess.run", missing)
    assert all(value is None for value in gpu_snapshot().values())


def test_initialization_failure_is_logged_and_propagated(monkeypatch):
    from worker import initialization_phase

    events = []
    monkeypatch.setattr("worker.log_event", lambda **fields: events.append(fields))
    monkeypatch.setattr("worker.gpu_snapshot", lambda: {})
    timings = {}
    with pytest.raises(RuntimeError, match="engine failed"):
        with initialization_phase("engine_construction", timings):
            raise RuntimeError("engine failed")
    assert events[-1]["status"] == "failed"
    assert timings["engine_construction_ms"] >= 0
    assert any(event["event"] == "initialization_resources" for event in events)


@pytest.mark.parametrize(
    "payload,category",
    [
        ('{"private":', "malformed_json"),
        ('{"conversation_id":"private"}', "schema_validation"),
        (
            '{"conversation_id":"private","outcome":"private","causes":[]}',
            "schema_validation",
        ),
    ],
)
def test_rejection_category_for_pydantic_errors(payload, category):
    from worker import rejection_category
    from pydantic import ValidationError

    with pytest.raises(ValidationError) as caught:
        Prediction.model_validate_json(payload)
    assert rejection_category(caught.value) == category
    assert "private" not in rejection_category(caught.value)


@pytest.mark.parametrize(
    "conversation_id,codes,turns,category",
    [
        ("other", ["boredom"], [2], "conversation_id_mismatch"),
        ("conv_test", ["unknown"], [2], "unknown_cause_code"),
        ("conv_test", ["boredom", "boredom"], [2], "duplicate_cause_code"),
        ("conv_test", ["boredom"], [1], "invalid_evidence_turn"),
    ],
)
def test_contract_rejection_categories(conversation_id, codes, turns, category):
    from worker import PredictionRejected, rejection_category, validate_prediction

    prediction = Prediction(
        conversation_id=conversation_id,
        outcome="failure",
        causes=[
            Cause(code=code, confidence=0.8, evidence_turns=turns) for code in codes
        ],
    )
    with pytest.raises(PredictionRejected) as caught:
        validate_prediction(
            prediction,
            {
                "record_id": "conv_test",
                "messages": [{"turn_id": 2, "role": "customer"}],
            },
            {"boredom"},
        )
    assert rejection_category(caught.value) == category


@pytest.mark.parametrize("reason", ["stop", "length", "abort", None, "private"])
def test_finish_reason_is_bounded(reason):
    from types import SimpleNamespace
    from worker import generation_finish_reason

    expected = reason if reason in ("stop", "length", "abort") else "unknown"
    assert generation_finish_reason(SimpleNamespace(finish_reason=reason)) == expected
    assert generation_finish_reason(SimpleNamespace()) == "unknown"


def test_rejection_log_excludes_validator_input(capsys):
    import json
    from pydantic import ValidationError
    from worker import log_event, rejection_category

    payload = '{"conversation_id":"private-content","outcome":"private-content"}'
    with pytest.raises(ValidationError) as caught:
        Prediction.model_validate_json(payload)
    log_event(
        event="prediction_rejected",
        record_id="conv_test",
        error_type=type(caught.value).__name__,
        rejection_category=rejection_category(caught.value),
        finish_reason="stop",
    )
    captured = capsys.readouterr().out
    assert "private-content" not in captured
    assert json.loads(captured)["rejection_category"] == "schema_validation"
