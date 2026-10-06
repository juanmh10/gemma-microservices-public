---
pretty_name: Gym Salesman Dataset
language:
  - en
license: mit
size_categories:
  - 10K<n<100K
task_categories:
  - text-classification
  - text-generation
tags:
  - synthetic
  - dialogue
  - sales
  - conversational
  - multi-agent
  - emergent-labels
configs:
  - config_name: conversations
    data_files: gym_v3_dataset_clean.jsonl
    default: true
  - config_name: latent_state
    data_files: gym_v3_metadata.jsonl
---

# Gym Salesman Dataset

Local editorial note: the incomplete upstream citation author field was omitted;
the source dataset files and pinned revision are unchanged.

**11,997 synthetic gym-membership sales conversations, each labelled `SUCCESS` or `FAILURE`.**

## 🔗 Project Links

|  **Live App** — practice against an AI customer | [Hugging Face Space](https://huggingface.co/spaces/elg4/gym-sales-coach) |
|  **Telegram Bot** — practice on the go | [@ido_salescoach_bot](https://t.me/ido_salescoach_bot) |
|  **Dataset** — 11,997 labelled conversations | [elg4/Gym_Salesman_Dataset](https://huggingface.co/datasets/elg4/Gym_Salesman_Dataset) |
|  **Data Generation** — how the data was built | [notebook](https://huggingface.co/datasets/elg4/Gym_Salesman_Dataset/blob/main/notebooks/generation.ipynb) |
|  **Recommendation** — the embedding retriever | [notebook](https://huggingface.co/datasets/elg4/Gym_Salesman_Dataset/blob/main/notebooks/recommendation.ipynb) |

---

Every conversation is a two-agent rollout: a salesperson with a hidden skill level talks to a
customer with hidden objections. Neither agent is ever told the outcome — because at the time
they speak, no outcome exists. A separate evaluator model then judges, objection by objection,
whether each concern was genuinely resolved, and the label falls out of arithmetic.

> **The label is an OUTPUT of the simulation, never an INPUT to it.**

That single constraint is what this dataset is really about. A generator told *"write a bad
pitch, the label is FAILURE"* writes a caricature, and a classifier then reads the answer off
the text without learning anything. An earlier version of this dataset did exactly that and
scored **90.4%** on the leakage probe below. This version scores 64.2% / 69.4% — at or under
the majority baseline.

---

## Quick facts

| | |
|---|---|
| Conversations | **11,997** (12,000 generated − 3 near-duplicates) |
| Label balance | 3,285 `SUCCESS` (27.4%) / 8,715 `FAILURE` (72.6%) |
| Turns per conversation | 6 or 8, always sales-first and customer-last |
| Customer personas | 4, each owning 4 distinct objections (16 total) |
| Objections per conversation | 2–3, drawn from that persona's pool |
| Generator | `Qwen2.5-3B-Instruct` |
| Evaluator & decision turn | `Qwen2.5-7B-Instruct` |
| Language | English |
| Reproducibility | counter-based RNG, `SEED = 42` |

---

## Dataset structure

The dataset ships as **two physically separate files**, and that separation is deliberate.

### 1. `gym_v3_dataset_clean.jsonl` — what a model may see

| field | type | description |
|---|---|---|
| `sample_id` | int | stable identifier, joins to the metadata file |
| `outcome` | string | `SUCCESS` or `FAILURE` — the target |
| `dialogue` | list | ordered turns, each exactly `{role, text}`; `role` ∈ `{sales, customer}` |
| `decision_turn_index` | int | index of the final customer turn that verbalises the decision |
| `persona` | string | e.g. `Ariel - Impulsive Commitment-Phobe` |
| `demographic` | string | who the customer is, drawn from that persona's compatible list |
| `mood` | string | how they feel walking in, drawn from that persona's compatible list |
| `gym` | string | one of 6 fictional gyms |
| `price` | string | monthly membership price, e.g. `₪219/month` |
| `n_turns` | int | 6 or 8 |

### 2. `gym_v3_metadata.jsonl` — latent state, **never a feature**

| field | type | description |
|---|---|---|
| `sample_id` | int | joins to the dataset file |
| `skill_level` | string | `novice` / `average` / `expert` |
| `hidden_objection_ids` | list | e.g. `["no_time", "wasted_money"]` |
| `hidden_objection_labels` | list | the objections in the customer's own words |
| `objections_resolved_flags` | list | per-objection verdict from the evaluator |
| `objections_resolved_count` | int | sum of the flags |
| `persuasion_threshold` | int | how many had to be resolved: `n-1` or `n` |
| `evaluator_reasoning` | string | one sentence per objection, joined by ` \| ` |

**Why two files.** The latent state *defines* the label:
`outcome = SUCCESS if objections_resolved_count >= persuasion_threshold`. If it sat in the same
row as the dialogue, someone would vectorise the whole record, hit 100% accuracy, and learn
nothing. Physical separation makes that mistake require intent rather than inattention.

> ⚠️ **Do not train on the metadata file.** It is for analysis, auditing, and error inspection.

---

## How it was built

| Stage | What happens | Model |
|---|---|---|
| **A** | Sample all latent state — persona, demographic, mood, objections, skill, threshold, gym, price, length — before any text exists | counter-based RNG |
| **B** | Generate the conversation one turn at a time, alternating agents | `Qwen2.5-3B-Instruct` |
| **C** | Judge each objection against its written `resolves_when` criterion | `Qwen2.5-7B-Instruct` |
| **D** | `label = SUCCESS if resolved >= threshold else FAILURE` | arithmetic — no model |
| **E** | Generate one final customer turn that verbalises the decision | `Qwen2.5-7B-Instruct` |

Three design decisions carry most of the weight:

**Skill describes capability, never style.** A `novice` is not written as pushy or generic — he
is a decent salesperson who gives quick general answers, handles only the first concern he
registers, and never circles back. An `expert` asks what is behind the worry, answers with the
exact figure, and returns to anything left open. All three receive **the same gym fact sheet**;
skill governs how well they deploy it, not what they know. Had skill controlled knowledge,
"novice" would mean "cannot answer" — a deterministic skill→outcome mapping, which is leakage
wearing a disguise.

**Roles are assigned by the loop, not generated.** Each turn dict is constructed in Python, so
alternation, turn count, and the `{role, text}` shape cannot be corrupted by model output. No
prompt anywhere contains a JSON example, so no prompt artefact can leak into the text.

**Sampling is a pure function of `(SEED, i)`.** `sample_spec(7331)` is byte-identical on any
machine, in any batch order, whether or not the run crashed and resumed.

---

## Validation

Four checks were run before release. All numbers below come from the accompanying EDA notebook
and are reproducible from the released files.

### 1. Does salesperson skill actually drive the outcome?

![Skill signal](https://huggingface.co/datasets/elg4/Gym_Salesman_Dataset/resolve/main/skill_signal.png)

| skill | SUCCESS rate | avg objections resolved | n |
|---|---|---|---|
| novice | 22.8% | 0.796 | 3,937 |
| average | 28.3% | 0.962 | 4,092 |
| expert | 30.9% | 1.036 | 3,971 |

Monotonic, as required. Skill was sampled **independently** of customer difficulty, so the only
path from skill to outcome runs through the salesperson resolving more objections. Had this
been flat, the labels would be noise and the dataset would look fine while teaching nothing.

**But keep the magnitude in perspective — see the persona confound below.**

### 2. Is the evaluator trustworthy? Cohen's κ against human labels

46 objection-judgements across 20 randomly-sampled conversations were hand-labelled by an
author, blind to the evaluator's verdict.

| | |
|---|---|
| Judgements | 46 |
| Raw agreement | 67.4% |
| **Cohen's κ** | **0.218** |

**This is below our own 0.4 threshold, and we report it as a genuine finding rather than a
footnote.** The evaluator's per-objection verdicts should be treated as *noisy*, not
authoritative — and therefore so should `outcome`.

Investigating *why* produced the most useful result in the whole audit:

![Objection difficulty](https://huggingface.co/datasets/elg4/Gym_Salesman_Dataset/resolve/main/objection_difficulty.png)

Resolution rates range from **2% to 80%** across the 16 objection types (corpus mean 37%):

| easiest to resolve | | hardest to resolve | |
|---|---|---|---|
| `equipment_confusion` | 80% | `no_time` | 2% |
| `contract_lock` | 76% | `peak_congestion` | 7% |
| `changing_room` | 74% | `price_gap` | 10% |

The criteria are not equally satisfiable. `no_time`, for instance, requires the salesperson to
state specific opening hours **and** explicitly connect them to an irregular study schedule — a
conjunction that is hard to satisfy and hard for two judges to agree on. That is the mechanism
behind the low κ, and it is what a v4 of this dataset should fix first.

### 3. The persona confound — read this before training anything

![Persona signal](https://huggingface.co/datasets/elg4/Gym_Salesman_Dataset/resolve/main/persona_signal.png)

Because objections are owned by personas, and criterion difficulty varies enormously, a
customer's persona partly determines the outcome before the salesperson says a word. Both
panels rank the four personas identically:

| persona | SUCCESS rate | share of its objections resolved |
|---|---|---|
| Yuval — Procrastinating CS Student | 15.9% | 26.9% |
| Ariel — Impulsive Commitment-Phobe | 21.7% | 30.8% |
| Omer — Analytical Bargain Hunter | 27.8% | 38.5% |
| Noam — Anxious First-Timer | 44.4% | 53.0% |

| effect | span |
|---|---|
| skill (novice → expert) | **8.1 points** |
| persona (Yuval → Noam) | **28.5 points** |

**Persona moves the outcome roughly 3.5× more than skill does.** The skill effect is real,
causal, and monotonic — but it is not the dominant one. Anyone training a classifier on
`outcome` should expect persona, not skill, to carry most of the predictive weight, and should
control for it.

### 4. Leakage audit — can a classifier cheat?

![Leakage audit](https://huggingface.co/datasets/elg4/Gym_Salesman_Dataset/resolve/main/leakage_audit.png)

TF-IDF + logistic regression trained on **the salesperson's turns only, excluding his final
pitch** — i.e. everything said before the customer decided anything. Stratified by `n_turns`,
because pooling lengths lets the classifier use length as a proxy.

| stratum | probe accuracy | majority baseline | n | reading |
|---|---|---|---|---|
| 6-turn | 64.2% ±1.1 | 63.4% | 2,976 | healthy band, essentially at baseline |
| 8-turn | 69.4% ±0.5 | 75.7% | 9,024 | borderline band by raw accuracy, but **below** its own baseline |

Both readings are reported rather than the flattering one. Neither stratum approaches the 70%
leakage line, and neither beats majority-class guessing by a meaningful margin. For reference,
an earlier version of this dataset scored **90.4%** here — the rewrite of the skill
descriptions (behaviour, not style) is what closed that gap.

---

## Known limitations

Stated honestly; none of these are hidden in the data.

| limitation | measurement |
|---|---|
| **Evaluator agreement is weak** | κ = 0.218 over 46 judgements. Criterion wording, not model capability, is the main cause. |
| **Persona dominates skill** | 28.5-point persona span vs 8.1-point skill span. A confound, not a bug, but it must be controlled for. |
| **Criteria are unevenly satisfiable** | 2%–80% resolution rate across the 16 objection types. |
| **Objection voicing is imperfect** | 79.7% of hidden objections detectably voiced (23,982/30,072). Keyword matching is a *floor* — indirect phrasings are missed, so the true rate is higher. |
| **Customers are too polite** | 20.1% of customer turns (6,637/33,024) contain thanking or agreement language the prompt forbade. Cosmetic: the evaluator judges the advisor's answer, not the customer's manners. |
| **Occasional fact hallucination** | 0.65% of salesperson turns (294/45,024) invent terms contradicting the fact sheet. |
| **Price is usually unstated** | The advisor quotes the sampled membership price in only 22.8% of conversations; 77.2% never state it. Direct contradictions are negligible (9/12,000 = 0.1%), but 4.1% write the figure with `$` instead of `₪`. Do not assume the `price` field appears in the text. |
| **Single model family** | Generator and evaluator are both Qwen2.5. A different judge family would give an independent read on κ. |
| **Fictional and English-only** | Gyms, prices, and facts are invented. Prices are nominally in shekels; the setting is otherwise culture-neutral. |

### Structural guarantees (all verified, zero violations)

| check | result |
|---|---|
| Every turn is exactly `{role, text}` | 0 violations |
| Roles alternate `sales → customer` | 0 violations |
| `outcome` ∈ `{SUCCESS, FAILURE}` | 0 violations |
| `decision_turn_index` valid | 0 violations |
| Duplicate `sample_id` | 0 |
| Exact duplicate dialogues | 0 |
| Near-duplicates (cosine ≥ 0.95, MiniLM-L6-v2) | 3, removed |

---

## Intended uses

**Well suited to:**
- Retrieval over sales conversations — "find me a call like the one I'm about to walk into"
- Training or evaluating a conversational sales-coaching assistant
- Studying objection handling and which resolution strategies co-occur with `SUCCESS`
- A worked example of emergent labelling and leakage auditing in synthetic data

**Use with care:**
- **Outcome classification.** Achievable, but persona will dominate. Report per-persona
  performance, not just aggregate accuracy, or the number will be misleading.
- **Anything treating `outcome` as ground truth.** κ = 0.218 says the labels are noisy.

**Not suitable for:**
- Real-world sales performance benchmarking — this is simulated data about fictional gyms
- Training systems that make decisions about real people
- Any claim about actual human sales behaviour

---

## Loading

```python
from datasets import load_dataset

# conversations (what a model may see)
ds = load_dataset("elg4/Gym_Salesman_Dataset", "conversations", split="train")

# latent state — for analysis only, never as a feature
meta = load_dataset("elg4/Gym_Salesman_Dataset", "latent_state", split="train")

print(ds[0]["outcome"])
for turn in ds[0]["dialogue"]:
    print(f"{turn['role']:9s}: {turn['text']}")
```

---

## Reproduction

The full pipeline is released as two notebooks:

1. **Generation** — Stage A–E, batched in shards of 1,000 with checkpoint-and-resume so a
   Colab disconnect never costs more than one shard. ~2 hours on an L4.
2. **EDA & validation** — every number and every chart on this card, CPU-only.

`SEED = 42`. Changing it produces a completely different dataset, not a variation on this one.

---

## Citation

```bibtex
@misc{gym_salesman_dataset,
  title  = {Gym Salesman Dataset: Synthetic Sales Conversations with Emergent Labels},
  year   = {2026},
  url    = {https://huggingface.co/datasets/elg4/Gym_Salesman_Dataset}
}
```

---

<sub>Built as the final project for an applied AI course. The dataset is synthetic; the gyms,
prices, and customers do not exist. Findings above — including the ones that are unflattering —
are reported as measured.</sub>
