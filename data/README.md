# Gym Salesman dataset

Source: [elg4/Gym_Salesman_Dataset](https://huggingface.co/datasets/elg4/Gym_Salesman_Dataset), revision `f605f28c47c047ad722cd5c59751d1a202d391c4`, MIT license. The upstream dataset card is preserved at `source/README.md`.

| File | Rows | SHA-256 | Use |
| --- | ---: | --- | --- |
| `source/gym_v3_dataset_clean.jsonl` | 11,997 | `74cab2e2d91343365cb11ea1b54ea790c00f5a134e8c07b1b93847cbb96376b2` | Original conversations and outcomes; provenance only |
| `source/gym_v3_metadata.jsonl` | 12,000 | `a488c5c7ab6ccac5f54778a35b9075bf249e0f0d5f746f5f63742dbd2de3ed5f` | Original latent labels; provenance only |
| `prepared/gym-sales-v1.jsonl` | 11,997 | `1e2aafd5ac676fe66c5602ec67116b2d2f54053ef159c7b60da3281dcbde17ef` | Worker A input: `record_id` and dialogue `messages` only |
| `ground-truth/gym-sales-v1.jsonl` | 11,997 | `8a55fdcc9e9616e260eabf41da154bfdebb2cd829f163fba3a0c5c0557b98c21` | Evaluator input: outcome and reference labels |

Run `python3 scripts/prepare_gym_dataset.py` from the repository root to regenerate the separated files. Three metadata rows have no matching conversation because the upstream cleaned dataset removed three near duplicates.

Worker A must receive only `prepared/gym-sales-v1.jsonl`, or shards derived exclusively from it. The source and ground truth files contain answers and must stay out of Worker A images and its GCS/IAM scope. The evaluator joins predictions to ground truth by `record_id` after inference. Local Git access is not an isolation boundary; the cloud deployment must enforce this with separate buckets and service accounts.

The dataset is synthetic. Keep any future real conversation data outside Git.
