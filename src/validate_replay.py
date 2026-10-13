"""Quick validation of the replay_preview.jsonl output."""
import json
from pathlib import Path

lines = Path("outputs/replay_preview.jsonl").read_text(encoding="utf-8").strip().splitlines()
print(f"Total JSONL lines: {len(lines)}")

errors = 0
for i, line in enumerate(lines, 1):
    try:
        json.loads(line)
    except json.JSONDecodeError as e:
        print(f"  Line {i}: INVALID JSON - {e}")
        errors += 1

if errors == 0:
    print("All lines are valid JSON.")

first = json.loads(lines[0])
last = json.loads(lines[-1])

print()
print("=== FIRST RECORD ===")
print("Top-level keys:", list(first.keys()))
print("record_id:", first["record_id"])
print("event_time:", first["event_time"])
print("Feature keys:", list(first["features"].keys()))
print("Feature count:", len(first["features"]))
print("Evaluation:", first["evaluation"])

feat_keys = set(first["features"].keys())
leaked = feat_keys & {"id", "label", "attack_cat"}
if leaked:
    print(f"\nWARNING: Leaked columns in features: {leaked}")
else:
    print("\nNo ground-truth leakage in features. Correct.")

print()
print("=== LAST RECORD ===")
print("record_id:", last["record_id"])
print("Evaluation:", last["evaluation"])

print()
print("=== SAMPLE RECORD (pretty) ===")
print(json.dumps(first, indent=2))
