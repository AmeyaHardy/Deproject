from pathlib import Path
import csv

root = Path("data/raw")

for path in sorted(root.rglob("*.csv")):
    print("\n" + "=" * 75)
    print(f"FILE: {path}")
    print(f"SIZE: {path.stat().st_size / (1024 * 1024):.2f} MB")

    try:
        with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as f:
            reader = csv.reader(f)

            for i, row in enumerate(reader):
                if i >= 2:
                    break
                print(f"ROW {i + 1} ({len(row)} columns): {row[:10]}")
    except Exception as e:
        print(f"ERROR: {e}")
