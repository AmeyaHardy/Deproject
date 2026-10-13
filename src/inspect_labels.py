import csv
from pathlib import Path

root = Path("data/raw")

files = [
    root / "OneDrive_1_10-12-2026" / "Training and Testing Sets" / "UNSW_NB15_training-set.csv",
    root / "OneDrive_1_10-12-2026" / "Training and Testing Sets" / "UNSW_NB15_testing-set.csv",
]

for path in files:
    print("\n" + "=" * 70)
    print("FILE:", path.name)

    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        print("TOTAL COLUMNS:", len(reader.fieldnames))
        print("ALL COLUMNS:")
        print(reader.fieldnames)
        print("\nLAST 8 COLUMNS:")
        print(reader.fieldnames[-8:])

        print("\nFIRST 3 RECORDS:")
        for i, row in enumerate(reader):
            if i == 3:
                break
            print({
                k: row[k]
                for k in reader.fieldnames[-8:]
            })
