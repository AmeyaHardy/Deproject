import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
import pandas as pd
from src.anomaly.session_split import load_raw_dataframe, assign_sessions
from src.raw_loader import FEATURE_NAMES

SEED = 42
print(f"SEED: {SEED}")

df = load_raw_dataframe(limit=200000, verbose=False)
df = assign_sessions(df)

s0 = df[df["session"] == 0]
s1 = df[df["session"] == 1]

print("=== Before global dedup ===")
print(f"S0: len={len(s0):,}, attacks={(s0['label'] == 1).sum():,} ({(s0['label'] == 1).mean()*100:.2f}%)")
print(f"S1: len={len(s1):,}, attacks={(s1['label'] == 1).sum():,} ({(s1['label'] == 1).mean()*100:.2f}%)")

df_dedup_with_label = df.drop_duplicates(subset=FEATURE_NAMES + ["label"], keep="first")
s1_after = df_dedup_with_label[df_dedup_with_label["session"] == 1]
print("\n=== After global dedup WITH label in subset (current code) ===")
print(f"S1: len={len(s1_after):,}, attacks={(s1_after['label'] == 1).sum():,} ({(s1_after['label'] == 1).mean()*100:.2f}%)")

df_dedup_no_label = df.drop_duplicates(subset=FEATURE_NAMES, keep="first")
s1_no_label = df_dedup_no_label[df_dedup_no_label["session"] == 1]
print("\n=== After global dedup WITHOUT label in subset (features only) ===")
print(f"S1: len={len(s1_no_label):,}, attacks={(s1_no_label['label'] == 1).sum():,} ({(s1_no_label['label'] == 1).mean()*100:.2f}%)")

s0_self = s0.drop_duplicates(subset=FEATURE_NAMES + ["label"], keep="first")
s1_self = s1.drop_duplicates(subset=FEATURE_NAMES + ["label"], keep="first")
print("\n=== Within-session dedup (each session independently) ===")
print(f"S0: len={len(s0_self):,}, attacks={(s0_self['label'] == 1).sum():,} ({(s0_self['label'] == 1).mean()*100:.2f}%)")
print(f"S1: len={len(s1_self):,}, attacks={(s1_self['label'] == 1).sum():,} ({(s1_self['label'] == 1).mean()*100:.2f}%)")
