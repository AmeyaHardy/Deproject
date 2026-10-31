import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score
from scipy import stats
from src.anomaly.session_split import load_raw_dataframe, assign_sessions, session_split, get_feature_matrix
from src.anomaly.preprocessing import FrozenPreprocessor

SEED = 42
np.random.seed(SEED)
print(f"SEED: {SEED}")

df = load_raw_dataframe(limit=200000, verbose=False)
df = assign_sessions(df)

for (s_train, s_test) in [(0, 1), (1, 0)]:
    train_df, test_df = session_split(df, train_session=s_train, test_session=s_test, verbose=False)
    X_train_raw, y_train = get_feature_matrix(train_df)
    X_test_raw, y_test = get_feature_matrix(test_df)

    pp = FrozenPreprocessor()
    X_train_proc = pp.fit_transform(X_train_raw)
    X_test_proc = pp.transform(X_test_raw)

    rng = np.random.RandomState(SEED)
    aucs = []
    for i in range(25):
        y_shuffled = y_train.sample(frac=1, random_state=rng.randint(0, 100000)).reset_index(drop=True)
        rf = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=SEED + i, n_jobs=-1)
        rf.fit(X_train_proc, y_shuffled)
        score = rf.predict_proba(X_test_proc)[:, 1]
        auc = roc_auc_score(y_test, score)
        aucs.append(auc)

    m = np.mean(aucs)
    s = np.std(aucs, ddof=1)
    ci = stats.t.interval(0.95, df=len(aucs)-1, loc=m, scale=s/np.sqrt(len(aucs)))
    print(f"\nSession {s_train} -> Session {s_test} (25 runs):")
    print(f"  Mean ROC-AUC: {m:.4f}")
    print(f"  Std:          {s:.4f}")
    print(f"  Min / Max:    {np.min(aucs):.4f} / {np.max(aucs):.4f}")
    print(f"  95% CI:       [{ci[0]:.4f}, {ci[1]:.4f}]")
    print(f"  Contains 0.5? {ci[0] <= 0.5 <= ci[1]}")
