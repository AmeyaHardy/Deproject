"""
Anomaly Detection Module
========================
Implements the 3-layer anomaly detection strategy for the UNSW-NB15
network traffic streaming pipeline:

  Layer 1: Shortcut baselines (srcip-prefix rule, TTL-trio tree)
  Layer 2: Random Forest on 41 behavioral features (primary detector)
  Layer 3: Streaming integration (model export + Spark UDF scoring)

All preprocessing is fit-on-train-only (anti-leakage) and serialized
with provenance metadata for deployment into the streaming pipeline.
"""
