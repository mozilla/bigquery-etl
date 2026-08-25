"""Highwind: covariate-adjusted sequential analysis of Nimbus experiments.

Computes per-branch sufficient statistics in BigQuery and turns them into confidence intervals with
gbstats, so that no per-client row ever leaves the warehouse. See `analysis.py` for the flow.

A proof of concept: Firefox Desktop experiments only, a hard-coded guardrail metric set rather than
one resolved from metric-hub, and a full recompute every run. Each of those limits is deliberate and
noted where it bites.
"""

from .analysis import run_daily_job, systemic_failure

__all__ = ["run_daily_job", "systemic_failure"]
