"""Per-distribution quantile helpers.

Predictors look up the distribution name from the sport adapter's
target_distributions dict, then call compute_quantiles() and
compute_p_ge_k() here.  Adding a new distribution requires only a new
elif branch in each function.
"""
from __future__ import annotations
import json
import math


def compute_quantiles(
    lam: float,
    distribution: str = "poisson",
    q_low: float = 0.10,
    q_high: float = 0.90,
) -> tuple[float, float]:
    """Return (q_low_val, q_high_val) for the given distribution and rate."""
    mu = max(float(lam), 1e-8)

    if distribution == "poisson":
        from scipy.stats import poisson
        return float(poisson.ppf(q_low, mu=mu)), float(poisson.ppf(q_high, mu=mu))

    elif distribution == "normal":
        # Treat mu as the mean; use sqrt(mu) as std (Poisson-like variance).
        # Suitable for targets with larger expected values (e.g. NBA points).
        from scipy.stats import norm
        std = max(math.sqrt(mu), 1e-4)
        return float(norm.ppf(q_low, loc=mu, scale=std)), float(norm.ppf(q_high, loc=mu, scale=std))

    else:
        raise ValueError(
            f"Unknown distribution '{distribution}'. "
            "Supported: 'poisson', 'normal'."
        )


def compute_p_ge_k(
    lam: float,
    k_max: int = 10,
    distribution: str = "poisson",
) -> str:
    """Return JSON array of P(X >= k) for k in 0..k_max."""
    mu = max(float(lam), 1e-8)
    arr: list[float] = []

    if distribution == "poisson":
        from scipy.stats import poisson
        for k in range(0, k_max + 1):
            p = 1.0 - poisson.cdf(k - 1, mu=mu) if k > 0 else 1.0
            arr.append(float(p))

    elif distribution == "normal":
        from scipy.stats import norm
        std = max(math.sqrt(mu), 1e-4)
        for k in range(0, k_max + 1):
            p = float(1.0 - norm.cdf(k - 0.5, loc=mu, scale=std))
            arr.append(max(0.0, min(1.0, p)))

    else:
        raise ValueError(f"Unknown distribution '{distribution}'.")

    return json.dumps(arr)
