from __future__ import annotations
import json
from scipy.stats import poisson

def poisson_quantiles(lmbda: float, q_low: float = 0.10, q_high: float = 0.90) -> tuple[float,float]:
    mu = max(float(lmbda), 1e-8)
    q10 = poisson.ppf(q_low, mu=mu)
    q90 = poisson.ppf(q_high, mu=mu)
    return float(q10), float(q90)

def p_ge_k_array(lmbda: float, k_max: int = 10) -> list[float]:
    mu = max(float(lmbda), 1e-8)
    arr = []
    for k in range(0, k_max + 1):
        p = 1.0 - poisson.cdf(k - 1, mu=mu) if k > 0 else 1.0
        arr.append(float(p))
    return arr

def p_ge_k_json(lmbda: float, k_max: int = 10) -> str:
    return json.dumps(p_ge_k_array(lmbda, k_max))
