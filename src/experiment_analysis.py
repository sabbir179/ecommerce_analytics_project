import math

import duckdb
import pandas as pd
from scipy import stats

from config import DB_PATH
from metrics import get_experiment_results


def two_proportion_result(control_conversions, control_n, variant_conversions, variant_n):
    control_rate = control_conversions / control_n
    variant_rate = variant_conversions / variant_n
    uplift = variant_rate - control_rate
    pooled = (control_conversions + variant_conversions) / (control_n + variant_n)
    se = math.sqrt(pooled * (1 - pooled) * (1 / control_n + 1 / variant_n))
    z_score = uplift / se if se else 0
    p_value = 2 * (1 - stats.norm.cdf(abs(z_score)))
    ci_low = uplift - 1.96 * se
    ci_high = uplift + 1.96 * se
    return {
        "control_rate": control_rate,
        "variant_rate": variant_rate,
        "uplift": uplift,
        "uplift_pct": uplift / control_rate if control_rate else 0,
        "p_value": p_value,
        "ci_low": ci_low,
        "ci_high": ci_high,
        "is_significant": p_value < 0.05,
    }


def overall_experiment_summary(filters=None):
    df = get_experiment_results(filters)
    df = df[(df["segment_type"] == "overall") & (df["segment_value"] == "all")]
    pivot = df.set_index("variant")
    stats_result = two_proportion_result(
        pivot.loc["control", "conversions"],
        pivot.loc["control", "sessions"],
        pivot.loc["variant", "conversions"],
        pivot.loc["variant", "sessions"],
    )
    impact = stats_result["uplift"] * pivot.loc["variant", "sessions"] * pivot.loc["variant", "avg_order_value"]
    return pd.Series({**stats_result, "commercial_impact_estimate": impact}).to_dict()
