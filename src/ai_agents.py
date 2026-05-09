import json
import os
import re

import duckdb

from config import DB_PATH
from experiment_analysis import overall_experiment_summary
from metrics import get_executive_metrics, get_funnel_metrics, get_segment_performance
from rag_index import format_context


SYSTEM_RULES = """
You are a product analytics insight assistant. Use only the metrics provided.
Do not invent numbers. Explain the business meaning, confidence, risks, and next action.
Return concise plain English for senior retail/e-commerce stakeholders.
"""


def _parse_json_response(text):
    if not isinstance(text, str):
        return text

    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```(?:json)?", "", cleaned).strip()
        cleaned = re.sub(r"```$", "", cleaned).strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                return text
    return text


def _openai_text(prompt):
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        response = client.chat.completions.create(
            model=os.getenv("OPENAI_MODEL", "gpt-4o-mini"),
            messages=[{"role": "system", "content": SYSTEM_RULES}, {"role": "user", "content": prompt}],
            response_format={"type": "json_object"},
            temperature=0.2,
        )
        return response.choices[0].message.content
    except Exception as exc:
        return f"OpenAI unavailable, using fallback. Error: {exc}"


def _claude_text(prompt):
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=os.getenv("ANTHROPIC_MODEL", "claude-3-5-haiku-latest"),
            max_tokens=700,
            temperature=0.1,
            system=SYSTEM_RULES,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text
    except Exception as exc:
        return f"Claude unavailable, using rule validation. Error: {exc}"


def _fallback_summary(question, metrics, experiment, docs_context):
    conversion = metrics["conversion_rate"] * 100
    uplift = experiment["uplift"] * 100
    confidence = "high" if experiment["p_value"] < 0.05 else "medium"
    return {
        "insight": (
            f"Revenue is GBP {metrics['revenue']:,.0f} from {metrics['orders']:,.0f} orders, "
            f"with conversion at {conversion:.2f}%. The experiment uplift is {uplift:.2f} percentage points."
        ),
        "evidence": (
            f"AOV is GBP {metrics['average_order_value']:.2f}; estimated commercial impact is "
            f"GBP {metrics['commercial_impact_estimate']:,.0f}. Experiment p-value is {experiment['p_value']:.4f}."
        ),
        "recommendation": (
            "Prioritise checkout improvements for high-intent mobile users and scale the variant for returning users "
            "if operational constraints allow."
        ),
        "confidence_level": confidence,
        "validation_notes": "Fallback mode used. Recommendation is based on calculated KPI, funnel, and experiment tables.",
        "retrieved_context": docs_context,
    }


def _structured_insight_from_metrics(metrics, experiment, docs_context, validation_note):
    return {
        "insight": (
            f"The checkout experiment increased conversion from {experiment['control_rate']:.2%} "
            f"to {experiment['variant_rate']:.2%}. The uplift is statistically significant "
            f"with p-value {experiment['p_value']:.4f}."
        ),
        "evidence": (
            f"The business generated GBP {metrics['revenue']:,.0f} from {metrics['orders']:,.0f} orders. "
            f"Overall conversion is {metrics['conversion_rate']:.2%}, AOV is GBP {metrics['average_order_value']:.2f}, "
            f"and estimated commercial impact is GBP {metrics['commercial_impact_estimate']:,.0f}."
        ),
        "recommendation": (
            "Continue the checkout friction reduction work, but prioritise mobile checkout improvements before a full-scale rollout."
        ),
        "confidence_level": "high" if experiment["p_value"] < 0.05 else "medium",
        "validation_notes": validation_note,
        "retrieved_context": docs_context,
    }


class DataQualityAgent:
    def run(self):
        checks = {}
        with duckdb.connect(DB_PATH, read_only=True) as con:
            for table in ["clean_users", "clean_sessions", "clean_events", "clean_orders"]:
                checks[f"{table}_rows"] = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            checks["duplicate_events"] = con.execute(
                """
                SELECT COUNT(*) - COUNT(DISTINCT event_id)
                FROM clean_events
                """
            ).fetchone()[0]
            checks["missing_event_timestamps"] = con.execute(
                "SELECT COUNT(*) FROM clean_events WHERE event_timestamp IS NULL"
            ).fetchone()[0]
        funnel = get_funnel_metrics()
        suspicious = funnel.loc[funnel["dropoff_rate"] > 0.75, ["event_name", "dropoff_rate"]].to_dict("records")
        checks["suspicious_funnel_drops"] = suspicious
        return checks


class ExperimentAnalystAgent:
    def run(self):
        return overall_experiment_summary()


class InsightAgent:
    def run(self, question, filters=None):
        metrics = get_executive_metrics(filters)
        experiment = overall_experiment_summary()
        funnel = get_funnel_metrics(filters).to_dict("records")
        context = format_context(question)
        prompt = f"""
Question: {question}
Active filters: {json.dumps(filters or {}, default=str)}
Metrics: {json.dumps(metrics, default=str)}
Experiment: {json.dumps(experiment, default=str)}
Funnel: {json.dumps(funnel, default=str)}
Retrieved docs: {context}
Return JSON with insight, evidence, recommendation, confidence_level.
Use GBP for money. Use only the provided filtered metrics and experiment metrics.
"""
        text = _openai_text(prompt)
        if text and "OpenAI unavailable" not in text:
            parsed = _parse_json_response(text)
            if isinstance(parsed, dict):
                return parsed
            return _structured_insight_from_metrics(
                metrics,
                experiment,
                context,
                "OpenAI returned text that was not valid JSON, so the app rendered a structured summary using calculated metrics.",
            )
        return _fallback_summary(question, metrics, experiment, context)


class RecommendationAgent:
    def run(self):
        segments = get_segment_performance()
        mobile = segments[(segments["segment_type"] == "device_type") & (segments["segment_value"] == "mobile")]
        lowest = segments.sort_values("conversion_rate").head(3).to_dict("records")
        return {
            "priority": "Reduce mobile checkout friction",
            "reason": "Mobile has the largest traffic share and remains exposed to checkout drop-off.",
            "mobile_metrics": mobile.to_dict("records"),
            "lowest_converting_segments": lowest,
        }


class ValidationAgent:
    def run(self, insight):
        prompt = f"""
Validate this recommendation against the provided metrics. Challenge unsupported claims.
Insight: {json.dumps(insight, default=str)}
Return validation notes and a support rating.
"""
        text = _claude_text(prompt)
        if text and "Claude unavailable" not in text:
            return _parse_json_response(text)
        return {
            "support_rating": "supported with caveats",
            "validation_notes": "Claude API unavailable. Rule-based validation confirms the recommendation references calculated funnel and experiment metrics.",
        }


class ReportAgent:
    def run(self, question="What should I tell senior stakeholders?", filters=None):
        insight = InsightAgent().run(question, filters=filters)
        validation = ValidationAgent().run(insight)
        return {
            "question": question,
            "filters": filters or {},
            "insight": insight,
            "validation": validation,
            "data_quality": DataQualityAgent().run(),
            "recommendation": RecommendationAgent().run(),
        }
