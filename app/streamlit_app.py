import sys
from pathlib import Path
import json
import re

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

sys.path.append(str(Path(__file__).resolve().parents[1] / "src"))

from ai_agents import DataQualityAgent, ReportAgent  # noqa: E402
from experiment_analysis import overall_experiment_summary  # noqa: E402
from metrics import (  # noqa: E402
    get_daily_kpis,
    get_executive_metrics,
    get_experiment_results,
    get_filter_options,
    get_funnel_metrics,
    get_segment_performance,
)


st.set_page_config(page_title="E-commerce Product Analytics AI", layout="wide")


st.markdown(
    """
    <style>
    .kpi-card {
        border: 1px solid #e4e7ec;
        border-radius: 8px;
        padding: 1rem 1.05rem;
        background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%);
        box-shadow: 0 1px 2px rgba(16, 24, 40, 0.05);
        min-height: 118px;
        margin-bottom: 0.75rem;
    }
    .kpi-card.accent {
        border-left: 4px solid #f05252;
    }
    .kpi-label {
        color: #667085;
        font-size: 0.84rem;
        font-weight: 700;
        margin-bottom: 0.65rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }
    .kpi-value {
        color: #1f2430;
        font-size: 1.85rem;
        line-height: 1.1;
        font-weight: 750;
        white-space: nowrap;
    }
    .kpi-help {
        color: #7a8496;
        font-size: 0.78rem;
        margin-top: 0.55rem;
        line-height: 1.3;
    }
    .impact-card {
        border: 1px solid #ffd6d6;
        border-radius: 8px;
        padding: 1rem 1.05rem;
        background: #fff7f7;
        box-shadow: 0 1px 2px rgba(16, 24, 40, 0.04);
        margin: 0.35rem 0 1.5rem 0;
        max-width: 430px;
    }
    @media (max-width: 760px) {
        .kpi-value {
            font-size: 1.55rem;
        }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def fmt_currency(value):
    return f"GBP {value:,.0f}"


def fmt_compact_currency(value):
    if abs(value) >= 1_000_000:
        return f"GBP {value / 1_000_000:.2f}M"
    if abs(value) >= 1_000:
        return f"GBP {value / 1_000:.1f}K"
    return fmt_currency(value)


def kpi_card(label, value, helper="", accent=False):
    accent_class = " accent" if accent else ""
    st.markdown(
        f"""
    <div class="kpi-card{accent_class}">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-help">{helper}</div>
    </div>
    """,
        unsafe_allow_html=True,
    )


def render_impact_card(value, helper):
    st.markdown(
        f"""
        <div class="impact-card">
            <div class="kpi-label">Commercial Impact Estimate</div>
            <div class="kpi-value">{value}</div>
            <div class="kpi-help">{helper}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def parse_jsonish(value):
    if not isinstance(value, str):
        return value

    cleaned = value.strip()
    cleaned = re.sub(r"^```(?:json)?", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"```$", "", cleaned).strip()

    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", cleaned, flags=re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                return value
    return value


def display_insight(insight):
    insight = parse_jsonish(insight)

    if isinstance(insight, str):
        st.markdown(f"**Insight:** {insight}")
        return

    if isinstance(insight, dict):
        evidence = insight.get("evidence", "")
        if isinstance(evidence, dict):
            evidence = " | ".join(f"{key}: {value}" for key, value in evidence.items())

        st.markdown(f"**Insight:** {insight.get('insight', '')}")
        st.markdown(f"**Evidence:** {evidence}")
        st.markdown(f"**Recommendation:** {insight.get('recommendation', '')}")
        st.markdown(f"**Confidence level:** {insight.get('confidence_level', '')}")
        if insight.get("validation_notes"):
            st.markdown(f"**Notes:** {insight.get('validation_notes')}")
        return

    st.write(insight)


def display_validation(validation):
    if isinstance(validation, str):
        try:
            validation = json.loads(validation)
        except json.JSONDecodeError:
            st.write(validation)
            return

    if isinstance(validation, dict):
        st.markdown(f"**Support rating:** {validation.get('support_rating', '')}")
        st.markdown(f"**Validation notes:** {validation.get('validation_notes', '')}")
        return

    st.write(validation)


def display_data_quality(data_quality):
    cols = st.columns(3)
    cols[0].metric("Clean users", f"{data_quality.get('clean_users_rows', 0):,}")
    cols[1].metric("Clean sessions", f"{data_quality.get('clean_sessions_rows', 0):,}")
    cols[2].metric("Clean orders", f"{data_quality.get('clean_orders_rows', 0):,}")
    cols = st.columns(3)
    cols[0].metric("Clean events", f"{data_quality.get('clean_events_rows', 0):,}")
    cols[1].metric("Duplicate events", f"{data_quality.get('duplicate_events', 0):,}")
    cols[2].metric("Missing timestamps", f"{data_quality.get('missing_event_timestamps', 0):,}")

    suspicious = data_quality.get("suspicious_funnel_drops", [])
    if suspicious:
        st.warning(f"Suspicious funnel drops detected: {suspicious}")
    else:
        st.success("No suspicious funnel drops detected.")


def display_recommendation(recommendation):
    st.markdown(f"**Priority:** {recommendation.get('priority', '')}")
    st.markdown(f"**Reason:** {recommendation.get('reason', '')}")

    mobile_metrics = recommendation.get("mobile_metrics", [])
    if mobile_metrics:
        st.caption("Mobile segment evidence")
        st.dataframe(pd.DataFrame(mobile_metrics), use_container_width=True, hide_index=True)

    low_segments = recommendation.get("lowest_converting_segments", [])
    if low_segments:
        st.caption("Lowest converting segments")
        st.dataframe(pd.DataFrame(low_segments), use_container_width=True, hide_index=True)


def filters_sidebar():
    options = get_filter_options()
    st.sidebar.header("Filters")
    return {
        "device_type": st.sidebar.selectbox("Device", options["device_type"]),
        "region": st.sidebar.selectbox("Region", options["region"]),
        "customer_type": st.sidebar.selectbox("Customer type", options["customer_type"]),
        "loyalty_flag": st.sidebar.selectbox("Loyalty", options["loyalty_flag"]),
        "channel": st.sidebar.selectbox("Channel", options["channel"]),
    }


def executive_overview(filters):
    st.title("E-commerce Product Funnel & Experimentation Analytics")
    metrics = get_executive_metrics(filters)
    daily = get_daily_kpis(filters)
    exp = overall_experiment_summary(filters)

    cols = st.columns([1.25, 1, 1, 1, 1, 1])
    with cols[0]:
        kpi_card("Revenue", fmt_compact_currency(metrics["revenue"]), "Completed order revenue", accent=True)
    with cols[1]:
        kpi_card("Orders", f"{metrics['orders']:,.0f}", "Payment-complete orders")
    with cols[2]:
        kpi_card("Conversion", f"{metrics['conversion_rate']:.2%}", "Orders per session")
    with cols[3]:
        kpi_card("AOV", fmt_currency(metrics["average_order_value"]), "Average order value")
    with cols[4]:
        kpi_card("Active Users", f"{metrics['active_users']:,.0f}", "Users with a session")
    with cols[5]:
        kpi_card("Experiment Uplift", f"{exp['uplift_pct']:.2%}", "Relative variant lift", accent=True)

    render_impact_card(
        fmt_currency(metrics["commercial_impact_estimate"]),
        "Estimated value from applying measured conversion uplift to selected traffic.",
    )

    revenue_fig = px.line(daily, x="event_date", y="revenue", markers=False)
    revenue_fig.update_layout(height=320, title="Daily Revenue", yaxis_title="Revenue")
    st.plotly_chart(revenue_fig, use_container_width=True)

    cols = st.columns(2)
    orders_fig = px.line(daily, x="event_date", y="orders", markers=False)
    orders_fig.update_layout(height=300, title="Daily Orders", yaxis_title="Orders")
    cols[0].plotly_chart(orders_fig, use_container_width=True)

    conversion_fig = px.line(daily, x="event_date", y="conversion_rate", markers=False)
    conversion_fig.update_layout(height=300, title="Daily Conversion Rate", yaxis_title="Conversion rate")
    conversion_fig.update_yaxes(tickformat=".1%")
    cols[1].plotly_chart(conversion_fig, use_container_width=True)


def funnel_analysis(filters):
    st.header("Funnel Analysis")
    funnel = get_funnel_metrics(filters)
    fig = go.Figure(go.Funnel(y=funnel["event_name"], x=funnel["sessions"], textinfo="value+percent initial"))
    fig.update_layout(height=520)
    st.plotly_chart(fig, use_container_width=True)
    table = funnel.copy()
    table["step_conversion_rate"] = table["step_conversion_rate"].map("{:.2%}".format)
    table["dropoff_rate"] = table["dropoff_rate"].map("{:.2%}".format)
    st.dataframe(table, use_container_width=True, hide_index=True)


def experimentation(filters):
    st.header("Experimentation")
    st.caption(active_filter_text(filters))
    results = get_experiment_results(filters)
    overall = results[(results["segment_type"] == "overall") & (results["segment_value"] == "all")]
    exp = overall_experiment_summary(filters)

    cols = st.columns(5)
    cols[0].metric("Control conversion", f"{exp['control_rate']:.2%}")
    cols[1].metric("Variant conversion", f"{exp['variant_rate']:.2%}")
    cols[2].metric("Uplift", f"{exp['uplift_pct']:.2%}")
    cols[3].metric("p-value", f"{exp['p_value']:.4f}")
    cols[4].metric("Impact", fmt_currency(exp["commercial_impact_estimate"]))

    fig = px.bar(overall, x="variant", y="conversion_rate", color="variant", text_auto=".2%")
    fig.update_layout(showlegend=False, height=380)
    st.plotly_chart(fig, use_container_width=True)

    st.info(
        f"The 95% confidence interval for absolute uplift is {exp['ci_low']:.2%} to {exp['ci_high']:.2%}. "
        f"The result is {'statistically significant' if exp['is_significant'] else 'not statistically significant'} at p < 0.05."
    )
    st.dataframe(results, use_container_width=True, hide_index=True)


def segment_analysis(filters):
    st.header("Segment Analysis")
    st.caption(active_filter_text(filters))
    segments = get_segment_performance(filters)
    for segment_type in ["device_type", "region", "customer_type", "loyalty_flag", "channel"]:
        df = segments[segments["segment_type"] == segment_type]
        fig = px.bar(df, x="segment_value", y="conversion_rate", color="segment_value", text_auto=".2%")
        fig.update_layout(title=segment_type.replace("_", " ").title(), showlegend=False, height=360)
        st.plotly_chart(fig, use_container_width=True)

    mobile = segments[(segments["segment_type"] == "device_type") & (segments["segment_value"] == "mobile")]
    if not mobile.empty:
        st.warning(
            f"Mobile conversion is {mobile.iloc[0]['conversion_rate']:.2%}. Review add-to-basket to checkout-start friction before scaling paid traffic."
        )


def active_filter_text(filters):
    active = {key: value for key, value in filters.items() if value != "All"}
    if not active:
        return "Active filters: All traffic"
    return "Active filters: " + ", ".join(f"{key.replace('_', ' ')} = {value}" for key, value in active.items())


def ai_assistant(filters):
    st.header("AI Insight Assistant")
    st.caption(active_filter_text(filters))
    question = st.text_input(
        "Ask a product analytics question",
        value="Did the experiment work and what should I tell senior stakeholders?",
    )
    if st.button("Generate insight", type="primary"):
        with st.spinner("Analysing metrics and retrieving business context..."):
            report = ReportAgent().run(question, filters=filters)
            st.subheader("Insight")
            display_insight(report["insight"])
            st.subheader("Validation")
            display_validation(report["validation"])
            st.subheader("Data quality")
            display_data_quality(report["data_quality"])
            st.subheader("Recommendation")
            display_recommendation(report["recommendation"])

    with st.expander("Data quality checks"):
        display_data_quality(DataQualityAgent().run())


def main():
    try:
        filters = filters_sidebar()
        tabs = st.tabs(
            [
                "Executive Overview",
                "Funnel Analysis",
                "Experimentation",
                "Segment Analysis",
                "AI Insight Assistant",
            ]
        )
        with tabs[0]:
            executive_overview(filters)
        with tabs[1]:
            funnel_analysis(filters)
        with tabs[2]:
            experimentation(filters)
        with tabs[3]:
            segment_analysis(filters)
        with tabs[4]:
            ai_assistant(filters)
    except Exception as exc:
        st.error("The dashboard could not load. Run the data pipeline commands in the README first.")
        st.exception(exc)


if __name__ == "__main__":
    main()
