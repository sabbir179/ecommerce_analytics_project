import duckdb
import pandas as pd

from config import DB_PATH


def query_df(sql, params=None):
    with duckdb.connect(DB_PATH, read_only=True) as con:
        return con.execute(sql, params or {}).fetchdf()


def get_executive_metrics(filters=None):
    where_clause, params = build_filter_clause(filters, prefix="s")
    sql = f"""
        WITH sessions AS (
            SELECT COUNT(DISTINCT s.session_id) AS sessions,
                   COUNT(DISTINCT s.user_id) AS active_users
            FROM clean_sessions s
            JOIN clean_users u ON s.user_id = u.user_id
            {where_clause}
        ),
        orders AS (
            SELECT COUNT(*) AS orders,
                   SUM(o.revenue) AS revenue,
                   AVG(o.revenue) AS aov
            FROM clean_orders o
            JOIN clean_sessions s ON o.session_id = s.session_id
            JOIN clean_users u ON s.user_id = u.user_id
            {where_clause}
        ),
        experiment AS (
            SELECT
                MAX(CASE WHEN variant='variant' THEN conversion_rate END) -
                MAX(CASE WHEN variant='control' THEN conversion_rate END) AS uplift
            FROM gold_experiment_results
            WHERE segment_type='overall' AND segment_value='all'
        )
        SELECT
            COALESCE(revenue, 0) AS revenue,
            COALESCE(orders, 0) AS orders,
            CASE WHEN sessions = 0 THEN 0 ELSE orders::DOUBLE / sessions END AS conversion_rate,
            COALESCE(aov, 0) AS average_order_value,
            active_users,
            COALESCE(uplift, 0) AS experiment_uplift,
            COALESCE(uplift, 0) * sessions * COALESCE(aov, 0) AS commercial_impact_estimate
        FROM sessions, orders, experiment
    """
    return query_df(sql, params).iloc[0].to_dict()


def build_filter_clause(filters=None, prefix="s"):
    filters = filters or {}
    clauses = []
    params = {}
    mapping = {
        "device_type": f"{prefix}.device_type",
        "channel": f"{prefix}.channel",
        "region": "u.region",
        "customer_type": "u.customer_type",
        "loyalty_flag": "u.loyalty_flag",
    }
    for key, column in mapping.items():
        value = filters.get(key)
        if value is None or value == "All":
            continue
        clauses.append(f"{column} = ${key}")
        params[key] = value
    return ("WHERE " + " AND ".join(clauses) if clauses else ""), params


def get_filter_options():
    return {
        "device_type": ["All"] + query_df("SELECT DISTINCT device_type FROM clean_sessions ORDER BY 1")["device_type"].tolist(),
        "region": ["All"] + query_df("SELECT DISTINCT region FROM clean_users ORDER BY 1")["region"].tolist(),
        "customer_type": ["All"] + query_df("SELECT DISTINCT customer_type FROM clean_users ORDER BY 1")["customer_type"].tolist(),
        "loyalty_flag": ["All", True, False],
        "channel": ["All"] + query_df("SELECT DISTINCT channel FROM clean_sessions ORDER BY 1")["channel"].tolist(),
    }


def get_funnel_metrics(filters=None):
    where_clause, params = build_filter_clause(filters, prefix="s")
    sql = f"""
        WITH filtered_events AS (
            SELECT e.event_name, e.session_id
            FROM clean_events e
            JOIN clean_sessions s ON e.session_id = s.session_id
            JOIN clean_users u ON s.user_id = u.user_id
            {where_clause}
        ),
        counts AS (
            SELECT event_name, COUNT(DISTINCT session_id) AS sessions
            FROM filtered_events
            GROUP BY event_name
        ),
        ordered AS (
            SELECT *
            FROM (VALUES
                ('homepage_view', 1),
                ('search', 2),
                ('product_view', 3),
                ('add_to_basket', 4),
                ('checkout_start', 5),
                ('payment_complete', 6)
            ) AS steps(event_name, step_order)
        )
        SELECT
            o.event_name,
            o.step_order,
            COALESCE(c.sessions, 0) AS sessions,
            COALESCE(c.sessions, 0)::DOUBLE / NULLIF(FIRST_VALUE(COALESCE(c.sessions, 0)) OVER (ORDER BY o.step_order), 0) AS conversion_from_start,
            COALESCE(c.sessions, 0)::DOUBLE / NULLIF(LAG(COALESCE(c.sessions, 0)) OVER (ORDER BY o.step_order), 0) AS step_conversion_rate,
            1 - COALESCE(c.sessions, 0)::DOUBLE / NULLIF(LAG(COALESCE(c.sessions, 0)) OVER (ORDER BY o.step_order), 0) AS dropoff_rate
        FROM ordered o
        LEFT JOIN counts c USING (event_name)
        ORDER BY o.step_order
    """
    df = query_df(sql, params)
    df["step_conversion_rate"] = df["step_conversion_rate"].fillna(1.0)
    df["dropoff_rate"] = df["dropoff_rate"].fillna(0.0)
    return df


def get_experiment_results(filters=None):
    where_clause, params = build_filter_clause(filters, prefix="s")
    sql = f"""
        WITH session_base AS (
            SELECT
                s.session_id,
                s.user_id,
                ea.variant,
                s.device_type,
                s.channel,
                u.region,
                u.customer_type,
                CAST(u.loyalty_flag AS VARCHAR) AS loyalty_flag,
                CASE WHEN o.order_id IS NULL THEN 0 ELSE 1 END AS converted,
                COALESCE(o.revenue, 0) AS revenue
            FROM clean_sessions s
            JOIN clean_users u ON s.user_id = u.user_id
            JOIN clean_experiment_assignments ea ON s.user_id = ea.user_id
            LEFT JOIN clean_orders o ON s.session_id = o.session_id
            {where_clause}
        ),
        segments AS (
            SELECT 'overall' AS segment_type, 'all' AS segment_value, variant, session_id, converted, revenue FROM session_base
            UNION ALL
            SELECT 'device_type', device_type, variant, session_id, converted, revenue FROM session_base
            UNION ALL
            SELECT 'region', region, variant, session_id, converted, revenue FROM session_base
            UNION ALL
            SELECT 'customer_type', customer_type, variant, session_id, converted, revenue FROM session_base
            UNION ALL
            SELECT 'loyalty_flag', loyalty_flag, variant, session_id, converted, revenue FROM session_base
            UNION ALL
            SELECT 'channel', channel, variant, session_id, converted, revenue FROM session_base
        )
        SELECT
            segment_type,
            segment_value,
            variant,
            COUNT(DISTINCT session_id) AS sessions,
            SUM(converted) AS conversions,
            SUM(converted)::DOUBLE / NULLIF(COUNT(DISTINCT session_id), 0) AS conversion_rate,
            SUM(revenue) AS revenue,
            AVG(NULLIF(revenue, 0)) AS avg_order_value
        FROM segments
        GROUP BY 1, 2, 3
        ORDER BY segment_type, segment_value, variant
    """
    return query_df(sql, params)


def get_segment_performance(filters=None):
    where_clause, params = build_filter_clause(filters, prefix="s")
    sql = f"""
        WITH session_base AS (
            SELECT
                s.session_id,
                s.user_id,
                s.device_type,
                s.channel,
                u.region,
                u.customer_type,
                CAST(u.loyalty_flag AS VARCHAR) AS loyalty_flag,
                CASE WHEN o.order_id IS NULL THEN 0 ELSE 1 END AS converted,
                COALESCE(o.revenue, 0) AS revenue
            FROM clean_sessions s
            JOIN clean_users u ON s.user_id = u.user_id
            LEFT JOIN clean_orders o ON s.session_id = o.session_id
            {where_clause}
        ),
        segments AS (
            SELECT 'device_type' AS segment_type, device_type AS segment_value, session_id, user_id, converted, revenue FROM session_base
            UNION ALL
            SELECT 'region', region, session_id, user_id, converted, revenue FROM session_base
            UNION ALL
            SELECT 'customer_type', customer_type, session_id, user_id, converted, revenue FROM session_base
            UNION ALL
            SELECT 'loyalty_flag', loyalty_flag, session_id, user_id, converted, revenue FROM session_base
            UNION ALL
            SELECT 'channel', channel, session_id, user_id, converted, revenue FROM session_base
        )
        SELECT
            segment_type,
            segment_value,
            COUNT(DISTINCT session_id) AS sessions,
            COUNT(DISTINCT user_id) AS users,
            SUM(converted) AS orders,
            SUM(converted)::DOUBLE / NULLIF(COUNT(DISTINCT session_id), 0) AS conversion_rate,
            SUM(revenue) AS revenue,
            AVG(NULLIF(revenue, 0)) AS average_order_value
        FROM segments
        GROUP BY 1, 2
        ORDER BY segment_type, conversion_rate DESC
    """
    return query_df(sql, params)


def get_daily_kpis(filters=None):
    where_clause, params = build_filter_clause(filters, prefix="s")
    sql = f"""
        WITH daily_sessions AS (
            SELECT
                CAST(s.session_start AS DATE) AS event_date,
                COUNT(DISTINCT s.session_id) AS sessions,
                COUNT(DISTINCT s.user_id) AS active_users
            FROM clean_sessions s
            JOIN clean_users u ON s.user_id = u.user_id
            {where_clause}
            GROUP BY 1
        ),
        daily_orders AS (
            SELECT
                CAST(o.order_timestamp AS DATE) AS event_date,
                COUNT(*) AS orders,
                SUM(o.revenue) AS revenue,
                AVG(o.revenue) AS average_order_value
            FROM clean_orders o
            JOIN clean_sessions s ON o.session_id = s.session_id
            JOIN clean_users u ON s.user_id = u.user_id
            {where_clause}
            GROUP BY 1
        )
        SELECT
            s.event_date,
            s.sessions,
            s.active_users,
            COALESCE(o.orders, 0) AS orders,
            COALESCE(o.revenue, 0) AS revenue,
            COALESCE(o.average_order_value, 0) AS average_order_value,
            COALESCE(o.orders, 0)::DOUBLE / NULLIF(s.sessions, 0) AS conversion_rate
        FROM daily_sessions s
        LEFT JOIN daily_orders o USING (event_date)
        ORDER BY s.event_date
    """
    return query_df(sql, params)


def metric_context():
    metrics = get_executive_metrics()
    funnel = get_funnel_metrics()
    experiment = get_experiment_results()
    segment = get_segment_performance()
    return {
        "executive": metrics,
        "funnel": funnel.to_dict("records"),
        "experiment": experiment.to_dict("records"),
        "segments": segment.to_dict("records"),
    }
