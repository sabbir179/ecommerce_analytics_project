CREATE OR REPLACE TABLE gold_daily_kpis AS
WITH daily_sessions AS (
    SELECT
        CAST(session_start AS DATE) AS event_date,
        COUNT(DISTINCT session_id) AS sessions,
        COUNT(DISTINCT user_id) AS active_users
    FROM clean_sessions
    GROUP BY 1
),
daily_orders AS (
    SELECT
        CAST(order_timestamp AS DATE) AS event_date,
        COUNT(*) AS orders,
        SUM(revenue) AS revenue,
        AVG(revenue) AS average_order_value
    FROM clean_orders
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
ORDER BY s.event_date;

