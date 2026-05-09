CREATE OR REPLACE TABLE gold_experiment_results AS
WITH session_base AS (
    SELECT
        s.session_id,
        s.user_id,
        ea.variant,
        s.device_type,
        s.channel,
        u.region,
        u.customer_type,
        u.loyalty_flag,
        CASE WHEN o.order_id IS NULL THEN 0 ELSE 1 END AS converted,
        COALESCE(o.revenue, 0) AS revenue
    FROM clean_sessions s
    JOIN clean_users u USING (user_id)
    JOIN clean_experiment_assignments ea USING (user_id)
    LEFT JOIN clean_orders o USING (session_id)
),
segments AS (
    SELECT 'overall' AS segment_type, 'all' AS segment_value, variant, * EXCLUDE(variant) FROM session_base
    UNION ALL
    SELECT 'device_type', device_type, variant, * EXCLUDE(variant) FROM session_base
    UNION ALL
    SELECT 'customer_type', customer_type, variant, * EXCLUDE(variant) FROM session_base
    UNION ALL
    SELECT 'channel', channel, variant, * EXCLUDE(variant) FROM session_base
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
GROUP BY 1, 2, 3;

