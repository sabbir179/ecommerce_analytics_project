CREATE OR REPLACE TABLE gold_segment_performance AS
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
    JOIN clean_users u USING (user_id)
    LEFT JOIN clean_orders o USING (session_id)
),
segments AS (
    SELECT 'device_type' AS segment_type, device_type AS segment_value, * FROM session_base
    UNION ALL
    SELECT 'region', region, * FROM session_base
    UNION ALL
    SELECT 'customer_type', customer_type, * FROM session_base
    UNION ALL
    SELECT 'loyalty_flag', loyalty_flag, * FROM session_base
    UNION ALL
    SELECT 'channel', channel, * FROM session_base
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
GROUP BY 1, 2;

CREATE OR REPLACE TABLE gold_product_performance AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    COUNT(DISTINCT e.session_id) AS product_view_sessions,
    COUNT(DISTINCT CASE WHEN e.event_name='add_to_basket' THEN e.session_id END) AS add_to_basket_sessions,
    AVG(p.base_price) AS average_price,
    AVG(p.margin_rate) AS margin_rate
FROM clean_products p
LEFT JOIN clean_events e USING (product_id)
GROUP BY 1, 2, 3;

