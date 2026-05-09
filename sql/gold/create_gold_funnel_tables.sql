CREATE OR REPLACE TABLE gold_funnel_metrics AS
WITH ordered_steps AS (
    SELECT *
    FROM (VALUES
        ('homepage_view', 1),
        ('search', 2),
        ('product_view', 3),
        ('add_to_basket', 4),
        ('checkout_start', 5),
        ('payment_complete', 6)
    ) AS steps(event_name, step_order)
),
step_counts AS (
    SELECT event_name, COUNT(DISTINCT session_id) AS sessions
    FROM clean_events
    GROUP BY event_name
)
SELECT
    o.event_name,
    o.step_order,
    COALESCE(c.sessions, 0) AS sessions,
    COALESCE(c.sessions, 0)::DOUBLE / NULLIF(FIRST_VALUE(COALESCE(c.sessions, 0)) OVER (ORDER BY o.step_order), 0) AS conversion_from_start,
    COALESCE(c.sessions, 0)::DOUBLE / NULLIF(LAG(COALESCE(c.sessions, 0)) OVER (ORDER BY o.step_order), 0) AS step_conversion_rate,
    1 - COALESCE(c.sessions, 0)::DOUBLE / NULLIF(LAG(COALESCE(c.sessions, 0)) OVER (ORDER BY o.step_order), 0) AS dropoff_rate
FROM ordered_steps o
LEFT JOIN step_counts c USING (event_name)
ORDER BY o.step_order;

