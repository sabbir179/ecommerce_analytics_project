CREATE OR REPLACE TABLE clean_users AS
SELECT
    CAST(user_id AS BIGINT) AS user_id,
    region,
    customer_type,
    CAST(loyalty_flag AS BOOLEAN) AS loyalty_flag,
    CAST(signup_date AS DATE) AS signup_date
FROM raw_users
WHERE user_id IS NOT NULL;

CREATE OR REPLACE TABLE clean_products AS
SELECT
    CAST(product_id AS BIGINT) AS product_id,
    product_name,
    category,
    CAST(base_price AS DOUBLE) AS base_price,
    CAST(margin_rate AS DOUBLE) AS margin_rate
FROM raw_products
WHERE product_id IS NOT NULL;

CREATE OR REPLACE TABLE clean_sessions AS
SELECT
    CAST(session_id AS BIGINT) AS session_id,
    CAST(user_id AS BIGINT) AS user_id,
    CAST(session_start AS TIMESTAMP) AS session_start,
    lower(device_type) AS device_type,
    lower(channel) AS channel
FROM raw_sessions
WHERE session_id IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY session_id ORDER BY session_start) = 1;

CREATE OR REPLACE TABLE clean_events AS
SELECT
    CAST(event_id AS BIGINT) AS event_id,
    CAST(session_id AS BIGINT) AS session_id,
    CAST(user_id AS BIGINT) AS user_id,
    CAST(event_timestamp AS TIMESTAMP) AS event_timestamp,
    lower(event_name) AS event_name,
    TRY_CAST(product_id AS BIGINT) AS product_id,
    lower(device_type) AS device_type,
    region,
    customer_type,
    CAST(loyalty_flag AS BOOLEAN) AS loyalty_flag,
    lower(channel) AS channel
FROM raw_events
WHERE event_id IS NOT NULL
  AND event_timestamp IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY event_timestamp) = 1;

CREATE OR REPLACE TABLE clean_orders AS
SELECT
    CAST(order_id AS BIGINT) AS order_id,
    CAST(session_id AS BIGINT) AS session_id,
    CAST(user_id AS BIGINT) AS user_id,
    CAST(order_timestamp AS TIMESTAMP) AS order_timestamp,
    CAST(revenue AS DOUBLE) AS revenue,
    CAST(basket_value AS DOUBLE) AS basket_value,
    CAST(items_count AS BIGINT) AS items_count,
    lower(device_type) AS device_type,
    region,
    customer_type,
    CAST(loyalty_flag AS BOOLEAN) AS loyalty_flag,
    lower(channel) AS channel,
    lower(variant) AS variant
FROM raw_orders
WHERE order_id IS NOT NULL
  AND revenue >= 0
QUALIFY row_number() OVER (PARTITION BY order_id ORDER BY order_timestamp) = 1;

CREATE OR REPLACE TABLE clean_experiment_assignments AS
SELECT
    CAST(user_id AS BIGINT) AS user_id,
    experiment_name,
    lower(variant) AS variant,
    CAST(assigned_at AS TIMESTAMP) AS assigned_at
FROM raw_experiment_assignments
WHERE user_id IS NOT NULL
QUALIFY row_number() OVER (PARTITION BY user_id, experiment_name ORDER BY assigned_at) = 1;

