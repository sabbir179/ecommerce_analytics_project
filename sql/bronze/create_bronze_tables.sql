CREATE OR REPLACE TABLE raw_users AS
SELECT * FROM read_csv_auto(getvariable('raw_path') || '/users.csv', header=true);

CREATE OR REPLACE TABLE raw_sessions AS
SELECT * FROM read_csv_auto(getvariable('raw_path') || '/sessions.csv', header=true);

CREATE OR REPLACE TABLE raw_events AS
SELECT * FROM read_csv_auto(getvariable('raw_path') || '/events.csv', header=true);

CREATE OR REPLACE TABLE raw_orders AS
SELECT * FROM read_csv_auto(getvariable('raw_path') || '/orders.csv', header=true);

CREATE OR REPLACE TABLE raw_products AS
SELECT * FROM read_csv_auto(getvariable('raw_path') || '/products.csv', header=true);

CREATE OR REPLACE TABLE raw_experiment_assignments AS
SELECT * FROM read_csv_auto(getvariable('raw_path') || '/experiment_assignments.csv', header=true);

