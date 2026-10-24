-- A forecast as Pipewright reads it: the series' IDs, forecast_timestamp, forecast_value (also as _pw_prediction)
-- and its interval; for evaluation, joined to the held-out actual values (a failed series is kept for its status).
CREATE OR REPLACE TABLE `{out_table}`
OPTIONS (labels = [{labels}], expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {keep_days} DAY))
AS
WITH f AS (
{forecast}
)
SELECT f.*, f.forecast_value AS _pw_prediction{actual}
FROM f{join}
