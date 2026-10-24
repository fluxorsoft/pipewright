-- A model's predictions for some of a run's rows, reduced to what Pipewright reads: the outputs (_pw_*) next to the
-- rows' own columns.
CREATE OR REPLACE TABLE `{out_table}`
OPTIONS (labels = [{labels}], expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {keep_days} DAY))
AS
SELECT * EXCEPT ({predicted_columns}), {outputs}
FROM ML.PREDICT(MODEL `{model}`, (SELECT * EXCEPT (_pw_test) FROM `{rows_table}` WHERE {where}){threshold})
