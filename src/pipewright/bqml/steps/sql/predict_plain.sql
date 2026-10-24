-- PCA's projections: the rows' own columns and principal_component_1 ... n, as ML.PREDICT gives them.
CREATE OR REPLACE TABLE `{out_table}`
OPTIONS (labels = [{labels}], expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {keep_days} DAY))
AS
SELECT *
FROM ML.PREDICT(MODEL `{model}`, (SELECT * EXCEPT (_pw_test) FROM `{rows_table}` WHERE {where}))
