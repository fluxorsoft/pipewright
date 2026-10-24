-- An imported model's predictions (promotion by files), shaped as predict.sql's: the rows' own columns, carried
-- through the model as one STRUCT (an imported TRANSFORM model rewrites the feature columns), then the outputs.
CREATE OR REPLACE TABLE `{out_table}`
OPTIONS (labels = [{labels}], expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {keep_days} DAY))
AS
SELECT p._pw_raw.*, {outputs}
FROM ML.PREDICT(MODEL `{model}`, ({inner})) AS p
