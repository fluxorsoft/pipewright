-- Scoring: the predictions into the use case's output table, with the run, the model version and the time.
INSERT INTO `{output_table}` ({columns})
SELECT {values}
FROM `{scored_table}`
