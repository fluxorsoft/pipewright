-- One numeric column of a run's rows, for a PSI baseline: its distinct values when few, else its deciles.
SELECT COUNT(DISTINCT v) AS distinct_values, APPROX_QUANTILES(v, 10) AS quantiles,
  ARRAY_AGG(DISTINCT v ORDER BY v LIMIT 11) AS smallest
FROM (SELECT CAST(`{column}` AS FLOAT64) AS v FROM `{table}` WHERE {where})
WHERE v IS NOT NULL
