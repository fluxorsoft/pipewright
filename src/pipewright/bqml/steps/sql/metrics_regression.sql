-- Regression metrics, defined as in the container path (mlkit): rmse, mae, median absolute error, mape over the rows
-- whose value is not 0, and r2 (none when every value is the same).
WITH t AS (
  SELECT {group} AS g, CAST(`{label}` AS FLOAT64) AS y, _pw_prediction AS p,
    ABS(_pw_prediction - CAST(`{label}` AS FLOAT64)) AS ae
  FROM `{predictions}`
  WHERE `{label}` IS NOT NULL AND _pw_prediction IS NOT NULL
),
med AS (
  SELECT DISTINCT g, PERCENTILE_CONT(ae, 0.5) OVER (PARTITION BY g) AS median_absolute_error FROM t
)
SELECT t.g AS _pw_group, COUNT(*) AS row_count,
  SQRT(AVG(POW(t.p - t.y, 2))) AS rmse,
  AVG(t.ae) AS mae,
  ANY_VALUE(m.median_absolute_error) AS median_absolute_error,
  AVG(IF(t.y != 0, ABS((t.p - t.y) / t.y), NULL)) AS mape,
  IF(VAR_POP(t.y) = 0, NULL, 1 - SUM(POW(t.p - t.y, 2)) / (VAR_POP(t.y) * COUNT(*))) AS r2
FROM t
JOIN med AS m ON m.g = t.g
GROUP BY t.g
