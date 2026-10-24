-- Fairness rates per group of one attribute (evaluation.fairness), as the container path computes them
-- (pipewright_runtime.fairness_measures): at the decision threshold, each group's selection rate and its true and
-- false positive rates. A missing attribute value is its own group. The gaps between groups are taken in Python.
WITH t AS (
  SELECT IFNULL(CAST({attribute} AS STRING), '(missing)') AS g,
    IF(CAST(`{label}` AS STRING) = '{positive}', 1, 0) AS y,
    IF(_pw_score >= {threshold}, 1, 0) AS hat
  FROM `{predictions}`
  WHERE _pw_score IS NOT NULL AND `{label}` IS NOT NULL
)
SELECT g AS _pw_group, COUNT(*) AS row_count, AVG(hat) AS selection_rate,
  SAFE_DIVIDE(COUNTIF(y = 1 AND hat = 1), COUNTIF(y = 1)) AS tpr,
  SAFE_DIVIDE(COUNTIF(y = 0 AND hat = 1), COUNTIF(y = 0)) AS fpr
FROM t
GROUP BY g
ORDER BY g
