-- Binary classification metrics, defined as in the container path (mlkit): AUC with tied scores ranked on average,
-- average precision (ties broken with negatives first), metrics at the decision threshold, log loss with scores
-- clipped to [1e-7, 1 - 1e-7], Brier score, and expected calibration error over 10 equal bins.
WITH t AS (
  SELECT {group} AS g, IF(CAST(`{label}` AS STRING) = '{positive}', 1, 0) AS y, _pw_score AS p
  FROM `{predictions}`
  WHERE _pw_score IS NOT NULL AND `{label}` IS NOT NULL
),
ranked AS (
  SELECT g, y, p,
    RANK() OVER (PARTITION BY g ORDER BY p) + (COUNT(*) OVER (PARTITION BY g, p) - 1) / 2 AS avg_rank,
    ROW_NUMBER() OVER (PARTITION BY g ORDER BY p DESC, y) AS k,
    SUM(y) OVER (PARTITION BY g ORDER BY p DESC, y ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS tp_at_k
  FROM t
),
ranks AS (
  SELECT g, SUM(IF(y = 1, avg_rank, 0)) AS rank_sum, SUM(IF(y = 1, tp_at_k / k, 0)) AS precision_sum
  FROM ranked GROUP BY g
),
bins AS (
  SELECT g, LEAST(CAST(FLOOR(p * 10) AS INT64), 9) AS b, COUNT(*) AS n, AVG(p) AS mean_p, AVG(y) AS mean_y
  FROM t GROUP BY g, b
),
ece AS (
  SELECT g, SUM(n * ABS(mean_p - mean_y)) / SUM(n) AS calibration_error FROM bins GROUP BY g
),
counts AS (
  SELECT g, COUNT(*) AS n, SUM(y) AS pos,
    COUNTIF(p >= {threshold} AND y = 1) AS tp, COUNTIF(p >= {threshold} AND y = 0) AS fp,
    COUNTIF(p < {threshold} AND y = 1) AS fn, COUNTIF(p < {threshold} AND y = 0) AS tn,
    AVG(-(y * LN(LEAST(GREATEST(p, 1e-7), 1 - 1e-7))
          + (1 - y) * LN(1 - LEAST(GREATEST(p, 1e-7), 1 - 1e-7)))) AS log_loss,
    AVG(POW(p - y, 2)) AS brier
  FROM t GROUP BY g
)
SELECT c.g AS _pw_group, c.n AS row_count,
  IF(c.pos = 0 OR c.pos = c.n, NULL, (r.rank_sum - c.pos * (c.pos + 1) / 2) / (c.pos * (c.n - c.pos))) AS auc,
  IF(c.pos = 0, NULL, r.precision_sum / c.pos) AS pr_auc,
  (c.tp + c.tn) / c.n AS accuracy,
  IF(c.tp + c.fp = 0, 0, c.tp / (c.tp + c.fp)) AS `precision`,
  IF(c.tp + c.fn = 0, 0, c.tp / (c.tp + c.fn)) AS recall,
  IF(2 * c.tp + c.fp + c.fn = 0, 0, 2 * c.tp / (2 * c.tp + c.fp + c.fn)) AS f1,
  c.log_loss, c.brier, e.calibration_error, c.tp, c.fp, c.fn, c.tn
FROM counts AS c
JOIN ranks AS r ON r.g = c.g
JOIN ece AS e ON e.g = c.g
