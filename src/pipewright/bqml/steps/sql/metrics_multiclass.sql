-- Multi-class metrics, defined as in the container path (mlkit): per class precision, recall and f1; macro averages
-- over the classes present in the rows; f1 weighted by support; log loss of the true class, clipped at 1e-7.
WITH t AS (
  SELECT {group} AS g, CAST(`{label}` AS STRING) AS y, _pw_label AS pred, _pw_true_prob AS true_prob
  FROM `{predictions}`
  WHERE `{label}` IS NOT NULL
),
classes AS (
  SELECT DISTINCT g, c FROM t, UNNEST([y, pred]) AS c WHERE c IS NOT NULL
),
per_class AS (
  SELECT k.g, k.c,
    COUNTIF(t.pred = k.c AND t.y = k.c) AS tp,
    COUNTIF(t.pred = k.c AND t.y != k.c) AS fp,
    COUNTIF(t.pred != k.c AND t.y = k.c) AS fn
  FROM classes AS k JOIN t ON t.g = k.g
  GROUP BY k.g, k.c
),
scored AS (
  SELECT g, c, tp + fn AS support,
    IF(tp + fp = 0, 0, tp / (tp + fp)) AS `precision`,
    IF(tp + fn = 0, 0, tp / (tp + fn)) AS recall,
    IF(2 * tp + fp + fn = 0, 0, 2 * tp / (2 * tp + fp + fn)) AS f1
  FROM per_class
),
overall AS (
  SELECT g, COUNT(*) AS n, COUNTIF(pred = y) / COUNT(*) AS accuracy,
    AVG(-LN(GREATEST(true_prob, 1e-7))) AS log_loss
  FROM t GROUP BY g
)
SELECT o.g AS _pw_group, o.n AS row_count, o.accuracy,
  AVG(IF(s.support > 0, s.recall, NULL)) AS balanced_accuracy,
  AVG(IF(s.support > 0, s.f1, NULL)) AS f1_macro,
  SAFE_DIVIDE(SUM(s.f1 * s.support), SUM(s.support)) AS f1_weighted,
  AVG(IF(s.support > 0, s.`precision`, NULL)) AS precision_macro,
  AVG(IF(s.support > 0, s.recall, NULL)) AS recall_macro,
  ANY_VALUE(o.log_loss) AS log_loss,
  ARRAY_AGG(STRUCT(s.c AS class, s.`precision` AS `precision`, s.recall AS recall, s.f1 AS f1,
                   s.support AS support) ORDER BY s.c) AS per_class
FROM overall AS o
JOIN scored AS s ON s.g = o.g
GROUP BY o.g, o.n, o.accuracy
