-- k-means and PCA: BigQuery ML's own measures. k-means on the held-out test rows; PCA from its training (its
-- explained variance does not depend on new rows).
SELECT * FROM ML.EVALUATE(MODEL `{model}`{rows})
