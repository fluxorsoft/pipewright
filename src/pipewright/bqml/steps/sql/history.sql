-- The trend for a retrain decision: this use case's latest monitoring rows.
SELECT monitored_at, max_feature_psi, prediction_psi, performance_value, alert
FROM `{table}`
ORDER BY monitored_at DESC
LIMIT 5
