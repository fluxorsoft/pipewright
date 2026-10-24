-- The held-out actual values a forecast is measured against: one per series and time point.
LEFT JOIN (
  SELECT {ids}CAST(`{time_column}` AS TIMESTAMP) AS _pw_t, `{label}` FROM `{rows_table}` WHERE _pw_test
) AS a ON a._pw_t = f.forecast_timestamp{on}
WHERE a.`{label}` IS NOT NULL OR f._pw_status != ''
