CAST(prepared.`{time_column}` AS TIMESTAMP) >= (
  SELECT APPROX_QUANTILES(CAST(`{time_column}` AS TIMESTAMP), 1000)[OFFSET({quantile})] FROM prepared)
