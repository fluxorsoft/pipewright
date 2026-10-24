-- ARIMA_PLUS: the forecast from the end of the model's training data, with its prediction interval.
SELECT {ids}forecast_timestamp, forecast_value, prediction_interval_lower_bound, prediction_interval_upper_bound,
  '' AS _pw_status
FROM ML.FORECAST(MODEL `{model}`, STRUCT({horizon} AS horizon, {confidence_level} AS confidence_level))
