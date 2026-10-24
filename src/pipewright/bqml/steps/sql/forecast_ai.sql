-- AI.FORECAST (bqml.forecast.engine ai_forecast, only where policies.bqml.ai_forecast allows it): Google's TimesFM
-- model forecasts from the history given, with the settings the version was approved with. Not a team's SQL (C-08).
SELECT {ids}forecast_timestamp, forecast_value, prediction_interval_lower_bound, prediction_interval_upper_bound,
  ai_forecast_status AS _pw_status
FROM AI.FORECAST(
  ({history}),
  data_col => '{label}', timestamp_col => '{time_column}', model => '{model}',{id_cols}
  horizon => {horizon}, confidence_level => {confidence_level}{context_window})
