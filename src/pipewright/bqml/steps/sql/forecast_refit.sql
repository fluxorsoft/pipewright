-- ARIMA_PLUS after gate 1: the options evaluation measured, refit on the whole series (the held-out period too), so
-- the registered model forecasts from the latest data rather than from where the measured model stopped.
CREATE OR REPLACE MODEL `{model}`
OPTIONS ({options})
AS
SELECT {columns}
FROM `{rows_table}`
