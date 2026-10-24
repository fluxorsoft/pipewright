-- Training rows only: the test rows never reach CREATE MODEL. The model takes the dataset's default key;
-- a retried step replaces it (registration comes after evaluation).
CREATE OR REPLACE MODEL `{model}`
{transform}
OPTIONS ({options})
AS
SELECT {columns}
FROM `{rows_table}`
WHERE NOT _pw_test
