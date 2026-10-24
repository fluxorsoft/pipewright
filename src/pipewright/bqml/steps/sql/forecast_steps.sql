-- A forecast's held-out period: the most time points one series has in it, the horizon evaluation forecasts.
SELECT IFNULL(MAX(n), 0) AS steps
FROM (SELECT COUNT(DISTINCT `{time_column}`) AS n FROM `{rows_table}` WHERE _pw_test{group_by})
