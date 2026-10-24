-- Rows per predicted class, for prediction drift on a multi-class model.
SELECT _pw_label AS class, COUNT(*) AS n FROM `{table}` WHERE _pw_label IS NOT NULL GROUP BY class
