CAST(`predicted_{label}` AS STRING) AS _pw_label,
(SELECT p.prob FROM UNNEST(`predicted_{label}_probs`) AS p WHERE CAST(p.label AS STRING) = '{positive}') AS _pw_score
