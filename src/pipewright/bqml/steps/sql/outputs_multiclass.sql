CAST(`predicted_{label}` AS STRING) AS _pw_label,
(SELECT MAX(p.prob) FROM UNNEST(`predicted_{label}_probs`) AS p) AS _pw_score
