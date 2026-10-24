CAST(p.`predicted_{label}` AS STRING) AS _pw_label,
(SELECT MAX(x) FROM UNNEST(p.`{label}_probs`) AS x) AS _pw_score
