(SELECT x FROM UNNEST(p.`{label}_probs`) AS x WITH OFFSET AS a JOIN UNNEST(p.`{label}_values`) AS v WITH OFFSET AS b ON a = b
 WHERE CAST(v AS STRING) = CAST(p._pw_raw.`{label}` AS STRING)) AS _pw_true_prob
