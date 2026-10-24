(SELECT p.prob FROM UNNEST(`predicted_{label}_probs`) AS p
 WHERE CAST(p.label AS STRING) = CAST(`{label}` AS STRING)) AS _pw_true_prob
