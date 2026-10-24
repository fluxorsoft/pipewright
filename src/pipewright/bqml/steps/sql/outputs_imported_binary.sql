(SELECT x FROM UNNEST(p.`{label}_probs`) AS x WITH OFFSET AS a JOIN UNNEST(p.`{label}_values`) AS v WITH OFFSET AS b ON a = b
 WHERE CAST(v AS STRING) = '{positive}') AS _pw_score,
IF((SELECT x FROM UNNEST(p.`{label}_probs`) AS x WITH OFFSET AS a JOIN UNNEST(p.`{label}_values`) AS v WITH OFFSET AS b ON a = b
    WHERE CAST(v AS STRING) = '{positive}') >= {threshold}, '{positive}',
   (SELECT CAST(v AS STRING) FROM UNNEST(p.`{label}_values`) AS v WHERE CAST(v AS STRING) != '{positive}' LIMIT 1)) AS _pw_label
