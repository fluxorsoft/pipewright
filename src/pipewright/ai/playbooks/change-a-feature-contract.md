# Playbook: a feature source changed

Intent: "training refuses: the feature definitions changed" or "use the new version of <feature table>".

## Steps

1. `features_check` the use case: which source differs (its SQL or its schema) or cannot be checked. Exit 0 means it
   matches, and the problem is elsewhere.
2. Find out why it changed. A data team's deliberate change is fine; an accident is not, and the fix is upstream,
   not in the contract. Say which in the review.
3. A deliberate change: a person runs `pipewright features contract --env <env> --usecase <name>` to record the new
   definitions (the version goes up by what changed); the contract file is part of the use case and its release.
4. A changed definition means a new model: retrain after the release, and the new version passes its gates and its
   champion comparison before anyone approves it. The served model keeps its approved definitions until then.
5. `validate`, then open a pull request with `pipewright review`, naming the source, what changed and why.

## Done when

The review names the changed source and the reason, the contract's new version is in the pull request, and a person
has it. Never edit `feature_contract` by hand.
