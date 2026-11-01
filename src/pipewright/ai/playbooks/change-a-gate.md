# Playbook: change a gate

Intent: "raise the AUC gate to 0.75" or "add a fairness gate".

## Steps

1. `resolve` the use case and `model list`: today's gates and how the current versions score against them.
2. Edit `config/evaluation.yaml`. Gates may only be loosened with a reason a person accepts in the review; the
   platform's minimums (the context pack's rules) can never be undercut.
3. `model check --version <champion>`: would the champion still pass? Put the answer in the review.
4. `validate`, then a pull request with `pipewright review`.

## Done when

The review states the old and new gate, whether the champion passes the new one, and the reason. A person decides.
