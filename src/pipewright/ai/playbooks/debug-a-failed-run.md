# Playbook: debug a failed run

Intent: "why did last night's run fail?"

## Steps

1. `status` for the use case: the latest runs and which failed. A failed run names the failed steps and the first
   error line of each (errors are value-free: a column and a rule, never a value).
2. Match the error to its cause:
   - a data contract rule (for example "48 values below 0"): the data changed; the owner of the source fixes it,
     not the use case;
   - a missing table or permission: `doctor` names who to ask;
   - a gate ("rejected by the gates"): the model was not good enough; see `model check`;
   - an image or code error: the step's code, in the team's components.
3. Propose a fix only when it is the use case's own (configuration or code), on a branch, verified with
   `validate`; otherwise write who should act and why.

## Done when

The cause is named with its evidence (the step, the error line, the check), and either a verified pull request or a
note of who should act exists. You do not re-run, retry or approve anything.
