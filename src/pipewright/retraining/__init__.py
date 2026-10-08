"""Retrain decisions: drift or a performance drop opens a case, and a person decides (CONTEXT.md, retrain decisions).

    inference run: drift or performance fired
      -> proposal (an observation) to the launcher, over the run requests topic or POST /retraining/propose
      -> the launcher opens a case: gs://<artifacts>/cases/<usecase>/<case_id>.json, emits retrain.proposed
         (emailed to retraining.decisions.notify; on the tenant's decision feed when decisions.publish is true)
      -> approvers decide with `pipewright retrain approve | decline | snooze`: a message to their approver group's
         topic, so IAM on the topic decides who belongs, and a Google-signed ID token says who they are
      -> quorum reached (one approval per required group): the launcher submits training, run key = case ID
      -> /retraining/tick (Cloud Scheduler): snoozes and declines expire, undecided cases escalate, weekly digest

Modes: off (alerts only), propose (nothing trains until approved), candidate (trains a candidate at once and never
promotes; a person still decides). The launcher is the only writer of cases.

    cases.py     the case and its transitions (pure)
    summary.py   the email-sized decision summary
    store.py     cases in the artifacts bucket
    identity.py  verifying approvers' ID tokens
    service.py   what the launcher does with proposals, decisions and ticks
    client.py    what `pipewright retrain` does
"""
