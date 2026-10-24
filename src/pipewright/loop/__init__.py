"""The closed retrain loop (FR-48, D-104 to D-109): a case approved in production is trained in the environment
production's releases come from (its promotion.from, "test"), compared with the model production serves, validated
by a scoring run in test, promoted, approved and verified in production. Production never trains.

    approved -> training -> candidate -> validating -> validated -> [approve] -> verifying -> closed
                    `-> rejected / failed     `-> validation-failed                  `-> verification-failed
    closed or verification-failed -> [rollback] -> rolled-back

The loop's record lives on the production case (case.loop), with one entry per step. `pipewright loop advance`
takes one step when the current one is done; every step is safe to repeat (run keys from the case ID). Production's
private launcher is never asked to reach test: the CLI, run by a person or by CI (FR-56), drives the loop.

driver.py   the state machine, over a Backend (jobs, registry, monitoring table, promotion, approval, case store)
backend.py  the Google Cloud implementation of the Backend
"""

TERMINAL = ("rejected", "failed", "validation-failed", "closed", "verification-failed", "rolled-back")
WAITING_FOR_APPROVAL = ("validated",)
