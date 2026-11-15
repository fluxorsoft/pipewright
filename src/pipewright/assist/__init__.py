"""Pipewright Assist (preview, 1.4.4, off by default): an assistant that answers incident questions.

    pipewright assist explain --env test [--usecase churn]
    pipewright assist ask "why did last night's churn training fail?" --env test --usecase churn

What it is, in one paragraph: a person asks why a run failed; Assist reads what Pipewright already recorded (the run
and the cause the launcher kept with it, since 1.4.4), and answers with what failed, why, whose it is and what to do
next. The cause, the owner and the advice come from Pipewright's fixed rules; the model finds the run and explains.

What it never does: read data rows, step logs, monitoring results or audit logs; reach the cluster; change anything.

The pieces (the harness):
    settings   the two switches and what Assist runs with
    explain    the page a team reads before it opts in
    fixes      the known fixes, by rule: no model writes advice
    replies    the fixed replies to questions outside incidents
    tools      the four tools, behind a guard (team, ceiling, screening)
    screen     Model Armor screening
    engine     one question from start to finish; composes the answer from recorded facts
    brain      the model: Google's ADK (the `assist` extra)
    record     the record of answers and the count per team
    replay     the test runner: frozen failures, scored against the pass mark
"""
