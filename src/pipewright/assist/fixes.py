"""The known fixes: what to do about a failed run, by the rule that named its cause.

A fixed list, written here and tested. No model writes advice: in the design's spike the model suggested the wrong
BigQuery role three times out of three, specifically and confidently. A fix names a setting that exists in
Pipewright or says who changes what; it never names a role to grant.
"""

from __future__ import annotations

TEAM, PLATFORM, UNCLEAR = "team", "platform", "unclear"
OWNER_WORDS = {TEAM: "your team", PLATFORM: "the platform team", UNCLEAR: "unclear"}

# rule -> who acts, what they do, and where (a file or a command), {step} filled in when known
FIXES = {
    "checks": (TEAM, "The data broke the contract your use case declares. Correct the data at its source, or, if the "
                     "contract is what is wrong, change data.contract and release again.", "config/data.yaml"),
    "use_case_settings": (TEAM, "Pipewright refused the step because of the use case's own settings or data. The "
                                "cause's message says what to change; change it and release again.",
                          "the setting or table the message names"),
    "platform_settings": (PLATFORM, "Pipewright refused the step because the environment is missing something the "
                                    "message names. The platform team provides it through the environment's "
                                    "Terraform; nothing to change in the use case.",
                          "the platform's Terraform for the environment"),
    "team_code": (TEAM, "Fix the line shown and release the use case again. Nothing is wrong with the platform.",
                  "the file and line in the cause"),
    "out_of_memory": (TEAM, "Give the step more memory and release again.",
                      "steps.{step}.memory in config/runtime.yaml"),
    "machine_shape": (TEAM, "Ask for a machine the region offers: change the step's cpu, memory or gpu and release "
                            "again.", "steps.{step} in config/runtime.yaml"),
    "bad_request": (TEAM, "The orchestrator refused the run as asked. Check the use case's pipeline and step "
                          "settings, then release again.", "config/runtime.yaml"),
    "permission": (PLATFORM, "The run's identity was refused the permission named in the cause. The platform team "
                             "grants it through the environment's Terraform, or the use case stops asking for it. "
                             "Do not grant roles by hand.", "`pipewright doctor` shows what the runner may do"),
    "refused_submission": (PLATFORM, "The orchestrator refused the launcher, or the identity the run would use, "
                                     "before the run started. The platform team restores that permission through "
                                     "the environment's Terraform. Nothing to change in the use case; ask for the "
                                     "run again afterwards.", "the platform's Terraform for the environment"),
    "perimeter": (PLATFORM, "A service perimeter refused the call. The platform team reviews the perimeter's rules "
                            "for the resource named in the cause; a use case reads only from inside the perimeter "
                            "(its landing prefix and granted datasets).", "the platform's perimeter settings"),
    "quota": (PLATFORM, "A Google Cloud quota was reached. The platform team raises the quota named in the cause, "
                        "or frees capacity. Nothing to change in the use case; run it again afterwards.",
              "the project's quotas"),
    "missing_image": (PLATFORM, "The step's image is missing from the registry. Release the use case again, which "
                                "rebuilds it; if it is still missing the platform team restores the registry or its "
                                "mirror.", "`pipewright release`"),
    "cancelled": (UNCLEAR, "Someone cancelled the run. Start it again if it is still needed.", "`pipewright run`"),
    "unclear": (UNCLEAR, "The recorded facts do not name the cause. Read the failed step's logs in the Google Cloud "
                         "console.", "the step's logs"),
}
NO_RECORD = (UNCLEAR, "The cause of this run was not recorded: failure records were off for the release that ran, "
                      "or the launcher no longer has its record. Read the failed step's logs in the Google Cloud "
                      "console. With monitoring.failure_records on (then a release), the next failure says why.",
             "config/operations.yaml")
NOT_YET = (UNCLEAR, "The cause of this run is not recorded yet: the launcher records it on its hourly check of "
                    "failed runs. Ask again in a while. Until then the failed step's logs in the Google Cloud "
                    "console have it.", "ask again after the launcher's next check")
RULES = tuple(FIXES)


def fix(rule: str | None, step: str | None = None, recent: bool = False) -> dict:
    """The fix for a rule: {who, what, where}. An unknown rule gets the unclear fix; never a guess. Without a rule
    (no cause recorded): `recent` says the run ended within the launcher's check interval, so the cause may still
    come."""
    who, what, where = (FIXES.get(rule or "", FIXES["unclear"]) if rule else NOT_YET if recent else NO_RECORD)
    return {"who": who, "what": what, "where": where.replace("{step}", step or "<step>")}
