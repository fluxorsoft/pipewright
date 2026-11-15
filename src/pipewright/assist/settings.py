"""The two switches of Pipewright Assist, and what it runs with.

Off by default. The platform allows Assist for an environment (the manifest's ai.assist.enabled); then each team opts
in for itself (assist.enabled in its use case) and names the version of the explain page it read (assist.understood).
Neither switch alone turns it on. An environment with no platform manifest (open source without a platform team) has
no platform switch: the team's own is enough.
"""

from __future__ import annotations

from dataclasses import dataclass

EXPLAIN_VERSION = "1.4.4"          # changes when what Assist reads or does changes: teams then confirm again
MODEL = "gemini-3.8-flash"
LOCATION = "global"                # the fast model runs only from the global endpoint
TEMPERATURE = 0.0                  # the same facts should give the same answer
MAX_OUTPUT_TOKENS = 1024
MAX_TOOL_CALLS = 8                 # the ceiling per question
MAX_TOKENS = 40_000
QUESTIONS_PER_MONTH = 200          # per team; enforced by Enterprise's hosted tool server, counted everywhere


@dataclass(frozen=True)
class Settings:
    platform_managed: bool         # the environment has a platform manifest, so it has a platform switch
    platform_allows: bool
    model: str
    location: str
    temperature: float
    max_output_tokens: int
    max_tool_calls: int
    max_tokens: int
    screening: bool
    screening_template: str | None
    questions_per_month: int
    model_service_account: str | None

    @property
    def platform_reason(self) -> str | None:
        """Why the platform's side is not on, or None when it is."""
        if self.platform_managed and not self.platform_allows:
            return ("the platform has not allowed Pipewright Assist in this environment (ai.assist.enabled in the "
                    "environment's manifest: `assist = true` in the starter's Terraform, spec.assist.enabled in "
                    "Pipewright Enterprise)")
        return None


def resolve(env) -> Settings:
    a = (env.ai or {}).get("assist") or {}
    s = a.get("screening") or {}
    return Settings(
        platform_managed=bool(getattr(env, "manifest_source", None)), platform_allows=bool(a.get("enabled")),
        model=a.get("model") or MODEL, location=a.get("location") or LOCATION, temperature=TEMPERATURE,
        max_output_tokens=MAX_OUTPUT_TOKENS, max_tool_calls=MAX_TOOL_CALLS, max_tokens=MAX_TOKENS,
        screening=bool(s.get("enabled") and s.get("template")), screening_template=s.get("template"),
        questions_per_month=int((a.get("budget") or {}).get("questions_per_month", QUESTIONS_PER_MONTH)),
        model_service_account=a.get("model_service_account"))


def team_reason(uc) -> str | None:
    """Why the team's side is not on for this use case, or None when it is."""
    a = getattr(uc, "assist", None) or {}
    if not a.get("enabled"):
        return (f"{uc.name} has not switched Pipewright Assist on: read `pipewright assist explain`, then set "
                f'assist: {{ enabled: true, understood: "{EXPLAIN_VERSION}" }} in its usecase.yaml')
    if a.get("understood") != EXPLAIN_VERSION:
        was = f"names version {a['understood']}" if a.get("understood") else "does not name the page it read"
        return (f"{uc.name} {was}: read `pipewright assist explain` (version {EXPLAIN_VERSION}), then set "
                f'assist.understood: "{EXPLAIN_VERSION}" in its usecase.yaml')
    return None


def why_off(env, uc=None) -> str | None:
    """Why Assist does not start, or None when both switches are on. Without a use case (a platform person's
    question about the whole environment) only the platform's side counts."""
    s = resolve(env)
    if s.platform_reason:
        return s.platform_reason
    if uc is not None:
        return team_reason(uc)
    if not s.platform_managed:
        return ("questions about the whole environment need a use case in an environment without a platform "
                "manifest: name one with --usecase")
    return None
