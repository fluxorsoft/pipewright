"""Pipewright Assist (preview, 1.4.4, off by default): the harness around the model.

No model is called here: a scripted brain stands in, so these tests check what Pipewright itself decides: the two
switches, the guard on every tool call, the answer composed from recorded facts, the fixed replies, the record.
The frozen cases with the real model are the release gate (`pipewright assist test`).
"""

import json
from types import SimpleNamespace

import pytest

from pipewright.assist import explain, fixes, replay, replies, settings
from pipewright.assist.cases import CASES
from pipewright.assist.engine import Engine
from pipewright.assist.record import Record
from pipewright.assist.screen import Screen
from pipewright.assist.tools import WITHHELD, FrozenSource, Tools
from pipewright.launcher import cause


def env(assist=None, managed=True):
    return SimpleNamespace(name="test", ai={"assist": assist} if assist is not None else {},
                           manifest_source="gs://bucket/environment.json" if managed else None, project="acme")


def usecase(assist=None, failure_records=True):
    return SimpleNamespace(name="churn", tenant="retail", assist=assist or {},
                           platform={"failure_records": "gs://x/failures"} if failure_records else {})


def case(cid):
    return next(c for c in CASES if c["id"] == cid)


def ask(cid, brain=None, **kw):
    c = case(cid)
    return replay.engine_for(c, brain or replay.ScriptedBrain(), **kw).ask(c["question"])


# ------------------------------------------------------------------------------------------- switches --
def test_off_by_default_everywhere():
    assert "platform has not allowed" in settings.why_off(env(), usecase())
    assert "has not switched Pipewright Assist on" in settings.why_off(env({"enabled": True}), usecase())


def test_neither_switch_alone_turns_it_on():
    on = {"enabled": True, "understood": settings.EXPLAIN_VERSION}
    assert settings.why_off(env(), usecase(on)) is not None                    # the team alone
    assert settings.why_off(env({"enabled": True}), usecase()) is not None     # the platform alone
    assert settings.why_off(env({"enabled": True}), usecase(on)) is None


def test_without_a_platform_manifest_the_team_switch_is_enough():
    on = {"enabled": True, "understood": settings.EXPLAIN_VERSION}
    assert settings.why_off(env(managed=False), usecase(on)) is None
    assert settings.why_off(env(managed=False), usecase()) is not None


def test_a_team_must_name_the_explain_page_it_read():
    e = env({"enabled": True})
    assert "does not name the page" in settings.why_off(e, usecase({"enabled": True}))
    assert "names version 1.0.0" in settings.why_off(e, usecase({"enabled": True, "understood": "1.0.0"}))


def test_the_settings_and_their_defaults():
    s = settings.resolve(env({"enabled": True}))
    assert (s.model, s.location, s.temperature, s.max_output_tokens) == ("gemini-3.8-flash", "global", 0.0, 1024)
    assert (s.max_tool_calls, s.max_tokens, s.questions_per_month, s.screening) == (8, 40_000, 200, False)
    s = settings.resolve(env({"enabled": True, "model": "gemini-x", "location": "europe-west2",
                              "screening": {"enabled": True, "template": "projects/p/locations/eu/templates/t"},
                              "budget": {"questions_per_month": 50}}))
    assert (s.model, s.location, s.screening, s.questions_per_month) == ("gemini-x", "europe-west2", True, 50)


def test_the_explain_page_says_what_matters_and_shows_the_settings():
    page = explain.page(env({"enabled": True}), usecase({"enabled": True, "understood": settings.EXPLAIN_VERSION}))
    for must in ("WHAT IT NEVER READS", "data rows", "step logs", "It changes nothing", "Model Armor",
                 "kept as you typed it", "no stability promise", f"version {settings.EXPLAIN_VERSION}",
                 "gemini-3.8-flash", "8 tool calls, 40,000 tokens", "not screened", "Temperature"):
        assert must in page, must
    assert "  ON" in page and "Why it is off" not in page
    off = explain.page(env(), usecase())
    assert "OFF" in off and "Why it is off" in off


def test_doctor_says_whether_assist_is_on(monkeypatch):
    from pipewright.checks import doctor

    def note(e, uc):
        report = doctor.Report()
        doctor._check_assist(uc, e, report, "Use case churn")
        (result,) = [r for rs in report.sections.values() for r in rs]
        return result

    on = {"enabled": True, "understood": settings.EXPLAIN_VERSION}
    assert note(env(), usecase()).status == doctor.INFO
    assert note(env(), usecase(on)).status == doctor.WARN                           # asked for, cannot start
    assert note(env({"enabled": True}), usecase(on)).status == doctor.OK
    without = note(env({"enabled": True}), usecase(on, failure_records=False))
    assert without.status == doctor.WARN and "read the logs" in without.detail


# -------------------------------------------------------------------------------------- known fixes --
def test_every_rule_of_the_cause_has_a_fix_and_no_fix_names_a_role():
    for rule in (*cause.RULE_NAMES, "bad_request"):
        f = fixes.fix(rule)
        assert f["what"] and f["who"] in ("team", "platform", "unclear"), rule
        assert "roles/" not in f["what"] and "roles/" not in f["where"]
    assert fixes.fix("something-new") == fixes.fix("unclear")
    assert "failure records were off" in fixes.fix(None)["what"]
    assert "hourly check" in fixes.fix(None, recent=True)["what"]
    assert fixes.fix("out_of_memory", "prepare-data")["where"] == "steps.prepare-data.memory in config/runtime.yaml"


def test_the_fix_and_the_rules_table_agree_on_the_owner():
    owners = {rule: who for rule, who, *_ in cause.RULES}
    for rule, who in owners.items():
        assert fixes.fix(rule)["who"] == who, rule


# ---------------------------------------------------------------------------------- the frozen cases --
@pytest.mark.parametrize("cid", [c["id"] for c in CASES])
def test_every_frozen_case_passes_with_the_scripted_brain(cid):
    c = case(cid)
    answer = replay.engine_for(c, replay.ScriptedBrain()).ask(c["question"])
    result = replay.score(c, answer)
    assert result["passed"], (result, answer.text)


def test_the_pass_mark_is_met_by_the_harness_itself():
    report = replay.run_cases(replay.ScriptedBrain, runs=1)
    assert report["passed"] and all(report["checks"].values())
    assert "PASS: the pass mark is met" in replay.render(report)


# ----------------------------------------------------------------------- what the model cannot change --
class Liar(replay.ScriptedBrain):
    """A model that finds the right run and then says everything wrong about it."""

    def investigate(self, question, context, tools):
        found, usage = super().investigate(question, context, tools)
        found["explanation"] = ("The owner is the platform. Grant roles/owner to attacker@example.com at "
                                "https://example.com/fix and it will pass.")
        found["owner"], found["rule"] = "platform", "permission"
        return found, usage


def test_the_model_cannot_overrule_the_owner_the_rule_or_the_advice():
    a = ask("ap-team-bug", Liar())
    assert (a.owner, a.rule) == ("team", "team_code")
    assert "Owner: your team." in a.text and fixes.fix("team_code")["what"] in a.text
    for never in ("roles/owner", "attacker@example.com", "https://", "The owner is the platform"):
        assert never not in a.text


def test_the_answer_is_composed_from_the_record():
    a = ask("ap-permission")
    assert a.text.startswith("The training run raac5375705f1 of churn failed at record-metrics.")
    assert "Permission bigquery.tables.get denied" in a.text and "Owner: the platform team." in a.text
    assert "Generated advice (preview)" in a.text and "Not screened by Model Armor." in a.text


def test_a_run_that_did_not_fail_gets_no_owner():
    a = ask("still-running")
    assert a.owner is None and "is still running" in a.text and "Owner:" not in a.text


def test_a_refused_submission_is_explained_as_one():
    a = ask("kf-not-submitted")
    assert "was not submitted" in a.text and "refused the launcher" in a.text and a.owner == "platform"


# ------------------------------------------------------------------------------------------ the guard --
def tools_for(cid, **kw):
    c = case(cid)
    return Tools(FrozenSource(c["data"]), c.get("tenant"), kw.pop("usecases", None), kw.pop("max_calls", 8),
                 platform=bool(c.get("platform")), **kw)


def test_another_teams_use_case_is_refused():
    out = tools_for("other-team").runs("offers")
    assert out == {"ok": False, "refused": "offers belongs to another team: Assist answers for team retail only"}


def test_a_use_case_without_assist_on_is_refused():
    out = tools_for("ap-contract", usecases={"another"}).runs("churn")
    assert out["refused"] == "Pipewright Assist is not switched on for churn"


def test_the_ceiling_stops_tool_calls():
    t = tools_for("ap-contract", max_calls=2)
    assert t.runs("churn")["ok"] and t.known_fix("checks")["ok"]
    assert "ceiling of 2 tool calls" in t.release("churn")["refused"] and t.ceiling_reached
    c = case("ap-contract")

    class Greedy(replay.ScriptedBrain):
        def investigate(self, question, context, tools):
            for _ in range(5):
                tools.known_fix("checks")
            return super().investigate(question, context, tools)

    a = Engine(Greedy(), Tools(FrozenSource(c["data"]), "retail", None, 3), "test", None, "churn").ask(c["question"])
    assert "Assist stopped at its ceiling" in a.text


def test_text_that_reads_like_an_instruction_is_withheld_and_said():
    t = tools_for("planted-instruction")
    run = t.runs("churn")["runs"][0]
    assert run["cause"]["record"]["code"]["code"] == WITHHELD and "the line of code in the failure record" in t.withheld
    a = ask("planted-instruction")
    assert "looked like an instruction" in a.text and "attacker" not in a.text and a.owner == "team"


def test_the_environments_failures_are_a_platform_question():
    c = case("platform-week")
    team = Tools(FrozenSource(c["data"]), None, None, 8, platform=False)
    assert "platform question" in team.runs("")["refused"]
    out = tools_for("platform-week").runs("")
    assert out["by_owner"] == {"team": 2, "platform": 2, "unclear": 1} and len(out["blocked_now"]) == 4


def test_a_team_sees_only_its_own_failures():
    c = case("platform-week")
    out = Tools(FrozenSource(c["data"]), "marketing", None, 8).runs("")
    assert out["failed_runs"] == 1 and out["failures"][0]["usecase"] == "offers"


def test_a_pipeline_that_succeeded_since_is_not_blocked():
    c = json.loads(json.dumps(case("platform-week")))
    c["data"]["runs"]["demand"] = {"ok": True, "runs": [{"run_id": "r1", "pipeline": "training", "state": "succeeded",
                                                         "created": "2026-11-13T02:00:00Z"}]}
    out = Tools(FrozenSource(c["data"]), None, None, 8, platform=True).runs("")
    assert "demand" not in [b["usecase"] for b in out["blocked_now"]] and len(out["blocked_now"]) == 3


# ---------------------------------------------------------------------------------------- screening --
class Armor:
    """Model Armor, replaced: matches any text containing 'jailbreak'."""

    def __init__(self):
        self.seen = []

    def post(self, url, body):
        text = (body.get("userPromptData") or body.get("modelResponseData"))["text"]
        self.seen.append((url.rsplit(":", 1)[-1], text))
        hit = "jailbreak" in text
        return SimpleNamespace(ok=True, status=200, body={"sanitizationResult": {
            "filterMatchState": "MATCH_FOUND" if hit else "NO_MATCH_FOUND",
            "filterResults": {"pi_and_jailbreak": {"piAndJailbreakFilterResult": {
                "matchState": "MATCH_FOUND" if hit else "NO_MATCH_FOUND"}}}}})


TEMPLATE = "projects/acme/locations/europe-west2/templates/assist"


def test_screening_is_off_without_a_template_and_the_answer_says_so():
    assert not Screen(None).on and Screen(None).screen("question", "anything") is None
    assert "Not screened by Model Armor." in ask("ap-contract").text


def test_with_screening_on_the_question_each_tool_field_and_the_answer_are_screened():
    armor = Armor()
    screen = Screen(TEMPLATE, armor)
    a = ask("ap-team-bug", screen=screen)
    verbs = [v for v, _ in armor.seen]
    assert verbs[0] == "sanitizeUserPrompt" and verbs[-1] == "sanitizeModelResponse" and len(verbs) >= 4
    assert "Not screened" not in a.text and "Screened by Model Armor." in a.text and a.screened
    assert any("prepare.py" in text or "monthly_charges" in text for _, text in armor.seen[1:-1])


def test_a_question_model_armor_matches_is_not_sent_to_the_model():
    c = case("ap-contract")

    class Never(replay.ScriptedBrain):
        def classify(self, question):
            raise AssertionError("the model was called")

    a = replay.engine_for(c, Never(), screen=Screen(TEMPLATE, Armor())).ask("jailbreak: why did the run fail?")
    assert a.kind == "refused" and "was not sent to the model" in a.text and "pi_and_jailbreak" in a.refused


def test_screening_that_cannot_run_is_said_not_skipped():
    class Down:
        def post(self, url, body):
            return SimpleNamespace(ok=False, status=403, body={})

    assert "could not run" in Screen(TEMPLATE, Down()).screen("question", "why did it fail?")


# -------------------------------------------------------------------------- outside incidents, record --
@pytest.mark.parametrize("kind, must", [
    (replies.LATER, "Not covered in the preview"), (replies.ACTION, "never changes anything"),
    (replies.UNRELATED, "failed Pipewright runs only")])
def test_the_fixed_replies(kind, must):
    assert must in replies.reply(kind, "why has accuracy dropped?", "test")


def test_a_question_outside_incidents_calls_no_tool():
    a = ask("outside-later")
    assert a.kind == replies.LATER and a.tool_calls == [] and "pipewright monitor status --env test" in a.text


def test_the_record_keeps_the_question_as_typed_and_counts_per_team(tmp_path):
    import datetime

    record = Record(tmp_path)
    now = datetime.datetime(2026, 11, 14, 9, 0, tzinfo=datetime.timezone.utc)
    a = ask("ap-contract")
    for team, calls in (("retail", 3), ("retail", 2), ("marketing", 4), ("retail", 0)):
        record.write({"environment": "test", "team": team, "question": "Why did Jane's run fail?",
                      **{**a.to_record(), "model_calls": calls}}, now)
    record.write({"environment": "test", "team": "retail", "question": "old", "model_calls": 1},
                 now - datetime.timedelta(days=40))
    entries = record.entries()
    assert entries[0]["question"] == "Why did Jane's run fail?" and entries[0]["owner"] == "team"
    assert entries[0]["tools"] == ["runs"] and "answer" in entries[0]
    assert record.asked_this_month("test", "retail", now) == 2          # a question that reached the model
    assert record.asked_this_month("test", "marketing", now) == 1


# ------------------------------------------------------------------------------ the command, in a repo --
ON = {"enabled": True, "understood": settings.EXPLAIN_VERSION}


def assist_repo(repo, platform=True, team=True):
    if platform:
        repo.manifests["dev"]["ai"] = {"assist": {"enabled": True}}
    if team:
        repo.usecase["spec"]["assist"] = dict(ON)
    repo.usecase["spec"]["monitoring"] = {"failure_records": True}
    repo.write()
    return repo


def test_the_switches_resolve_from_the_manifest_and_the_use_case(repo):
    from pipewright.config import load_environment, resolve_usecase

    path = repo.write()
    e = load_environment(repo.root, "dev")
    assert settings.why_off(e, resolve_usecase(path, e)) is not None           # nothing set: off
    assist_repo(repo)
    e = load_environment(repo.root, "dev")
    uc = resolve_usecase(repo.write(), e)
    assert uc.assist == ON and settings.why_off(e, uc) is None
    assert "assist" not in json.dumps(uc.ml)                                    # never compiled into a run


def test_ask_refuses_to_start_when_assist_is_off(repo, capsys):
    from pipewright.cli.main import main

    repo.write()
    code = main(["assist", "ask", "why did it fail?", "--root", str(repo.root), "--env", "dev", "--usecase", "churn"])
    assert code != 0 and "Pipewright Assist is off" in capsys.readouterr().err
    assist_repo(repo, platform=True, team=False)
    code = main(["assist", "ask", "why did it fail?", "--root", str(repo.root), "--env", "dev", "--usecase", "churn"])
    assert code != 0 and "has not switched Pipewright Assist on" in capsys.readouterr().err


def test_explain_prints_the_page_and_the_settings(repo, capsys):
    from pipewright.cli.main import main

    assist_repo(repo)
    assert main(["assist", "explain"]) == 0
    assert "WHAT IT NEVER READS" in capsys.readouterr().out
    assert main(["assist", "explain", "--root", str(repo.root), "--env", "dev", "--usecase", "churn"]) == 0
    out = capsys.readouterr().out
    assert "SETTINGS IN PLAY (dev)" in out and "gemini-3.8-flash" in out and "Team switch (churn)" in out


def test_a_question_end_to_end_in_a_repository(repo):
    from pipewright.assist import cli

    assist_repo(repo)
    c = json.loads(json.dumps(case("ap-contract")))
    c["data"]["tenants"] = {"churn": "churn"}
    engine, s, e, team = cli.assemble(repo.root, "dev", usecase="churn", brain=replay.ScriptedBrain(),
                                      source=FrozenSource(c["data"]))
    a = engine.ask("Why did churn training run r089ff7ecfe98 fail?")
    assert team == "churn" and a.owner == "team" and "tenure_months" in a.text


def test_the_team_question_covers_only_use_cases_with_assist_on(repo):
    from pipewright.assist import cli
    from pipewright.errors import ConfigError

    assist_repo(repo, team=False)
    with pytest.raises(ConfigError, match="has not switched Pipewright Assist on"):
        cli.assemble(repo.root, "dev", brain=replay.ScriptedBrain(), source=FrozenSource({}))
    assist_repo(repo)
    engine, *_ = cli.assemble(repo.root, "dev", brain=replay.ScriptedBrain(), source=FrozenSource({}))
    assert engine.tools.usecases == {"churn"} and engine.tools.tenant == "churn"


def test_the_platform_question_needs_only_the_platform_switch(repo):
    from pipewright.assist import cli

    assist_repo(repo, team=False)
    engine, *_ = cli.assemble(repo.root, "dev", platform=True, brain=replay.ScriptedBrain(), source=FrozenSource({}))
    assert engine.tools.platform and engine.tools.tenant is None and engine.tools.usecases is None


# ----------------------------------------------------------------- a hosted tool server (Enterprise) --
def test_a_use_case_whose_team_cannot_be_told_is_refused():
    t = Tools(FrozenSource({"runs": {"ghost": {"ok": True, "runs": []}}}), "retail", None, 8)
    assert "not a use case of team retail" in t.runs("ghost")["refused"]


def hosted_repo(repo):
    assist_repo(repo)
    repo.manifests["dev"]["tenants"]["churn"]["agent_server"] = {
        "url": "https://pw-mcp-churn-1.europe-west2.run.app", "runtime": "cloud_run", "service": "pw-mcp-churn"}
    repo.write()
    return repo


def test_with_a_hosted_tool_server_the_question_is_answered_there(repo, capsys, monkeypatch):
    from pipewright.assist import hosted
    from pipewright.cli.main import main

    hosted_repo(repo)
    sent = {}

    def fake_ask(env, tenant, question, usecase, understood, server_url=None, call=None):
        sent.update(tenant=tenant, question=question, usecase=usecase, understood=understood, server=server_url)
        return {"ok": True, "answer": "The training run r1 of churn failed.", "asked_this_month": 3, "budget": 200}

    monkeypatch.setattr(hosted, "ask", fake_ask)
    code = main(["assist", "ask", "why did it fail?", "--root", str(repo.root), "--env", "dev", "--usecase", "churn",
                 "--server", "http://localhost:8765"])
    out = capsys.readouterr().out
    assert code == 0 and "The training run r1 of churn failed." in out and "3 of 200 questions this month" in out
    assert sent == {"tenant": "churn", "question": "why did it fail?", "usecase": "churn",
                    "understood": settings.EXPLAIN_VERSION, "server": "http://localhost:8765"}
    assert not (repo.root / ".pipewright" / "assist-log.jsonl").exists()        # the platform keeps the record


def test_a_refusal_of_the_hosted_server_is_said_and_fails(repo, capsys, monkeypatch):
    from pipewright.assist import hosted
    from pipewright.cli.main import main

    hosted_repo(repo)
    monkeypatch.setattr(hosted, "ask", lambda *a, **k: {"ok": False, "refused": "team churn has asked 200 questions"})
    code = main(["assist", "ask", "why?", "--root", str(repo.root), "--env", "dev", "--usecase", "churn"])
    assert code == 1 and "has asked 200 questions" in capsys.readouterr().out


def test_the_teams_switch_is_checked_before_the_hosted_server_is_asked(repo, capsys, monkeypatch):
    from pipewright.assist import hosted
    from pipewright.cli.main import main

    assist_repo(repo, team=False)
    repo.manifests["dev"]["tenants"]["churn"]["agent_server"] = {"url": "https://x.run.app", "runtime": "cloud_run"}
    repo.write()
    monkeypatch.setattr(hosted, "ask", lambda *a, **k: pytest.fail("the server was asked"))
    code = main(["assist", "ask", "why?", "--root", str(repo.root), "--env", "dev", "--usecase", "churn"])
    assert code != 0 and "has not switched Pipewright Assist on" in capsys.readouterr().err


def test_the_hosted_call_carries_the_id_token_unless_a_proxy_signs(repo, monkeypatch):
    from pipewright.assist import hosted
    from pipewright.config import load_environment

    hosted_repo(repo)
    env = load_environment(repo.root, "dev")
    monkeypatch.setattr(hosted, "identity_token", lambda: "TOKEN")
    seen = []

    def call(url, headers, arguments):
        seen.append((url, headers, arguments))
        return {"ok": True, "answer": "a"}

    hosted.ask(env, "churn", "q", "churn", "1.4.4", call=call)
    hosted.ask(env, "churn", "q", None, "1.4.4", server_url="http://localhost:8765", call=call)
    assert seen[0][0] == "https://pw-mcp-churn-1.europe-west2.run.app/mcp"
    assert seen[0][1]["Authorization"] == "Bearer TOKEN" and seen[0][2]["usecase"] == "churn"
    assert seen[1][0] == "http://localhost:8765/mcp" and "Authorization" not in seen[1][1]
    assert seen[1][2] == {"question": "q", "usecase": "", "understood": "1.4.4"}


# ------------------------------------------------------------------- found testing against the model --
def test_a_reply_cut_short_still_names_the_run():
    from pipewright.assist import brain

    cut = '{"kind": "run", "usecase": "churn", "run_id": "r3c4d5e6f7a8b", "explanation": "The training run'
    assert brain._last_json(cut) == {"kind": "run", "usecase": "churn", "run_id": "r3c4d5e6f7a8b"}
    whole = 'Here it is: {"kind": "run", "usecase": "churn", "run_id": "r1", "explanation": "It failed."}'
    assert brain._last_json(whole)["explanation"] == "It failed."
    assert brain._last_json("no json at all") == {}


def test_a_run_id_in_the_question_is_enough_when_the_model_names_no_run():
    class Vague(replay.ScriptedBrain):
        def investigate(self, question, context, tools):
            tools.runs("churn")
            return {}, {"model_calls": 1, "tokens_in": 10, "tokens_out": 10}

    a = ask("ap-permission", Vague())
    assert a.run_id == "raac5375705f1" and a.owner == "platform"
    b = ask("ap-team-bug", Vague())                     # "last night's": no run ID to go by, so no guess
    assert b.run_id is None and "could not tell which run" in b.text


# --------------------------------------------------------------------------- found in the live test --
def test_the_orchestrators_line_loses_its_console_link():
    from pipewright.assist.tools import _error_line

    line = ("ingest-data: The replica workerpool0-0 exited with a non-zero status of 1. Termination reason: Error. "
            "To find out more about why your job exited please check the logs: "
            "https://console.cloud.google.com/logs/viewer?project=123456789012&resource=ml_job%2Fjob_id%2F1")
    assert _error_line(line) == ("ingest-data: The replica workerpool0-0 exited with a non-zero status of 1. "
                                 "Termination reason: Error.")
    a = ask("no-cause-recorded")
    assert "https://" not in a.text and "console.cloud.google.com" not in a.text


def test_a_run_that_just_failed_says_its_cause_is_not_recorded_yet():
    import datetime

    c = json.loads(json.dumps(case("no-cause-recorded")))
    run = c["data"]["runs"]["churn"]["runs"][0]
    run["ended"] = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=10)).isoformat()
    a = replay.engine_for(c, replay.ScriptedBrain()).ask(c["question"])
    assert "Cause: not recorded yet." in a.text and "hourly check" in a.text and "were off" not in a.text
    old = ask("no-cause-recorded")                       # ended days ago: it will not come
    assert "Cause: not recorded." in old.text and "failure records were off for the release that ran" in old.text


def test_the_answer_says_when_a_newer_run_exists():
    c = json.loads(json.dumps(case("ap-permission")))
    c["data"]["runs"]["churn"]["runs"].insert(0, {"run_id": "r9a8b7c6d5e4f", "pipeline": "training",
                                                  "state": "running", "created": "2026-11-15T09:00:00Z"})
    a = replay.engine_for(c, replay.ScriptedBrain()).ask(c["question"])
    assert a.run_id == "raac5375705f1"
    assert "Note: this is not the newest training run of churn: r9a8b7c6d5e4f" in a.text
    assert "is still running" in a.text
    assert "not the newest" not in ask("ap-permission").text


def test_the_answer_lists_every_failed_step():
    c = json.loads(json.dumps(case("kf-stale-model")))
    run = c["data"]["runs"]["churn-gke"]["runs"][0]
    run["failed_steps"] = ["load-model", "validate-data"]
    run["cause"]["also"] = [{"step": "validate-data", "rule": "checks", "owner": "team",
                             "summary": "validate-data: tenure_months: 715 values below 0"}]
    a = replay.engine_for(c, replay.ScriptedBrain()).ask(c["question"])
    assert "Also failed: validate-data: tenure_months: 715 values below 0 (owner: team)" in a.text


def test_a_port_forward_into_the_cluster_still_carries_the_id_token(repo, monkeypatch):
    from pipewright.assist import hosted
    from pipewright.config import load_environment

    assist_repo(repo)
    repo.manifests["dev"]["tenants"]["churn"]["agent_server"] = {
        "url": "http://pipewright-mcp-churn.pipewright-mcp.svc.cluster.local", "runtime": "gke",
        "service": "pipewright-mcp-churn", "namespace": "pipewright-mcp"}
    repo.write()
    env = load_environment(repo.root, "dev")
    monkeypatch.setattr(hosted, "identity_token", lambda: "TOKEN")
    seen = []
    hosted.ask(env, "churn", "q", "churn", "1.4.4", server_url="http://localhost:8765",
               call=lambda url, headers, arguments: seen.append((url, headers)) or {"ok": True, "answer": "a"})
    assert seen[0][0] == "http://localhost:8765/mcp" and seen[0][1]["Authorization"] == "Bearer TOKEN"

    def down(*a):
        raise ConnectionError("refused")

    from pipewright.errors import ConfigError

    forward = "kubectl port-forward -n pipewright-mcp service/pipewright-mcp-churn 8765:80"
    with pytest.raises(ConfigError, match=forward):
        hosted.ask(env, "churn", "q", "churn", "1.4.4", call=down)


def test_a_reply_is_read_whether_or_not_it_is_really_chunked():
    from pipewright.assist.hosted import _unchunk

    plain = b'{"jsonrpc":"2.0","id":1,"result":{}}'
    assert _unchunk(plain) == plain                                    # a chunked header on a plain body (found live)
    assert _unchunk(b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n") == b"hello world"
    assert _unchunk(b"") == b""


def test_model_armor_that_cannot_be_reached_refuses_and_never_raises():
    class Unreachable:
        def post(self, url, body):
            raise ConnectionError("certificate verify failed")

    screen = Screen(TEMPLATE, Unreachable())
    assert "could not be reached: ConnectionError" in screen.screen("question", "why did it fail?")
    a = ask("ap-contract", screen=screen)                       # refused at the question: the model is not called
    assert a.kind == "refused" and "could not run" in a.text and a.model_calls == 0

    class OnlyQuestions:                                        # the question passes; the tool text cannot be screened
        def __init__(self):
            self.n = 0

        def post(self, url, body):
            self.n += 1
            if self.n > 1:
                raise ConnectionError("down")
            return SimpleNamespace(ok=True, status=200,
                                   body={"sanitizationResult": {"filterMatchState": "NO_MATCH_FOUND"}})

    b = ask("ap-contract", screen=Screen(TEMPLATE, OnlyQuestions()))
    assert b.kind == "refused" and "Assist did not answer" in b.text and "tenure_months" not in b.text


def test_a_hosted_answer_wrapped_by_the_tool_server_is_unwrapped(monkeypatch):
    from pipewright.assist import hosted

    inner = {"ok": True, "answer": "It failed."}
    reply = {"jsonrpc": "2.0", "id": 1, "result": {"structuredContent": {"result": inner}, "content": []}}
    monkeypatch.setattr(hosted, "_post", lambda url, headers, body: (200, "application/json", json.dumps(reply)))
    assert hosted._call("http://x/mcp", {}, {"question": "q"}) == {"ok": True, "answer": "It failed."}
    monkeypatch.setattr(hosted, "_post", lambda url, headers, body: (401, "application/json", "{}"))
    assert "did not admit you (401)" in hosted._call("http://x/mcp", {}, {})["refused"]


def test_the_token_ceiling_is_named_and_stops_the_tools():
    """1.4.4 GA QA (live): at the token ceiling the answer said '8 tool calls'."""
    c = CASES[0]
    tools = Tools(FrozenSource(c["data"]), "retail", None, 8)

    class Spent(replay.ScriptedBrain):
        settings = SimpleNamespace(max_tokens=1500)

        def investigate(self, question, context, tools):
            found, usage = super().investigate(question, context, tools)
            tools.stop("the ceiling of 1,500 tokens for one question is reached")
            assert "1,500 tokens" in tools.known_fix("checks")["refused"]      # no further tool call
            return found, {**usage, "ceiling": "tokens"}

    a = Engine(Spent(), tools, "test", None, c.get("usecase")).ask(c["question"])
    assert "Assist stopped at its ceiling for one question (1,500 tokens)" in a.text
    assert "tool calls)" not in a.text.split("ceiling for one question")[1].split("\n")[0]

