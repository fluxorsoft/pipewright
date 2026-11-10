"""`pipewright ab`: plan, start, set, pause, resume, stop, analyse, decide, conclude, status and list.

The CLI runs these as the person who types them (their identity is recorded); the launcher records approver-group
decisions (`ab decide` over the group's topic, as retrain decisions travel) and runs the scheduled analysis as the
tenant's runner. Every change is an event (`ab_test.*`), copied to the tenant's feed when `ab_test.publish.events`.
"""

from __future__ import annotations

import base64
import io
import json
from datetime import datetime, timezone

from pipewright.abtest import analysis as ab_analysis
from pipewright.abtest import config as ab_config
from pipewright.abtest import stats
from pipewright.abtest.store import ACTIVE, TestStore
from pipewright.errors import ConfigError

DECISION_KIND = "ab.decision"


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def section_of(uc) -> dict:
    t = (uc.ml or {}).get("ab_test")
    if not t:
        raise ConfigError(f"{uc.name} has no A/B test in {uc.environment}: set ab_test.enabled (usually in the "
                          f"environment file of the environment that runs it)")
    return t


def feed_topic(env, tenant: str) -> str | None:
    return ((env.tenants.get(tenant) or {}).get("ab_test_feed") or {}).get("topic")


RESULTS_KEPT = 200                 # analyses kept on the record in brief (FR-121); the results table keeps all


def without_by(value):
    """The event without who acted (FR-122): every `by`, at any depth (a decision's approvals carry theirs too)."""
    if isinstance(value, dict):
        return {k: without_by(v) for k, v in value.items() if k != "by"}
    if isinstance(value, list):
        return [without_by(v) for v in value]
    return value


def emit(env, uc, gcp, event_type: str, test: dict, **data) -> None:
    """The event, on the events topic and, with publish.events, on the tenant's feed (aggregates only)."""
    from pipewright.observability import events

    e = events.event(event_type, uc.name, env.name, uc.tenant, test.get("release"), None, ab_test=test["name"],
                     **data)
    events.emit(e, env.observability.get("events_topic"), gcp, stream=io.StringIO(), region=env.region)
    publish = ((uc.ml or {}).get("ab_test") or {}).get("publish") or {}
    topic = feed_topic(env, uc.tenant)
    if publish.get("events") and topic:
        sent = e if publish.get("include_by") else without_by(e)
        r = gcp.post(events.publish_url(topic, env.region), {"messages": [{
            "data": base64.b64encode(json.dumps(sent, default=str).encode()).decode(),
            "attributes": {"usecase": uc.name, "ab_test": test["name"], "event_type": event_type}}]})
        if not r.ok:
            print(f"note: the event was not copied to the feed {topic} (HTTP {r.status})")


class ABTests:
    def __init__(self, uc, env, gcp, log=print, online=None):
        self.uc, self.env, self.gcp, self.log = uc, env, gcp, log
        self.store = TestStore(gcp, env.artifacts_bucket)
        self.online = online          # phase B: starts and stops the treatment's serving arm (None: batch only)

    # ------------------------------------------------------------------------------------------- reading --
    def current(self) -> dict | None:
        return self.store.current(self.uc.name)

    def get(self, name: str | None) -> dict:
        t = self.store.get(self.uc.name, name) if name else self.current()
        if t is None:
            raise ConfigError(f"{self.uc.name} has no {'test ' + name if name else 'running test'} in "
                              f"{self.env.name}: `pipewright ab list` shows past ones")
        return t

    def _change(self, name: str, fn) -> dict:
        out = self.store.update(self.uc.name, name, fn)
        if out is None:
            raise ConfigError(f"test {name} of {self.uc.name} is gone")
        from pipewright.abtest import online

        if online.is_kserve(out):                 # a KServe router follows this file within 30 seconds
            online.write_state(self.gcp, self.env.artifacts_bucket, out)
        return out

    def _history(self, t: dict, action: str, by: str | None, **extra) -> dict:
        t["history"] = (t.get("history") or []) + [{"at": now_iso(), "action": action, "by": by, **extra}]
        return t

    # -------------------------------------------------------------------------------------------- start --
    def start(self, identity: str | None, approved: bool) -> dict:
        from pipewright.loop import driver
        from pipewright.models import deploy as deploy_model
        from pipewright.models import registry
        from pipewright.retraining.store import CaseStore
        from pipewright.runs import launch

        section = section_of(self.uc)
        if self.online is not None and hasattr(self.online, "check"):
            self.online.check()                                         # an arm Pipewright can make here
        deploy_model.check_approval(self.uc, identity, approved)        # D6: starting a test is a deploy
        case = CaseStore(self.gcp, self.env.artifacts_bucket).current(self.uc.name)   # not during a closed loop
        loop = (case or {}).get("loop")
        if loop and not driver.finished(loop):
            raise ConfigError(f"{self.uc.name} has a closed retrain loop running (case {case.get('case_id')}): "
                              "finish it before an A/B test")
        creds = self.gcp.session.credentials
        default = next((v for v in registry.versions(self.uc, self.env, creds, check_gates=False) if v.default), None)
        if default is None:
            raise ConfigError(f"{self.uc.model['name']} has no default version (the control) in {self.env.name}")
        treatment = str(section["arms"]["treatment"]["model_version"]).removeprefix("v")
        if treatment == default.version_id:
            raise ConfigError(f"v{treatment} is the default: the treatment must be another version")
        overlaps = self._overlaps(section)
        release = launch.live_release(self.env, self.uc.name, self.gcp)
        record = registry.approve_for_test(self.uc, self.env, creds, treatment, section["name"], identity, approved,
                                           self.log)
        test = {"name": section["name"], "usecase": self.uc.name, "tenant": self.uc.tenant,
                "environment": self.env.name, "mode": section["mode"], "unit": section["unit"],
                "digest": ab_config.digest(section), "release": release, "state": "running",
                "started_at": now_iso(), "started_by": identity,
                "arms": {"control": {"version": default.version_id},
                         "treatment": {"version": treatment, "files": len(record.get("files") or [])}},
                "allocation": [{"at": now_iso(), "treatment_bp": section["allocation"]["treatment_bp"],
                                "by": identity, "reason": "start"}],
                "decision": {"required": list(section["decision"]["approvals"]["required"]), "approvals": []},
                "history": []}
        if overlaps:
            test["overlaps"] = overlaps
        if section.get("layer"):                                # FR-123: the test's range of its layer
            test["layer"] = self._claim(section)
        self._history(test, "start", identity)
        try:
            self.store.create(test)
        except Exception:
            self._release(test)
            raise
        if self.online is not None:
            arms = {}
            try:
                arms = self.online.start(test)                # the arm first: the gateway routes to what is stored
                test = self._change(test["name"], lambda t: {**t, "arms": {**t["arms"], **arms}})
                self.online.apply(test)
            except Exception:
                stopped = self._change(test["name"], lambda t: self._history(
                    {**t, "state": "stopped"}, "stop", identity, reason="the serving arm could not start"))
                if arms:
                    try:
                        self.online.stop(stopped)
                    except Exception as e:  # noqa: BLE001 - the first failure is the one to report
                        self.log(f"the treatment's arm was not removed ({e}): run pipewright ab stop again")
                raise
        emit(self.env, self.uc, self.gcp, "ab_test.started", test, by=identity, control=default.version_id,
             treatment=treatment,
             treatment_bp=section["allocation"]["treatment_bp"], mode=section["mode"], digest=test["digest"])
        self.log(f"started {test['name']}: v{treatment} against the default v{default.version_id}, "
                 f"{section['allocation']['treatment_bp'] / 100:g}% of units to the treatment (mode {section['mode']})")
        if test.get("layer"):
            ly = test["layer"]
            self.log(f"  layer {ly['name']}: this test has {ly['share_bp'] / 100:g}% of its units (positions "
                     f"{ly['start_bp']} to {ly['end_bp']} of 10000); the rest are not in the test")
        return test

    def _claim(self, section: dict) -> dict:
        from pipewright.abtest import layers
        from pipewright.abtest.store import ACTIVE

        def active(usecase: str, name: str) -> bool:
            other = self.store.get(usecase, name)
            return bool(other and other.get("state") in ACTIVE)

        return layers.claim(self.gcp, self.env.artifacts_bucket, section["layer"], section["unit"], self.uc.name,
                            section["name"], active)

    def _release(self, t: dict) -> None:
        """The test's range of its layer, freed (a later claim drops an ended test's range anyway)."""
        if not t.get("layer"):
            return
        from pipewright.abtest import layers

        try:
            layers.release(self.gcp, self.env.artifacts_bucket, t["layer"]["name"], self.uc.name, t["name"])
        except Exception as e:  # noqa: BLE001 - the next claim in the layer frees it
            self.log(f"note: layer {t['layer']['name']} still lists {t['name']} ({e}); the next start frees it")

    def _overlaps(self, section: dict) -> list[dict]:
        """Other use cases' active tests in this environment on the same unit (policies.ab_test.overlap): their
        units would be in two tests at once, so neither result is clean. refuse stops the start; warn records them on
        the test (Fleet View shows overlaps across environments too); allow (the default) does not look."""
        mode = (((self.env.policies or {}).get("ab_test") or {}).get("overlap") or "allow")
        if mode == "allow":
            return []
        from pipewright.abtest.store import ACTIVE

        found = []
        for other in self.store.usecases():
            if other == self.uc.name:
                continue
            t = self.store.current(other)
            same_layer = bool(section.get("layer")) and (t or {}).get("layer", {}).get("name") == \
                section["layer"]["name"]                     # FR-123: apart by construction
            if t and t.get("state") in ACTIVE and t.get("unit") == section["unit"] and not same_layer:
                found.append({"usecase": other, "ab_test": t["name"], "unit": t["unit"], "state": t["state"]})
        if found and mode == "refuse":
            names = ", ".join(f"{o['ab_test']} ({o['usecase']})" for o in found)
            raise ConfigError(f"{names} already test(s) units by {section['unit']} in {self.env.name}: a unit in two "
                              "tests at once spoils both results (policies.ab_test.overlap: refuse); wait for it to "
                              "end, or test on another unit")
        for o in found:
            self.log(f"warning: {o['ab_test']} ({o['usecase']}) also tests units by {o['unit']}: the results of "
                     "both are affected (policies.ab_test.overlap: warn)")
        return found

    # ------------------------------------------------------------------------------- allocation and state --
    def set_allocation(self, bp: int, reason: str, identity: str | None) -> dict:
        t = self.get(None)

        def change(t):
            cur = int(t["allocation"][-1]["treatment_bp"])
            if bp >= cur:
                raise ConfigError(f"the allocation may only go down while a test runs ({cur} now): units must never "
                                  "move into the treatment; start a new test (a new name) for more")
            t["allocation"].append({"at": now_iso(), "treatment_bp": bp, "by": identity, "reason": reason})
            return self._history(t, "set", identity, treatment_bp=bp, reason=reason)

        t = self._change(t["name"], change)
        emit(self.env, self.uc, self.gcp, "ab_test.allocation_changed", t, treatment_bp=bp, reason=reason, by=identity)
        if self.online is not None:
            self.online.apply(t)
        return t

    def set_state(self, state: str, reason: str, identity: str | None) -> dict:
        if state == "stopped" and self.current() is None and self.online is not None:
            # A stop or a conclusion whose removal of the serving arm failed part way (found in the battle test: the
            # cluster's API endpoint dropped a connection and the router stayed): `ab stop` finishes it.
            last = (self.store.pointer(self.uc.name)[0] or {}).get("last")
            done = self.store.get(self.uc.name, last) if last else None
            if done and done.get("state") in ("stopped", "concluded"):
                self.log(f"test {done['name']} is {done['state']}: removing what is left of its serving arm")
                self._remove(done)
                return done
        t = self.get(None)
        allowed = {"paused": ("running", "decision_required"), "running": ("paused",),
                   "stopped": ACTIVE}[state]

        def change(t):
            if t["state"] not in allowed:
                raise ConfigError(f"test {t['name']} is {t['state']}: cannot {state.removesuffix('ed')}")
            t["state"] = state
            return self._history(t, {"paused": "pause", "running": "resume", "stopped": "stop"}[state], identity,
                                 reason=reason)

        t = self._change(t["name"], change)
        if state == "stopped":
            self._release(t)
        emit(self.env, self.uc, self.gcp, {"paused": "ab_test.paused", "running": "ab_test.resumed",
                                           "stopped": "ab_test.stopped"}[state], t, reason=reason, by=identity)
        if self.online is not None:
            self._remove(t) if state == "stopped" else self.online.apply(t)
        return t

    def _remove(self, t: dict) -> None:
        try:
            self.online.stop(t)
        except Exception as e:
            raise ConfigError(f"test {t['name']} is {t['state']}, but its serving arm was not fully removed ({e}): "
                              f"run `pipewright ab stop --env {self.env.name} --usecase {self.uc.name}` again to "
                              "finish") from e

    # ---------------------------------------------------------------------------------------- analysis --
    def analyse(self, name: str | None = None, query=None, now: datetime | None = None, write: bool = True,
                test: dict | None = None, record: bool = True) -> dict:
        """The analysis now, as the person running it (their BigQuery access); written to the record and the
        results table, and ab_test.analysed emitted. query: (sql) -> rows, for tests. The tenant's scheduled job
        passes the test it was handed and record=False: the launcher records its result (watch.ingest)."""
        from pipewright.gcp import bigquery

        t = test or self.get(name)
        section = section_of(self.uc)
        if ab_config.digest(section) != t.get("digest"):
            raise ConfigError(f"the ab_test section differs from what test {t['name']} promised at its start "
                              "(name, unit, mode, metric, guardrails, analysis or arms changed): the test is judged "
                              "by what it promised; restore the section, or stop this test and start a new one")
        dataset = self.uc.resources.get("bigquery_dataset")
        kms = (self.env.security or {}).get("bigquery_kms_key")
        if query is None:
            def query(sql):
                # BigQuery refuses a job-level key on DDL and DML (found in the battle test, CMEK in Enterprise):
                # the results table carries its key in its options instead
                write = sql.lstrip().upper().startswith(("CREATE", "INSERT"))
                return bigquery.rows(bigquery.query(self.gcp, self.env.project, sql, location=self.env.region,
                                                    kms_key=None if write else kms))
        output = (self.uc.ml.get("inference") or {}).get("output_table") if "inference" in self.uc.pipelines else None
        online = bool(self.uc.model.get("serving", {}).get("enabled"))
        result = ab_analysis.run(t, section, self.env.project, dataset, output, online and self._has_online(query),
                                 query, now, previous=t.get("analysis"))
        if write:
            self._write_result(t, result, query)
        if write and record:
            t = self.record_result(t["name"], result)
        return {"test": t, "result": result}

    def _has_online(self, query) -> bool:
        try:
            query(f"SELECT 1 FROM `{self.env.project}.{self.uc.resources.get('bigquery_dataset')}."
                  f"{ab_analysis.EXPOSURES_ONLINE}` LIMIT 0")
            return True
        except Exception:  # noqa: BLE001 - no exposure table yet: no online exposures yet
            return False

    def results_table(self) -> str:
        return section_of(self.uc).get("results_table") or f"{self.uc.name.replace('-', '_')}_ab_results"

    def _write_result(self, t: dict, result: dict, query) -> None:
        dataset = self.uc.resources.get("bigquery_dataset")
        ref = f"`{self.env.project}.{dataset}.{self.results_table()}`"
        cols = ", ".join(f"{c['name']} {c['type'].replace('FLOAT', 'FLOAT64').replace('INTEGER', 'INT64')}"
                         for c in ab_analysis.RESULTS_SCHEMA)
        kms = (self.env.security or {}).get("bigquery_kms_key")
        options = f" OPTIONS(kms_key_name='{kms}')" if kms else ""
        query(f"CREATE TABLE IF NOT EXISTS {ref} ({cols}){options}")
        row = ab_analysis.results_row(t, result, t.get("release"))

        def lit(v):
            if v is None:
                return "NULL"
            if isinstance(v, (int, float)):
                return repr(float(v)) if isinstance(v, float) else str(v)
            return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"

        names = [c["name"] for c in ab_analysis.RESULTS_SCHEMA]
        values = ", ".join(f"TIMESTAMP({lit(row[n])})" if n == "analysed_at" else lit(row[n]) for n in names)
        query(f"INSERT INTO {ref} ({', '.join(names)}) VALUES ({values})")

    def record_result(self, name: str, result: dict, ingested: str | None = None) -> dict:
        """The result into the record (the newest, and the looks for a sequential test), the state moved to
        decision_required when it says so, and ab_test.analysed emitted; guardrail breaches are events of their own."""
        def change(t):
            t["analysis"] = result
            if ingested:
                t["ingested"] = ingested
            p = (result.get("primary") or {}).get("p_sequential")
            if p is not None:
                t["looks"] = (t.get("looks") or []) + [{"at": result["at"], "p": p}]
            # FR-121: every analysis in brief, so a reader sees the result settle (or not) over the test
            prim = result.get("primary") or {}
            t["results"] = ((t.get("results") or []) + [{
                "at": result["at"], "status": result.get("status"), "diff": prim.get("diff"), "ci": prim.get("ci"),
                "p": prim.get("p"), "p_sequential": prim.get("p_sequential"),
                "exposed": result.get("exposed")}])[-RESULTS_KEPT:]
            if result["status"] == "decision_required" and t["state"] == "running":
                t["state"] = "decision_required"
                self._history(t, "decision_required", None, reasons=result["decision_reasons"])
            return t

        t = self._change(name, change)
        emit(self.env, self.uc, self.gcp, "ab_test.analysed", t, status=result["status"],
             primary={k: (result.get("primary") or {}).get(k) for k in ("name", "diff", "ci", "p", "p_sequential")},
             exposed=result["exposed"], blocked=result["blocked"], reasons=result["decision_reasons"])
        for g in result.get("guardrails") or []:
            if g.get("breached"):
                emit(self.env, self.uc, self.gcp, "ab_test.guardrail_breached", t, guardrail=g["name"],
                     kind="business", relative=g.get("relative"))
        return t

    # ---------------------------------------------------------------------------------------- decisions --
    def decide(self, winner: str, reason: str, group: str | None) -> str:
        """An approver group's decision, sent over the group's topic (membership is publish access), as retrain
        decisions travel; the launcher records it."""
        from pipewright.observability.events import approval, publish_url
        from pipewright.retraining.client import can_publish
        from pipewright.runs import launch

        t = self.get(None)
        if winner not in ("treatment", "control"):
            raise ConfigError("--winner treatment or control")
        if not (reason or "").strip():
            raise ConfigError("give --reason: it is recorded with your name")
        required = t["decision"]["required"]
        if not required:
            raise ConfigError(f"test {t['name']} needs no approver group: `pipewright ab conclude` decides")
        todo = [g for g in required if g not in {a["group"] for a in t["decision"]["approvals"]}]
        candidates = [group] if group else todo
        member = [g for g in candidates if g in self.env.approver_groups
                  and can_publish(self.gcp, self.env.approver_groups[g]["topic"])]
        if not member:
            raise ConfigError(f"you are not a member of {'group ' + group if group else 'a group'} this test still "
                              f"needs ({', '.join(candidates) or 'none left'}); membership is publish access to the "
                              "group's topic")
        g = member[0]
        url = launch.launcher_url(self.env)
        body = {"kind": DECISION_KIND, "usecase": self.uc.name, "ab_test": t["name"], "winner": winner,
                "reason": reason.strip(), "approval": approval(), "id_token": launch.id_token(url)}
        topic = self.env.approver_groups[g]["topic"]
        r = self.gcp.post(publish_url(topic, self.env.region), {"messages": [{
            "data": base64.b64encode(json.dumps(body).encode()).decode(),
            "attributes": {"usecase": self.uc.name, "ab_test": t["name"], "action": "ab-decide"}}]})
        if not r.ok:
            raise ConfigError(f"decision not sent to {topic}: HTTP {r.status}")
        return f"sent as approver group {g} (message {r.body['messageIds'][0]}); the launcher records it"

    def conclude(self, winner: str | None, reason: str, identity: str | None, approved: bool) -> dict:
        """Ends the test with its winner. With approver groups, the winner is the one they recorded (`ab decide`);
        without, the person concluding decides here. The treatment winning becomes the default through the normal
        approval (judged by the released gates, its files checked); either way the treatment's arm is removed."""
        from pipewright.models import registry

        t = self.get(None)
        decided = (t.get("decision") or {}).get("winner")
        if t["decision"]["required"]:
            if not decided:
                missing = [g for g in t["decision"]["required"]
                           if g not in {a["group"] for a in t["decision"]["approvals"]}]
                raise ConfigError(f"test {t['name']} waits for approver group(s) {', '.join(missing)} "
                                  "(`pipewright ab decide`)")
            if winner and winner != decided:
                raise ConfigError(f"the approver groups chose the {decided}, not the {winner}")
            winner = decided
        if winner not in ("treatment", "control"):
            raise ConfigError("--winner treatment or control")
        if winner == "treatment" and t.get("mode") == "shadow":
            from pipewright.abtest.decide import SHADOW

            raise ConfigError(SHADOW)
        if not (reason or "").strip():
            raise ConfigError("give --reason: it is recorded with your name")
        if winner == "treatment" and t["state"] != "decision_required" and not t["decision"]["required"]:
            res = t.get("analysis") or {}
            if not res or res.get("blocked"):
                raise ConfigError(f"test {t['name']} has no decision-ready analysis: the treatment can win only on "
                                  "evidence (`pipewright ab analyse`); stop the test to keep the control")
        if winner == "treatment":
            registry.approve(self.uc, self.env, self.gcp.session.credentials, t["arms"]["treatment"]["version"],
                             identity, approved, self.log)

        def change(t):
            t["state"] = "concluded"
            t["decision"] = {**t["decision"], "winner": winner, "at": now_iso(), "by": identity, "reason": reason}
            return self._history(t, "conclude", identity, winner=winner, reason=reason)

        t = self._change(t["name"], change)
        self._release(t)
        if self.online is not None:
            self._remove(t)
        emit(self.env, self.uc, self.gcp, "ab_test.decided", t, winner=winner, reason=reason, by=identity,
             control=t["arms"]["control"]["version"], treatment=t["arms"]["treatment"]["version"],
             result={k: ((t.get("analysis") or {}).get("primary") or {}).get(k) for k in ("name", "diff", "ci", "p")})
        return t

    # ------------------------------------------------------------------------------------------- planning --
    def plan(self, baseline: float | None = None, sd: float | None = None, per_day: float | None = None) -> dict:
        """Units needed per arm for the planned effect, and how long that takes at the scoring volume; the cost of a
        dedicated treatment arm when the use case serves online."""
        section = section_of(self.uc)
        m, a, bp = section["metric"], section["analysis"], section["allocation"]["treatment_bp"]
        out = {"test": section["name"], "allocation_bp": bp, "metric": m["name"], "type": m["type"],
               "minimum_effect": m["minimum_effect"], "alpha": a["alpha"], "power": a["power"]}
        if baseline is None:
            out["note"] = ("give --baseline (the control's current value of the metric; and --sd for a mean or "
                           "ratio) to size the test")
            return out
        need_c, need_t = stats.sample_size(m["type"], baseline, m["minimum_effect"], sd, a["alpha"], a["power"], bp)
        out.update(baseline=baseline, needed={"control": need_c, "treatment": need_t}, total=need_c + need_t)
        share = int((section.get("layer") or {}).get("share_bp") or 10000)
        if per_day and share < 10000:          # FR-123: a test in a layer sees only its share of the units
            out["layer"] = {"name": section["layer"]["name"], "share_bp": share,
                            "units_per_day_in_test": round(per_day * share / 10000, 1)}
            per_day = per_day * share / 10000
        if per_day:
            days = (need_c + need_t) / per_day + m["window_days"]
            out["days"] = round(days, 1)
            d = section["duration"]
            if days > d["max_days"]:
                out["warning"] = (f"about {days:.0f} days (units at {per_day:g} a day, plus the {m['window_days']}-day "
                                  f"outcome window) is beyond max_days {d['max_days']}: raise the allocation, accept "
                                  "a larger minimum_effect, or pool more units")
            elif days < d["min_days"]:
                out["note"] = f"the sample arrives sooner, but no decision before min_days {d['min_days']}"
        if self.uc.model.get("serving", {}).get("enabled"):
            from pipewright.serving import prices

            try:
                hour = prices.machine_hour(self.uc.model["serving"].get("machine", "n1-standard-2"))
                monthly = hour * prices.HOURS_A_MONTH
                out["online_arm_cost"] = (f"a dedicated treatment arm: about ${monthly:.0f} a month "
                                          f"per {self.uc.model['serving'].get('machine', 'n1-standard-2')} replica "
                                          "(none extra on a shared pool)")
            except Exception:  # noqa: BLE001 - an unknown machine has no list price here
                pass
        return out


def render_test(t: dict) -> str:
    alloc = t.get("allocation") or []
    lines = [f"{t['name']} ({t['usecase']}, {t.get('environment')}): {t['state']}, mode {t.get('mode', 'ab')}",
             f"  control v{t['arms']['control']['version']}, treatment v{t['arms']['treatment']['version']}; "
             f"allocation {int(alloc[-1]['treatment_bp']) / 100:g}% (since {alloc[-1]['at']})" if alloc else "",
             f"  started {t.get('started_at')} by {t.get('started_by')}; release {t.get('release')}"]
    d = t.get("decision") or {}
    if d.get("required"):
        got = ", ".join(f"{a['group']} ({a['winner']})" for a in d.get("approvals") or []) or "none yet"
        lines.append(f"  approvals needed: {', '.join(d['required'])}; recorded: {got}")
    if d.get("winner"):
        lines.append(f"  decided: {d['winner']} ({d.get('at')}, {d.get('by')}): {d.get('reason')}")
    if t.get("breaker"):
        lines.append(f"  the gateway's breaker tripped at {t['breaker'].get('tripped_at')}: "
                     f"{t['breaker'].get('error_rate')} errors on the treatment")
    if t.get("layer"):
        ly = t["layer"]
        lines.append(f"  layer {ly['name']}: {ly['share_bp'] / 100:g}% of its units (positions {ly['start_bp']} to "
                     f"{ly['end_bp']} of 10000)")
    if t.get("analysis"):
        lines.append(ab_analysis.render(t["analysis"], t))
    from pipewright.abtest.decide import next_step

    step = next_step(t)
    lines.append(f"  next: {step['text']}" + (f" (`{step['command']}`)" if step.get("command") else ""))
    return "\n".join(x for x in lines if x)
