"""Keeping approved files protected for as long as they are needed (Pipewright 1.4.1, FR-101 item 8; section 8.18,
"Lock renewal, settled").

Invariant: when policies.integrity.lock is on, every stored artifact needed to verify, serve, score or roll back an
active approved model has effective protection before it becomes active, for as long as it is needed.

- `discover` finds the artifacts, deduplicated, each by bucket, object and generation, with its type (model file,
  KServe copy, release contract) and why it is protected (default, serving, rollback candidate, live contract).
  Operational truth comes from live sources (registry, endpoints, KServe, release pointers) and the event log; there
  is no second deployment-state store. Failing safe means protecting more: when the history is unclear, every
  candidate is protected and the run is `degraded`, never "nothing renewed".
- `reconcile` extends retention on exact generations when the protection left is under the threshold
  T = max(lock.days / 2, 3 days): read the metadata, patch with ifMetagenerationMatch, re-read on a lost race, never
  shorten, verify after. The patch body is only {"retention": ...}.
- `pipewright integrity renew` runs both as the dedicated renewer identity (not the launcher), records
  model.lock.renewed and model.lock.failed, and one model.lock.reconciliation summary per run.

Names, generations, dates and counts only; never a file's contents.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

GCS = "https://storage.googleapis.com/storage/v1"
MIN_THRESHOLD_DAYS = 3
RETRIES = 3


@dataclass
class Artifact:
    bucket: str
    object: str
    generation: str
    type: str                          # model_file | kserve_copy | release_contract
    reasons: list = field(default_factory=list)
    usecase: str | None = None
    version: str | None = None

    @property
    def key(self) -> tuple:
        return (self.bucket, self.object, str(self.generation))

    def record(self) -> dict:
        return {k: v for k, v in asdict(self).items() if v not in (None, [], "")}


def threshold_days(lock_days: int) -> float:
    return max(lock_days / 2, MIN_THRESHOLD_DAYS)


def _split(folder: str) -> tuple[str, str]:
    bucket, _, prefix = folder.removeprefix("gs://").partition("/")
    return bucket, prefix.rstrip("/") + "/"


def _add(found: dict, art: Artifact, reason: str) -> None:
    have = found.setdefault(art.key, art)
    if reason not in have.reasons:
        have.reasons.append(reason)


def _files(found: dict, approved: dict | None, kind: str, reason: str, usecase: str, version: str) -> None:
    if not approved:
        return
    bucket, prefix = _split(approved["folder"])
    for f in approved["files"]:
        _add(found, Artifact(bucket, prefix + f["file"], str(f["generation"]), kind, [], usecase, version), reason)


def _made_default(events: list[dict]) -> list[dict]:
    """Every event that made a version the default, oldest first: approvals (with `previous`) and registrations that
    were the default at once. Other ways (loop rollback re-approves; the console leaves no event) are caught by the
    cross-check with what is default now."""
    out = []
    for e in events:
        if e.get("type") == "model.approved" or (e.get("type") == "model.registered" and e.get("default")):
            out.append(e)
    return sorted(out, key=lambda e: e["at"])


def rollback_candidates(usecase: str, current: str, timeline: list[dict], days: int,
                        now: datetime) -> tuple[list, bool]:
    """([(version, replaced_at)], degraded) for one use case: the immediately previous default while
    now < replaced_at + days. degraded when the event that made `current` the default is missing or unclear, and then
    every version made default within the window is a candidate (protect more, never less)."""
    if days <= 0:
        return [], False
    mine = [e for e in timeline if e.get("usecase") == usecase]
    made_current = [e for e in mine if str(e.get("version")) == str(current)]
    window_start = now - timedelta(days=days)
    if not made_current:
        others = {str(e["version"]) for e in mine if str(e.get("version")) != str(current)
                  and datetime.fromisoformat(e["at"]).replace(tzinfo=timezone.utc) >= window_start}
        return [(v, None) for v in sorted(others)], True
    last = made_current[-1]
    replaced_at = datetime.fromisoformat(last["at"]).replace(tzinfo=timezone.utc)
    if now >= replaced_at + timedelta(days=days):
        return [], False
    previous = last.get("previous")
    if not previous:
        earlier = [e for e in mine if e["at"] < last["at"] and str(e.get("version")) != str(current)]
        previous = str(earlier[-1]["version"]) if earlier else None
    return ([(str(previous), last["at"])] if previous else []), False


def discover(gcp, env, models: dict | None = None, now: datetime | None = None) -> dict:
    """{"artifacts": [Artifact], "notes": [...], "degraded": [...], "rollback_candidates": n, "contracts": n}."""
    from pipewright.models import approvals, integrity
    from pipewright.models import watch as iwatch

    now = now or datetime.now(timezone.utc)
    pol = integrity.policy(env)
    found, notes, degraded = {}, [], []
    uses, use_notes = iwatch.in_use(gcp, env, models)
    notes += use_notes
    defaults = {}
    for u in uses:
        approval = (approvals.events(gcp, env, "model.approved", u.usecase, u.version) or [None])[0]
        reason = "default" if u.kind == "default" else "serving"
        if approval is None:
            notes.append(f"{u.usecase} v{u.version} ({u.where}) has no approval: nothing approved to protect")
        else:
            _files(found, integrity.approved_files(approval), "model_file", reason, u.usecase, u.version)
        if u.kind == "kserve" and u.copy:
            _files(found, {"folder": u.copy["folder"], "files": u.copy["files"]}, "kserve_copy", "serving",
                   u.usecase, u.version)
        if u.kind == "default":
            defaults[u.usecase] = u.version
    timeline = []
    for kind in ("model.approved", "model.registered"):
        timeline += [{**e, "type": kind} for e in approvals.all_events(gcp, env, kind)]
    timeline = _made_default(timeline)
    candidates = 0
    for uc, current in defaults.items():
        found_candidates, unclear = rollback_candidates(uc, current, timeline, pol["lock"]["rollback_days"], now)
        if unclear and found_candidates:     # found live: with nothing to protect more, it is a note, not degraded
            degraded.append(f"{uc}: the event that made v{current} the default is missing, so every version made "
                            "default in the rollback window is protected")
        for version, _ in found_candidates:
            approval = (approvals.events(gcp, env, "model.approved", uc, version) or [None])[0]
            if approval is None:
                notes.append(f"{uc} v{version} (rollback candidate) has no approval")
                continue
            _files(found, integrity.approved_files(approval), "model_file", "rollback_candidate", uc, version)
            candidates += 1
    contracts = 0
    for uc in sorted({u.usecase for u in uses}):
        art = _live_contract(gcp, env, uc)
        if art:
            _add(found, art, "live_contract")
            contracts += 1
    return {"artifacts": list(found.values()), "notes": notes, "degraded": degraded,
            "rollback_candidates": candidates, "contracts": contracts}


def _live_contract(gcp, env, usecase: str) -> Artifact | None:
    from pipewright.gcp import gcs_read
    from pipewright.release.contract import Pointer, pointer_name

    try:
        text, _ = gcs_read(gcp, env.artifacts_bucket, pointer_name(usecase))
    except Exception:  # noqa: BLE001 - a use case with no readable pointer has no live contract to protect here
        return None
    if not text:
        return None
    name = Pointer.from_json(text).contract
    meta = gcp.get(f"{GCS}/b/{env.artifacts_bucket}/o/{quote(name, safe='')}")
    if not meta.ok:
        return None
    return Artifact(env.artifacts_bucket, name, str(meta.body["generation"]), "release_contract", [], usecase)


# -- reconciliation ------------------------------------------------------------------------------------------------

def _meta(gcp, art: Artifact):
    return gcp.get(f"{GCS}/b/{art.bucket}/o/{quote(art.object, safe='')}", generation=str(art.generation),
                   fields="generation,metageneration,retention,temporaryHold,eventBasedHold")


def _until(meta: dict) -> datetime | None:
    value = ((meta or {}).get("retention") or {}).get("retainUntilTime")
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def _bucket_retention(gcp, bucket: str, cache: dict) -> bool:
    if bucket not in cache:
        r = gcp.get(f"{GCS}/b/{bucket}", fields="objectRetention")
        cache[bucket] = r.ok and ((r.body or {}).get("objectRetention") or {}).get("mode") == "Enabled"
    return cache[bucket]


def reconcile_one(gcp, art: Artifact, lock_days: int, lock_mode: str, now: datetime) -> dict:
    """Extends the artifact's retention to now + lock_days when less than the threshold is left. Never shortens.
    {"status": unchanged | renewed | failed, "previous", "new", "remaining_days", "reason"}."""
    t = timedelta(days=threshold_days(lock_days))
    for _attempt in range(RETRIES):
        r = _meta(gcp, art)
        if r.status == 404:
            return {"status": "failed", "reason": "the approved generation is gone: it cannot be protected again"}
        if not r.ok:
            return {"status": "failed", "reason": f"metadata cannot be read (HTTP {r.status})"}
        meta = r.body or {}
        until = _until(meta)
        if until and until - now >= t:
            return {"status": "unchanged", "previous": meta["retention"]["retainUntilTime"],
                    "remaining_days": round((until - now).total_seconds() / 86400, 1)}
        new = now + timedelta(days=lock_days)
        if until and new <= until:
            return {"status": "unchanged", "previous": meta["retention"]["retainUntilTime"],
                    "remaining_days": round((until - now).total_seconds() / 86400, 1)}
        mode = (meta.get("retention") or {}).get("mode") or ("Locked" if lock_mode == "locked" else "Unlocked")
        body = {"retention": {"mode": mode, "retainUntilTime": new.strftime("%Y-%m-%dT%H:%M:%SZ")}}
        p = gcp.patch(f"{GCS}/b/{art.bucket}/o/{quote(art.object, safe='')}", body, generation=str(art.generation),
                      ifMetagenerationMatch=str(meta["metageneration"]))
        if p.status == 412:
            continue                                   # another writer (or renewer) changed it: read it again
        if not p.ok:
            return {"status": "failed", "reason": f"retention cannot be extended (HTTP {p.status})",
                    "previous": (meta.get("retention") or {}).get("retainUntilTime")}
        after = _until((_meta(gcp, art).body or {}))
        if not after or after < new - timedelta(seconds=1):
            return {"status": "failed", "reason": "the new retention date did not hold when read back"}
        return {"status": "renewed", "previous": (meta.get("retention") or {}).get("retainUntilTime"),
                "new": body["retention"]["retainUntilTime"],
                "remaining_days": round((after - now).total_seconds() / 86400, 1)}
    return {"status": "failed", "reason": f"lost {RETRIES} races with other writers"}


def reconcile(gcp, env, artifacts: list[Artifact], now: datetime | None = None) -> list[dict]:
    from pipewright.models import integrity

    now = now or datetime.now(timezone.utc)
    pol = integrity.policy(env)["lock"]
    buckets, out = {}, []
    for art in artifacts:
        if not _bucket_retention(gcp, art.bucket, buckets):
            r = _meta(gcp, art)
            held = r.ok and (r.body or {}).get("temporaryHold")
            out.append({**art.record(), "status": "hold" if held else "unprotected",
                        "reason": "the bucket has no object retention: protected by a temporary hold, which never "
                                  "expires (renewal does not apply)" if held else
                                  "the bucket has no object retention and the file has no hold"})
            continue
        out.append({**art.record(), **reconcile_one(gcp, art, pol["days"], pol["mode"], now)})
    return out


def summary(results: list[dict], found: dict, lock_days: int) -> dict:
    failed = [r for r in results if r["status"] in ("failed", "unprotected")]
    remaining = [r["remaining_days"] for r in results if r.get("remaining_days") is not None]
    status = "failed" if failed else "degraded" if found.get("degraded") else "healthy"
    return {"status": status, "protected": len(results),
            "renewed": sum(1 for r in results if r["status"] == "renewed"), "failed": len(failed),
            "holds": sum(1 for r in results if r["status"] == "hold"),
            "least_protection_remaining_days": min(remaining) if remaining else None,
            "threshold_days": threshold_days(lock_days), "rollback_candidates": found.get("rollback_candidates", 0),
            "contracts": found.get("contracts", 0), "degraded": found.get("degraded", [])[:10]}


def renew(gcp, env, models: dict | None = None, dry_run: bool = False, emit: bool = True, log=print) -> dict:
    """One renewer run: discover, reconcile, record. Returns the summary with the per-artifact results."""
    from pipewright.models import integrity
    from pipewright.observability import events

    pol = integrity.policy(env)["lock"]
    if pol["days"] <= 0 or not pol["renew"]:
        return {"status": "off", "reason": "policies.integrity.lock.days is 0 or lock.renew is false"}
    now = datetime.now(timezone.utc)
    found = discover(gcp, env, models, now)
    for n in found["notes"]:
        log(f"note: {n}")
    for d in found["degraded"]:
        log(f"DEGRADED: {d}")
    if dry_run:
        return {"status": "dry-run", "artifacts": [a.record() for a in found["artifacts"]],
                "degraded": found["degraded"]}
    results = reconcile(gcp, env, found["artifacts"], now)
    out = summary(results, found, pol["days"])
    topic = (env.observability or {}).get("events_topic")
    if emit:
        for r in results:
            if r["status"] in ("renewed", "failed"):
                kind = "model.lock.renewed" if r["status"] == "renewed" else "model.lock.failed"
                events.emit(events.event(kind, r.get("usecase"), env.name, None, None, None, actor=gcp.identity(),
                                         **{k: v for k, v in r.items() if k != "usecase"}), topic, gcp,
                            region=env.region)
        events.emit(events.event("model.lock.reconciliation", None, env.name, None, None, None, **out), topic, gcp,
                    region=env.region)
    log(f"lock renewal: {out['status']}: {out['protected']} protected, {out['renewed']} renewed, {out['failed']} failed"
        + (f", least protection left {out['least_protection_remaining_days']} days"
           if out['least_protection_remaining_days'] is not None else ""))
    return {**out, "results": results}


def protect_now(gcp, env, approved: dict | None, log=print) -> list[str]:
    """Lock on activation: before a version becomes active (deploy, promote, rollback), its approved files get at
    least the threshold of protection, synchronously. Returns problems (the caller enforces them under refuse)."""
    from pipewright.models import integrity

    pol = integrity.policy(env)["lock"]
    if pol["days"] <= 0 or not approved:
        return []
    now = datetime.now(timezone.utc)
    arts: dict = {}
    _files(arts, approved, "model_file", "activation", None, approved.get("version"))
    problems = []
    for r in reconcile(gcp, env, list(arts.values()), now):
        if r["status"] in ("failed", "unprotected"):
            problems.append(f"{r['object'].rsplit('/', 1)[-1]}: not protected: {r.get('reason')}")
    if not problems:
        log(f"integrity: {len(arts)} approved files protected for at least {threshold_days(pol['days']):g} days")
    return problems


# -- the cross-identity check (the launcher, read-only, watches the renewer's work) ---------------------------------

def observe(gcp, env, state: dict, models: dict | None = None) -> list[dict]:
    """Run by the launcher's watch, not the renewer: each protected artifact's temporary hold and metageneration,
    compared with the last run. A hold that disappeared, or a metadata change no model.lock.renewed explains, is
    reported (the renewer's role technically allows both: the spike showed storage.objects.update is needed). A
    compromised renewer cannot hide its own changes, because another identity reports them. Updates `state` in
    place; returns model.integrity payloads."""
    from pipewright.models import approvals

    found = discover(gcp, env, models)
    previous = state.get("protected") or {}
    renewed = {}
    for e in approvals.all_events(gcp, env, "model.lock.renewed"):
        key = f"{e.get('bucket')}/{e.get('object')}#{e.get('generation')}"
        renewed[key] = max(renewed.get(key, ""), (e.get("at") or "").replace(" ", "T"))
    since = state.get("observed_at") or ""
    now, changes = {}, []
    for art in found["artifacts"]:
        r = _meta(gcp, art)
        if not r.ok:
            continue
        key = f"{art.bucket}/{art.object}#{art.generation}"
        meta = r.body or {}
        now[key] = {"metageneration": str(meta.get("metageneration")), "hold": bool(meta.get("temporaryHold")),
                    "event_hold": bool(meta.get("eventBasedHold"))}
        was = previous.get(key)
        if not was:
            continue
        base = {"usecase": art.usecase, "version": art.version, "where": "protection", "kind": art.type,
                "object": f"gs://{art.bucket}/{art.object}", "generation": art.generation, "severity": "incident"}
        if (was["hold"] and not now[key]["hold"]) or (was.get("event_hold") and not now[key]["event_hold"]):
            changes.append({**base, "status": "hold_released", "previous": "held",
                            "problems": ["a hold on a protected file was released"]})
        elif was["metageneration"] != now[key]["metageneration"] and renewed.get(key, "") <= since:
            changes.append({**base, "status": "metadata_changed", "previous": was["metageneration"],
                            "problems": [f"metageneration {was['metageneration']} -> {now[key]['metageneration']} "
                                         "with no model.lock.renewed recorded"]})
    from datetime import datetime, timezone

    state["protected"] = now
    state["observed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    return changes
