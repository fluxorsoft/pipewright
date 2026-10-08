"""Agent Platform Pipelines adapter."""

from __future__ import annotations

import logging

from pipewright.release.contract import PipelineTarget, ReleaseContract

log = logging.getLogger("pipewright.launcher")


class AlreadySubmitted(Exception):
    """The deterministic job ID already exists: a duplicate delivery of the same trigger."""


def _experiment(contract: ReleaseContract, target: PipelineTarget) -> str | None:
    """Training runs are grouped in an Agent Platform experiment. A failure here never blocks the run."""
    if not target.experiment:
        return None
    from google.cloud import aiplatform

    try:
        aiplatform.Experiment.get_or_create(target.experiment, project=contract.project, location=contract.region)
        return target.experiment
    except Exception as e:  # noqa: BLE001
        log.warning("experiment %s unavailable, submitting without it: %s", target.experiment, e)
        return None


def submit(contract: ReleaseContract, target: PipelineTarget, parameters: dict, job_id: str, labels: dict,
           security: dict | None = None) -> str:
    from google.api_core import exceptions as gexc
    from google.cloud import aiplatform

    security = security or {}
    job = aiplatform.PipelineJob(
        display_name=job_id,
        template_path=target.template_uri,
        job_id=job_id,
        pipeline_root=contract.pipeline_root,
        parameter_values=parameters,
        enable_caching=None,          # per-step caching comes from the template (spec.steps.<step>.cache)
        labels=labels,
        encryption_spec_key_name=security.get("kms_key"),
        project=contract.project,
        location=contract.region,
    )
    try:
        # Private profiles: the job's workers peer with the environment's VPC (Private Services Access).
        job.submit(service_account=contract.runner_service_account, network=security.get("network"),
                   reserved_ip_ranges=security.get("reserved_ip_ranges") or None,
                   experiment=_experiment(contract, target))
    except gexc.AlreadyExists as e:
        raise AlreadySubmitted(job_id) from e
    except RuntimeError as e:
        # The SDK creates the job, then joins it to the experiment; joining can fail (for example in a new project
        # whose experiment is not ready yet). The run is already going: report it, never fail it (retrying would only
        # meet AlreadyExists).
        try:
            name = job.resource_name
        except Exception:  # noqa: BLE001 - the job was not created: the original error stands
            raise e from None
        log.warning("%s submitted, but not joined to experiment %s: %s", name, target.experiment, e)
        return name
    return job.resource_name
