"""Serving beyond one endpoint per model (Pipewright 1.4.2): endpoints the platform creates in Terraform, private
endpoints over Private Service Connect (FR-92) and model co-hosting on shared deployment resource pools (FR-105).

Everything here is off until the platform turns it on (manifest `serving`) and a use case opts in
(`model.serving.pool`). A use case without either deploys exactly as before.
"""
