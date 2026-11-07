"""Choosing the online feature store (FR-104, CONTEXT section 8.19): BigQuery with BI Engine, Cloud SQL for PostgreSQL
with Pipewright's sync and lookup service, or a team's own store over the `http` contract, next to the Bigtable online
store of Pipewright 1.1. Everything here is off until a use case sets `model.serving.online_store`."""

TYPES = ("bigtable", "bigquery", "cloudsql", "http")
