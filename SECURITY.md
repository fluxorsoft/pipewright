# Security policy

## Reporting a vulnerability

Please report security issues privately, not in public issues or pull requests.

Use GitHub's private vulnerability reporting: on the repository page, open **Security**, then **Report a
vulnerability**. Include what you found, how to reproduce it, and the version or commit you tested.

We aim to acknowledge reports within three working days and to agree a fix and disclosure timeline with you.

## Supported versions

Security fixes are made on the latest release (1.0.x) and on `main`.

## Scope notes

- Pipewright runs in your own Google Cloud project with your own identities. Never put credentials, tokens or
  service account keys in configuration files, examples or issues; Pipewright never needs them there.
- The launcher is designed to run with IAM-authenticated Cloud Run (and, in private environments, internal ingress
  only). Exposing it without authentication is not a supported configuration.
- Retrain approvals rely on Google Cloud IAM for approver group membership. Anyone with project-wide Pub/Sub publish
  permissions (Owner, Editor, Pub/Sub roles on the project) counts as a member of every approver group; `pipewright
  doctor` names them. See CONTEXT.md, retrain decisions.
