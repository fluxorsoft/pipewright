"""AI build assistance (Pipewright 1.3.0): what coding agents use to work with Pipewright safely.

- `identity`: the agent's own identity (a service account the person impersonates) and labelled calls;
- `context`: generated context packs agents read on start;
- `mcp`: the local MCP tool server (`pipewright mcp`);
- `review`: the review bundle for a change (`pipewright review`).

The agent writes, Pipewright verifies, a person decides.
"""
