# Retention renewal: which Cloud Storage permissions it needs (FR-101, phase 7)

Evidence for the renewer identity in CONTEXT.md section 8.18: why it holds exactly `storage.objects.get`,
`storage.objects.setRetention` and `storage.objects.update` on model folders, and why it is not the launcher.
Run live on 2026-11-06; the results are in `results-2026-11-06.json` (every result re-read from the object as an
administrator, not taken from the API's answer).

To repeat it: `setup.sh`, then `retention_permissions.py` with the variables setup prints, then `teardown.sh`.
Cost: pennies. The bucket stays for about 90 minutes because of one Locked-mode test object.

| # | Test | 2026-11-06 |
|---|---|---|
| 1 | `get` alone: read metadata in `models/`; outside; extend | read; refused outside; cannot extend |
| 2 | `get` + `setRetention`: extend | refused: needs `storage.objects.update` |
| 3 | + `update`: extend Unlocked | extended |
| 4 | extend Locked to a later date | extended |
| 5 | stale `ifMetagenerationMatch` | 412, unchanged; a fresh one extends |
| 6 | release a temporary hold | refused without `update`; **released with `update`** |
| 7 | shorten or remove retention (also with `overrideUnlockedRetention=true`); Unlocked to Locked | refused |
| 8 | overwrite, delete; change content type and custom metadata | overwrite and delete refused; **metadata changed with `update`** |
| 9 | extend the approved generation while a newer one is live | only the approved generation changed |
| 9b | the same call without `?generation=` | applies to the live version |
| 10 | extend outside `models/` | refused |
