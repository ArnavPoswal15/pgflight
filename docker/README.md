# Docker dev databases

`docker-compose.yml` runs two Postgres 17 instances with the same seeded `shop` workload:

| Service            | Host port | Superuser URL                                              |
|--------------------|-----------|------------------------------------------------------------|
| `postgres-primary` | 5432      | `postgresql://postgres:postgres@127.0.0.1:5432/shop`     |
| `postgres-shadow`  | 5434      | `postgresql://postgres:postgres@127.0.0.1:5434/shop`     |

The shadow instance also creates a **SELECT-only** login used by `pgflightd` and `pg-shadow`:

```text
postgresql://pgflight_shadow:pgflight_shadow@127.0.0.1:5434/shop
```

Example:

```bash
pgflightd --listen 127.0.0.1:5433 --upstream 127.0.0.1:5432 \
  --shadow-url 'postgresql://pgflight_shadow:pgflight_shadow@127.0.0.1:5434/shop'
```

Init order on **shadow only**: `seed.sql` then `shadow-init.sql` (role + grants). Primary loads `seed.sql` only.

## Reset shadow between A/B replays

Drop indexes/planner experiments and restore baseline data:

```bash
./scripts/shadow-reset.sh
```

Optional volume recreate (slower, full container reset):

```bash
./scripts/shadow-reset.sh --recreate-volume
```

Warm-up before replay (stabilize caches / planner):

```bash
./scripts/shadow-warmup.sh 5
# or: ./scripts/shadow-reset.sh --warmup 5
```

Apply schema changes on shadow as **postgres** (e.g. `CREATE INDEX` for plan compare), then run `shadow-reset.sh` before the next labeled replay.