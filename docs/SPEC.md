# Guerite

![Keeping guard over your containers](guerite-256.png)

Guerite is a small Docker container management tool written in Python that watches for changes to the images of running containers with a specific label and pulls and restarts those containers when their base images are updated.

It is inspired by Watchtower but, like a Guerite (a small fortification), it aims to be minimalistic and focused on a specific task without unnecessary complexity.

## Features

- Minimal code base for easy understanding and maintenance.
- Small container footprint (minimal Alpine base image with only the required Python runtime).
- Talks to the local Docker daemon directly via the socket, but can also connect to remote Docker hosts.
- Checks for image updates and notifies users via Pushover when new images are pulled and containers are restarted.
- Containers to be monitored are identified by labels: `guerite.update` for image update checks, `guerite.restart` for scheduled in-place restarts, `guerite.recreate` for scheduled container recreation, and `guerite.health_check` for scheduled health checks that trigger restarts when the container is not `healthy` (rate-limited by a configurable backoff). The labels carry cron expressions (e.g., `guerite.update: "*/10 * * * *"`). Optional dependency ordering uses Docker `Links` and/or a `guerite.depends_on` label so supporting services are handled before their dependents; actions are skipped when declared dependencies are down or unhealthy.
- Watches for new containers that are started with the appropriate label.
- Configurable via environment variables for Pushover integration.
- Notifications can be enabled per event type (update/restart/health/startup) via `GUERITE_NOTIFICATIONS`.
- Health-based restarts only apply to containers that define a Docker healthcheck. Containers with a health label but no healthcheck are skipped with a warning.
- Health restart backoff is persisted across process restarts to avoid rapid restart loops.
- Bind mounts are preflight-checked for missing host paths, and non-local volume drivers log warnings before a recreate.
- Optional cron-driven image pruning removes unused images; failures are logged and can be notified.
- New monitored containers trigger a detect notification, batched to at most one per minute when enabled.
- Monitor-only and no-restart modes allow detecting updates without recreating containers.
- No-pull mode skips image pulls during update checks.
- Optional rolling restart limits updates to one container per compose project per cycle.
- HTTP API can trigger on-demand runs and expose optional Prometheus-style metrics.

## Configuration

| Variable                               | Default                                    | Description                                                                                                                                                            |
| -------------------------------------- | ------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `DOCKER_HOST`                          | `unix://var/run/docker.sock`               | Docker endpoint to use.                                                                                                                                                |
| `GUERITE_UPDATE_LABEL`                 | `guerite.update`                           | Label key containing cron expressions that schedule image update checks.                                                                                               |
| `GUERITE_RESTART_LABEL`                | `guerite.restart`                          | Label key containing cron expressions that schedule in-place restarts (without pulling).                                                                               |
| `GUERITE_RECREATE_LABEL`               | `guerite.recreate`                         | Label key containing cron expressions that schedule forced container recreation (swap to a newly created container without pulling).                                   |
| `GUERITE_HEALTH_CHECK_LABEL`           | `guerite.health_check`                     | Label key containing cron expressions that schedule health checks/restarts.                                                                                            |
| `GUERITE_HEALTH_CHECK_BACKOFF_SECONDS` | `300`                                      | Minimum seconds between health-based restarts per container.                                                                                                           |
| `GUERITE_HEALTH_CHECK_TIMEOUT_SECONDS` | `60`                                       | Maximum seconds to wait for a recreated container to become `healthy` before triggering rollback.                                                                      |
| `GUERITE_STATE_FILE`                   | `/tmp/guerite_state.json`                  | Path to persist health backoff timing across restarts.                                                                                                                 |
| `GUERITE_PRUNE_CRON`                   | unset                                      | Cron expression to periodically prune unused images (non-dangling only). When unset, pruning is skipped.                                                               |
| `GUERITE_NOTIFICATIONS`                | `update`                                   | Comma-delimited list of events to notify via Pushover; accepted values: `update`, `restart`, `recreate`, `health`/`health_check`, `startup`, `detect`, `prune`, `all`. |
| `GUERITE_RESTART_RETRY_LIMIT`          | `3`                                        | Max consecutive restart/recreate attempts before extended backoff.                                                                                                     |
| `GUERITE_DEPENDS_LABEL`                | `guerite.depends_on`                       | Label key listing dependencies (comma list of base names).                                                                                                             |
| `GUERITE_ACTION_COOLDOWN_SECONDS`      | `60`                                       | Minimum seconds between actions on the same container name to avoid rapid repeat triggers.                                                                             |
| `GUERITE_TZ`                           | `UTC`                                      | Time zone used to evaluate cron expressions.                                                                                                                           |
| `GUERITE_DRY_RUN`                      | `false`                                    | If `true`, log actions without restarting containers.                                                                                                                  |
| `GUERITE_LOG_LEVEL`                    | `INFO`                                     | Log level (e.g., `DEBUG`, `INFO`).                                                                                                                                     |
| `GUERITE_PUSHOVER_TOKEN`               | unset                                      | Pushover app token; required to send Pushover notifications.                                                                                                           |
| `GUERITE_PUSHOVER_USER`                | unset                                      | Pushover user/group key; required to send Pushover notifications.                                                                                                      |
| `GUERITE_PUSHOVER_API`                 | `https://api.pushover.net/1/messages.json` | Pushover endpoint override.                                                                                                                                            |
| `GUERITE_MONITOR_ONLY`                 | `false`                                    | If `true`, do not restart or recreate containers (monitor-only).                                                                                                       |
| `GUERITE_NO_PULL`                      | `false`                                    | If `true`, skip pulling images for update checks.                                                                                                                      |
| `GUERITE_NO_RESTART`                   | `false`                                    | If `true`, skip restarts/recreates for updates or health.                                                                                                              |
| `GUERITE_ROLLING_RESTART`              | `false`                                    | If `true`, only one update/recreate per compose project per cycle.                                                                                                     |
| `GUERITE_STOP_TIMEOUT_SECONDS`         | unset                                      | Optional stop timeout (seconds) when stopping old containers.                                                                                                          |
| `GUERITE_RUN_ONCE`                      | `false`                                   | If `true`, perform one cycle and exit.                                                                                                                                 |
| `GUERITE_HTTP_API`                     | `false`                                    | Enable the HTTP API.                                                                                                                                                   |
| `GUERITE_HTTP_API_HOST`                | `0.0.0.0`                                  | Bind address for the HTTP API.                                                                                                                                         |
| `GUERITE_HTTP_API_PORT`                | `8080`                                     | Bind port for the HTTP API.                                                                                                                                            |
| `GUERITE_HTTP_API_TOKEN`               | unset                                      | Optional bearer token for the HTTP API.                                                                                                                                |
| `GUERITE_HTTP_API_METRICS`             | `false`                                    | If `true`, expose `/v1/metrics`.                                                                                                                                       |
| `GUERITE_SCOPE`                        | unset                                      | Optional scope value to filter monitored containers.                                                                                                                   |
| `GUERITE_SCOPE_LABEL`                  | `guerite.scope`                            | Label key used for scope matching.                                                                                                                                     |
| `GUERITE_INCLUDE_CONTAINERS`           | unset                                      | Comma/space list of container names to include.                                                                                                                        |
| `GUERITE_EXCLUDE_CONTAINERS`           | unset                                      | Comma/space list of container names to exclude.                                                                                                                        |
| `GUERITE_HOOK_TIMEOUT_SECONDS`         | `60`                                       | Default timeout (seconds) for lifecycle hook execution.                                                                                                                |
| `GUERITE_LIFECYCLE_HOOKS`              | `false`                                    | Enable lifecycle hooks for labeled containers.                                                                                                                         |
| `GUERITE_PRE_CHECK_LABEL`              | `guerite.lifecycle.pre_check`              | Label key for pre-check lifecycle hook command.                                                                                                                        |
| `GUERITE_PRE_UPDATE_LABEL`             | `guerite.lifecycle.pre_update`             | Label key for pre-update lifecycle hook command.                                                                                                                       |
| `GUERITE_POST_UPDATE_LABEL`            | `guerite.lifecycle.post_update`            | Label key for post-update lifecycle hook command.                                                                                                                      |
| `GUERITE_POST_CHECK_LABEL`             | `guerite.lifecycle.post_check`             | Label key for post-check lifecycle hook command.                                                                                                                       |
| `GUERITE_PRE_UPDATE_TIMEOUT_LABEL`     | `guerite.lifecycle.pre_update_timeout_seconds` | Label key for pre-update hook timeout (seconds).                                                                                                                  |
| `GUERITE_POST_UPDATE_TIMEOUT_LABEL`    | `guerite.lifecycle.post_update_timeout_seconds` | Label key for post-update hook timeout (seconds).                                                                                                                 |

## Notifications

- Update: sent when an image is pulled and the container is restarted (if `GUERITE_NOTIFICATIONS` includes `update`); failures to pull are also reported when update notifications are enabled.
- Restart: sent on cron-driven restarts when `GUERITE_NOTIFICATIONS` includes `restart`; restart failures are also reported when enabled.
- Recreate: sent on cron-driven recreation when `GUERITE_NOTIFICATIONS` includes `recreate`; recreate failures are also reported when enabled.
- Health: sent on health-check-driven restarts when `GUERITE_NOTIFICATIONS` includes `health`/`health_check`; restart failures are reported when enabled.
- Detect: sent when new monitored containers appear; batched to at most one notification per minute when `detect` is enabled.
- Prune: sent when cron-driven image pruning runs or fails if `GUERITE_NOTIFICATIONS` includes `prune`.

Special value:

- `all`: enables all notification categories.

## Dependency ordering

- Express dependencies with Docker `Links` and/or a `guerite.depends_on` label (override key via `GUERITE_DEPENDS_LABEL`).
- Containers are grouped by `com.docker.compose.project` and ordered topologically using those dependencies.
- Guerite skips acting on a container if any declared dependency is missing, stopped, or unhealthy.
- Compose `depends_on` is not exposed by Docker; use `guerite.depends_on` to mirror those relationships.

Example labels:

- `guerite.depends_on=db,cache`
- `guerite.update=*/10 * * * *`

## HTTP API

When `GUERITE_HTTP_API=true`, Guerite exposes:

- `POST /v1/update` to trigger an immediate cycle.
- `GET /v1/metrics` (when `GUERITE_HTTP_API_METRICS=true`) for Prometheus-style metrics.

If `GUERITE_HTTP_API_TOKEN` is set, requests must include `Authorization: Bearer <token>`.

## Lifecycle hooks

If `GUERITE_LIFECYCLE_HOOKS=true`, Guerite executes hook commands inside containers:

- Pre-check (`guerite.lifecycle.pre_check`) before evaluating updates.
- Pre-update (`guerite.lifecycle.pre_update`) before recreate/update.
- Post-update (`guerite.lifecycle.post_update`) after recreate/update.
- Post-check (`guerite.lifecycle.post_check`) after the cycle.

Hook failures are logged and do not stop the update flow.
