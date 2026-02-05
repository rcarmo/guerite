# Configuration Reference

This document provides a complete reference of all environment variables used to configure Guerite.

## Docker Connection

| Variable      | Default                       | Description             |
| ------------- | ----------------------------- | ----------------------- |
| `DOCKER_HOST` | `unix://var/run/docker.sock` | Docker endpoint to use. |

## General Settings

| Variable              | Default                   | Description                                                           |
| --------------------- | ------------------------- | --------------------------------------------------------------------- |
| `GUERITE_TZ`          | `UTC`                     | Time zone used to evaluate cron expressions.                          |
| `GUERITE_LOG_LEVEL`   | `INFO`                    | Log level (e.g., `DEBUG`, `INFO`).                                    |
| `GUERITE_STATE_FILE`  | `/tmp/guerite_state.json` | Path to persist health backoff state across restarts; must be writable. |
| `GUERITE_DRY_RUN`     | `false`                   | If `true`, log actions without restarting containers.                 |
| `GUERITE_PRUNE_CRON`  | unset                     | Cron expression to periodically prune unused images. When unset, pruning is skipped. |
| `GUERITE_MONITOR_ONLY` | `false`                 | If `true`, do not restart or recreate containers (monitor-only).       |
| `GUERITE_NO_PULL`      | `false`                 | If `true`, skip pulling images for update checks.                      |
| `GUERITE_NO_RESTART`   | `false`                 | If `true`, skip restarts/recreates for updates or health.              |
| `GUERITE_ROLLING_RESTART` | `false`             | If `true`, only one update/recreate per compose project per cycle.     |
| `GUERITE_STOP_TIMEOUT_SECONDS` | unset          | Optional stop timeout (seconds) when stopping old containers.          |
| `GUERITE_RUN_ONCE`     | `false`                 | If `true`, perform one cycle and exit.                                 |
| `GUERITE_HTTP_API`     | `false`                 | Enable the HTTP API.                                                   |
| `GUERITE_HTTP_API_HOST` | `0.0.0.0`              | Bind address for the HTTP API.                                         |
| `GUERITE_HTTP_API_PORT` | `8080`                 | Bind port for the HTTP API.                                            |
| `GUERITE_HTTP_API_TOKEN` | unset                 | Optional bearer token for the HTTP API.                                |
| `GUERITE_HTTP_API_METRICS` | `false`            | If `true`, expose `/v1/metrics`.                                       |
| `GUERITE_SCOPE`        | unset                  | Optional scope value to filter monitored containers.                   |
| `GUERITE_SCOPE_LABEL`  | `guerite.scope`        | Label key used for scope matching.                                     |
| `GUERITE_INCLUDE_CONTAINERS` | unset            | Comma/space list of container names to include.                        |
| `GUERITE_EXCLUDE_CONTAINERS` | unset            | Comma/space list of container names to exclude.                        |

## Label Configuration

| Variable                     | Default               | Description                                                                                                          |
| ---------------------------- | --------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `GUERITE_UPDATE_LABEL`       | `guerite.update`      | Label key containing cron expressions that schedule image update checks.                                             |
| `GUERITE_RESTART_LABEL`      | `guerite.restart`     | Label key containing cron expressions that schedule in-place restarts (without pulling).                             |
| `GUERITE_RECREATE_LABEL`     | `guerite.recreate`    | Label key containing cron expressions that schedule forced container recreation (without pulling).                   |
| `GUERITE_HEALTH_CHECK_LABEL` | `guerite.health_check` | Label key containing cron expressions that schedule health checks/restarts.                                          |
| `GUERITE_DEPENDS_LABEL`      | `guerite.depends_on`  | Label key listing dependencies (comma-delimited base names) to gate and order restarts within a project.            |
| `GUERITE_MONITOR_ONLY_LABEL` | `guerite.monitor_only` | Per-container override for monitor-only mode.                                                                        |
| `GUERITE_NO_PULL_LABEL`      | `guerite.no_pull`     | Per-container override to skip image pulls.                                                                          |
| `GUERITE_NO_RESTART_LABEL`   | `guerite.no_restart`  | Per-container override to skip restarts/recreates.                                                                   |
| `GUERITE_SCOPE_LABEL`        | `guerite.scope`       | Label key used to scope containers for filtering.                                                                    |
| `GUERITE_PRE_CHECK_LABEL`    | `guerite.lifecycle.pre_check` | Label key for pre-check lifecycle hook command.                                                              |
| `GUERITE_PRE_UPDATE_LABEL`   | `guerite.lifecycle.pre_update` | Label key for pre-update lifecycle hook command.                                                             |
| `GUERITE_POST_UPDATE_LABEL`  | `guerite.lifecycle.post_update` | Label key for post-update lifecycle hook command.                                                           |
| `GUERITE_POST_CHECK_LABEL`   | `guerite.lifecycle.post_check` | Label key for post-check lifecycle hook command.                                                             |
| `GUERITE_PRE_UPDATE_TIMEOUT_LABEL` | `guerite.lifecycle.pre_update_timeout_seconds` | Label key for pre-update hook timeout (seconds).    |
| `GUERITE_POST_UPDATE_TIMEOUT_LABEL` | `guerite.lifecycle.post_update_timeout_seconds` | Label key for post-update hook timeout (seconds).  |

## Timing and Behavior

| Variable                               | Default | Description                                                                                              |
| -------------------------------------- | ------- | -------------------------------------------------------------------------------------------------------- |
| `GUERITE_HEALTH_CHECK_BACKOFF_SECONDS` | `300`   | Minimum seconds between health-based restarts per container.                                             |
| `GUERITE_HEALTH_CHECK_TIMEOUT_SECONDS` | `60`    | Maximum seconds to wait for a recreated container to become `healthy` before triggering rollback.        |
| `GUERITE_PRUNE_TIMEOUT_SECONDS`        | `180`   | Docker API timeout (in seconds) used for image pruning. |
| `GUERITE_ACTION_COOLDOWN_SECONDS`      | `60`    | Minimum seconds between actions on the same container name to avoid repeated triggers in a short window. |
| `GUERITE_ROLLBACK_GRACE_SECONDS`       | `3600`  | Keep temporary rollback containers/images for at least this many seconds before allowing prune cleanup.  |
| `GUERITE_RESTART_RETRY_LIMIT`          | `3`     | Maximum consecutive restart/recreate attempts before backing off harder for that container.              |
| `GUERITE_HOOK_TIMEOUT_SECONDS`         | `60`    | Default timeout (seconds) for lifecycle hook execution.                                                    |

## Notifications

| Variable                 | Default                                    | Description                                                                                                                             |
| ------------------------ | ------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------- |
| `GUERITE_NOTIFICATIONS`  | `update`                                   | Comma-delimited list of events to notify on: `update`, `restart`, `recreate`, `health`/`health_check`, `startup`, `detect`, `prune`, `all`. |
| `GUERITE_PUSHOVER_TOKEN` | unset                                      | Pushover app token; required to send Pushover notifications.                                                                            |
| `GUERITE_PUSHOVER_USER`  | unset                                      | Pushover user/group key; required to send Pushover notifications.                                                                       |
| `GUERITE_PUSHOVER_API`   | `https://api.pushover.net/1/messages.json` | Pushover endpoint override.                                                                                                             |
| `GUERITE_WEBHOOK_URL`    | unset                                      | If set, sends JSON `{ "title": ..., "message": ... }` POSTs to this URL for enabled events.                                             |
| `GUERITE_LIFECYCLE_HOOKS` | `false`                                   | Enable lifecycle hooks for labeled containers.                                                                                          |
