# Guerite

Guerite is a small Docker container management tool written in Python that watches for changes to the images of running containers with a specific label and pulls and restarts those containers when their base images are updated.

It is inspired by Watchtower but, like a Guerite (a small fortification), it aims to be minimalistic and focused on a specific task without unnecessary complexity.

## Features

- Minimal code base for easy understanding and maintenance.
- Small container footprint (minimal Alpine base image with only the required Python runtime).
- Talks to the local Docker daemon directly via the socket, but can also connect to remote Docker hosts.
- Checks for image updates and notifies users via Pushover when new images are pulled and containers are restarted.
- Containers to be monitored are identified by labels: `guerite.update` for image update checks, `guerite.restart` for scheduled restarts, and `guerite.health_check` for scheduled health checks that trigger restarts when the container is not `healthy` (rate-limited by a configurable backoff). The update/restart/health labels carry cron expressions (e.g., `guerite.update: "*/10 * * * *"`).
- Watches for new containers that are started with the appropriate label.
- Configurable via environment variables for Pushover integration.
- Notifications can be enabled per event type (update/restart/health/startup) via `GUERITE_NOTIFICATIONS`.
- Health-based restarts only apply to containers that define a Docker healthcheck. Containers with a health label but no healthcheck are skipped with a warning.
- Health restart backoff is persisted across process restarts to avoid rapid restart loops.
- Bind mounts are preflight-checked for missing host paths, and non-local volume drivers log warnings before a recreate.
- Optional cron-driven image pruning removes unused images; failures are logged and can be notified.
- New monitored containers trigger a detect notification, batched to at most one per minute when enabled.

## Configuration

- `DOCKER_HOST` (default `unix://var/run/docker.sock`): Docker endpoint to use.
- `GUERITE_UPDATE_LABEL` (default `guerite.update`): Label key containing cron expressions that schedule image update checks.
- `GUERITE_RESTART_LABEL` (default `guerite.restart`): Label key containing cron expressions that schedule forced restarts (without pulling).
- `GUERITE_HEALTH_CHECK_LABEL` (default `guerite.health_check`): Label key containing cron expressions that schedule health checks/restarts.
- `GUERITE_HEALTH_CHECK_BACKOFF_SECONDS` (default `300`): Minimum seconds between health-based restarts per container.
- `GUERITE_STATE_FILE` (default `/tmp/guerite_state.json`): Path to persist health backoff timing across restarts.
- `GUERITE_PRUNE_CRON` (default unset): Cron expression to periodically prune unused images (non-dangling only). When unset, pruning is skipped.
- `GUERITE_NOTIFICATIONS` (default `update`): Comma-delimited list of events to notify via Pushover; accepted values: `update`, `restart`, `health`/`health_check`, `startup`, `detect`, `prune`.
- `GUERITE_TZ` (default `UTC`): Time zone used to evaluate cron expressions.
- `GUERITE_DRY_RUN` (default `false`): If `true`, log actions without restarting containers.
- `GUERITE_LOG_LEVEL` (default `INFO`): Log level (e.g., `DEBUG`, `INFO`).
- `GUERITE_PUSHOVER_TOKEN` / `GUERITE_PUSHOVER_USER`: Enable notifications when both are set.
- `GUERITE_PUSHOVER_API` (default `https://api.pushover.net/1/messages.json`): Pushover endpoint override.

## Notifications

- Update: sent when an image is pulled and the container is restarted (if `GUERITE_NOTIFICATIONS` includes `update`); failures to pull are also reported when update notifications are enabled.
- Restart: sent on cron-driven restarts when `GUERITE_NOTIFICATIONS` includes `restart`; restart failures are also reported when enabled.
- Health: sent on health-check-driven restarts when `GUERITE_NOTIFICATIONS` includes `health`/`health_check`; restart failures are reported when enabled.
- Detect: sent when new monitored containers appear; batched to at most one notification per minute when `detect` is enabled.
- Prune: sent when cron-driven image pruning runs or fails if `GUERITE_NOTIFICATIONS` includes `prune`.
