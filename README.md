# Guerite

> _A guerite is a small, enclosed structure used for temporary or makeshift purposes, while a [watchtower](https://github.com/containrrr/watchtower) is a tall, elevated structure used for permanent or sturdy purposes._

Guerite is a [watchtower](https://github.com/containrrr/watchtower) alternative that watches Docker containers that carry a specific label, pulls their base images when updates appear, and restarts the containers. It talks directly to the Docker API and can reach a local or remote daemon.

## Requirements

- Docker API access (local socket or remote TCP/TLS endpoint)
- Python 3.9+ if running from source; otherwise build the container image
- Optional: Pushover token/user for notifications

## Build the image

Build from the included Dockerfile:

```bash
docker build -t guerite .
```

### Run against the local socket

```bash
docker run --rm \
	-v /var/run/docker.sock:/var/run/docker.sock:ro \
	-e GUERITE_LOG_LEVEL=INFO \
	guerite:latest
```

### Run against a remote daemon (TLS)

Place `ca.pem`, `cert.pem`, and `key.pem` under `./certs` and point `DOCKER_HOST` to the remote engine:

```bash
docker run --rm \
	-e DOCKER_HOST=tcp://remote-docker-host:2376 \
	-e DOCKER_TLS_VERIFY=1 \
	-e DOCKER_CERT_PATH=/certs \
	-v "$PWD"/certs:/certs:ro \
	guerite:latest
```

## Configuration

Set environment variables to adjust behavior:

- `DOCKER_HOST` (default `unix://var/run/docker.sock`): Docker endpoint to use.
- `GUERITE_UPDATE_LABEL` (default `guerite.update`): Label key containing cron expressions that schedule image update checks.
- `GUERITE_RESTART_LABEL` (default `guerite.restart`): Label key containing cron expressions that schedule forced restarts (without pulling).
- `GUERITE_HEALTH_CHECK_LABEL` (default `guerite.health_check`): Label key containing cron expressions that schedule health checks/restarts.
- `GUERITE_HEALTH_CHECK_BACKOFF_SECONDS` (default `300`): Minimum seconds between health-based restarts per container.
- `GUERITE_NOTIFICATIONS` (default `update`): Comma-delimited list of events to notify via Pushover; accepted values: `update`, `restart`, `health` (or `health_check`).
- `GUERITE_TZ` (default `UTC`): Time zone used to evaluate cron expressions.
- `GUERITE_DRY_RUN` (default `false`): If `true`, log actions without restarting containers.
- `GUERITE_LOG_LEVEL` (default `INFO`): Log level (e.g., `DEBUG`, `INFO`).
- `GUERITE_PUSHOVER_TOKEN` / `GUERITE_PUSHOVER_USER`: Enable notifications when both are set.
- `GUERITE_PUSHOVER_API` (default `https://api.pushover.net/1/messages.json`): Pushover endpoint override.

## Container labels

Add labels to any container you want Guerite to manage (any label opts the container in):

- `guerite.update=*/10 * * * *` schedules image pull/update checks and restarts when the image changes.
- `guerite.restart=0 3 * * *` schedules forced restarts at the specified cron times (no image pull).
- `guerite.health_check=*/5 * * * *` runs a health check on the cron schedule; if the container is not `healthy`, it is restarted (rate-limited by the backoff).

Notifications:
- Update: sent when an image is pulled and the container is restarted (if `GUERITE_NOTIFICATIONS` includes `update`).
- Restart: sent on cron-driven restarts when `GUERITE_NOTIFICATIONS` includes `restart`.
- Health: sent on health-check-driven restarts when `GUERITE_NOTIFICATIONS` includes `health`/`health_check`.

## Quick start (local Docker socket)

Use the provided compose file to build and run Guerite against the local daemon:

```bash
docker compose -f docker-compose.local.yml up -d --build
```

This starts Guerite and a sample `nginx` container labeled for monitoring. The daemon socket is mounted read-only.

## Remote daemon over TCP/TLS

Guerite can talk to a remote Docker host via the standard TLS variables. Prepare TLS client certs from the remote daemon and place them under `./certs` (ca.pem, cert.pem, key.pem). Then run:

```bash
docker compose -f docker-compose.remote.yml up -d --build
```

The compose file sets `DOCKER_HOST=tcp://remote-docker-host:2376`, enables TLS verification, and mounts the certs. Adjust the host name and poll interval to your environment.

### Using an SSH tunnel instead of exposing TCP

If you prefer an SSH tunnel, forward the remote socket locally and point `DOCKER_HOST` at the local port:

```bash
ssh -N -L 2376:/var/run/docker.sock user@remote-host
DOCKER_HOST=tcp://localhost:2376 DOCKER_TLS_VERIFY=0 docker compose -f docker-compose.remote.yml up -d --build
```

## Running from source

You can run Guerite without containers:

```bash
pip install -e .
python -m guerite
```

Ensure `DOCKER_HOST` and optional Pushover variables are set in the environment.
