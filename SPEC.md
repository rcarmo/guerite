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
- Notifications can be enabled per event type (update/restart/health) via `GUERITE_NOTIFICATIONS`.
