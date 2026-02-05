from http.client import HTTPConnection
from http.client import HTTPSConnection
from json import dumps
from logging import getLogger
from urllib.parse import urlencode, urlsplit

from .config import DEFAULT_NOTIFICATION_TIMEOUT_SECONDS, Settings

LOG = getLogger(__name__)

_NOTIFICATION_TIMEOUT = DEFAULT_NOTIFICATION_TIMEOUT_SECONDS


def notify_pushover(settings: Settings, title: str, message: str) -> None:
    if settings.pushover_token is None or settings.pushover_user is None:
        LOG.debug("Pushover disabled; missing token or user")
        return

    endpoint = urlsplit(settings.pushover_api)
    connection = None
    body = urlencode(
        {
            "token": settings.pushover_token,
            "user": settings.pushover_user,
            "title": title,
            "message": message,
        }
    ).encode("ascii")
    path = endpoint.path or "/"
    if endpoint.query:
        path = f"{path}?{endpoint.query}"

    try:
        connection = HTTPSConnection(endpoint.netloc, timeout=_NOTIFICATION_TIMEOUT)
        connection.request(
            "POST", path, body=body, headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        response = connection.getresponse()
        if response.status >= 300:
            LOG.warning("Pushover returned %s: %s", response.status, response.reason)
    except OSError as error:
        LOG.warning("Failed to send Pushover notification: %s", error)
    finally:
        if connection is not None:
            connection.close()


def notify_webhook(settings: Settings, title: str, message: str) -> None:
    if settings.webhook_url is None:
        LOG.debug("Webhook disabled; missing URL")
        return

    endpoint = urlsplit(settings.webhook_url)
    connection = None
    body = dumps({"title": title, "message": message}).encode("utf-8")
    path = endpoint.path or "/"
    if endpoint.query:
        path = f"{path}?{endpoint.query}"

    try:
        if endpoint.scheme == "https":
            connection = HTTPSConnection(endpoint.netloc, timeout=_NOTIFICATION_TIMEOUT)
        else:
            connection = HTTPConnection(endpoint.netloc, timeout=_NOTIFICATION_TIMEOUT)
        connection.request(
            "POST", path, body=body, headers={"Content-Type": "application/json"}
        )
        response = connection.getresponse()
        if response.status >= 300:
            LOG.warning("Webhook returned %s: %s", response.status, response.reason)
    except OSError as error:
        LOG.warning("Failed to send webhook notification: %s", error)
    finally:
        if connection is not None:
            connection.close()
