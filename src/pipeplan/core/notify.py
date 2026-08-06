"""Run notifications (failure alerts, SLA breaches).

The orchestration block declares notification *channels*; each is dispatched
through a :class:`Notifier`. The built-in :class:`LoggingNotifier` writes to the
``pipeplan`` logger and is always available, so a pipeline is fully runnable with
no external integration. Real Slack/email/PagerDuty channels are registered
through the ``pipeplan.notifiers`` entry-point group and selected by the
channel's ``type``.
"""

from __future__ import annotations

import logging
from typing import Protocol, runtime_checkable

logger = logging.getLogger("pipeplan.notify")


@runtime_checkable
class Notifier(Protocol):
    def notify(self, event: str, message: str, target: str | None = None) -> None: ...


class LoggingNotifier:
    """Default notifier: emits a warning-level log line per event."""

    def notify(self, event: str, message: str, target: str | None = None) -> None:
        dest = f" -> {target}" if target else ""
        logger.warning("[notify:%s%s] %s", event, dest, message)


class CompositeNotifier:
    """Fan a notification out to several channel notifiers."""

    def __init__(self, channels: list[tuple[str, Notifier, str | None]]) -> None:
        # (channel_type, notifier, target)
        self._channels = channels

    def notify(self, event: str, message: str, target: str | None = None) -> None:
        for _type, notifier, chan_target in self._channels:
            try:
                notifier.notify(event, message, chan_target)
            except Exception:  # pragma: no cover - a broken channel must not abort
                logger.exception("notifier channel %r failed", _type)


def build_notifier(channels: list[dict]) -> CompositeNotifier:
    """Construct a composite notifier from orchestration channel configs.

    Unknown channel types fall back to the logging notifier so misconfiguration
    degrades gracefully rather than silencing alerts.
    """
    from .registry import NOTIFIERS

    built: list[tuple[str, Notifier, str | None]] = []
    for chan in channels:
        ctype = chan.get("type", "log")
        target = chan.get("target")
        try:
            factory = NOTIFIERS.get(ctype)
            notifier = factory()
        except Exception:
            notifier = LoggingNotifier()
        built.append((ctype, notifier, target))
    if not built:
        built.append(("log", LoggingNotifier(), None))
    return CompositeNotifier(built)


# Register the always-available default channel.
from .registry import NOTIFIERS  # noqa: E402

if "log" not in NOTIFIERS._items:  # idempotent on re-import
    NOTIFIERS.register("log", LoggingNotifier)
