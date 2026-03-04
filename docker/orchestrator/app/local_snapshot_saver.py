from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from app.config import LocalSnapshotConfig
from app.models import NodeHealth
from app.node_client import NodeClient

if TYPE_CHECKING:
    from app.watchdog import Watchdog

logger = logging.getLogger(__name__)

# Maximum time to wait for a snapshot save to finish.
_SAVE_WAIT_TIMEOUT = 120
_SAVE_POLL_INTERVAL = 2

# Health states in which a periodic snapshot save should be skipped.
# Saving during these states would delay a watchdog restart.
_UNHEALTHY_STATES = frozenset({
    NodeHealth.STUCK,
    NodeHealth.MISALIGNED,
    NodeHealth.CRASHED,
    NodeHealth.EPOCH_BEHIND,
    NodeHealth.VERSION_INCOMPATIBLE,
})


class LocalSnapshotSaver:
    """Periodically trigger local snapshot saves (equivalent to pressing F8).

    This runs in normal mode to ensure the node's state is regularly saved
    to disk, providing recovery points in case of crashes or restarts.
    """

    def __init__(
        self,
        config: LocalSnapshotConfig,
        node_client: NodeClient,
        watchdog: Watchdog | None = None,
    ) -> None:
        self._config = config
        self._node_client = node_client
        self._watchdog = watchdog
        self._last_save_time: float = 0

    def set_watchdog(self, watchdog: Watchdog) -> None:
        """Attach the watchdog after construction (breaks circular init)."""
        self._watchdog = watchdog

    async def run(self, shutdown_event: asyncio.Event) -> None:
        """Main loop: periodically request snapshot saves."""
        logger.info(
            f"Local snapshot saver started "
            f"(interval={self._config.interval_seconds}s)"
        )

        while not shutdown_event.is_set():
            try:
                await asyncio.wait_for(
                    shutdown_event.wait(),
                    timeout=self._config.interval_seconds,
                )
                # Shutdown requested
                return
            except asyncio.TimeoutError:
                # Interval elapsed, trigger save
                pass

            await self._trigger_save()

    async def save_and_wait(self) -> bool:
        """Trigger a snapshot save and wait for it to complete.

        Used by the orchestrator shutdown sequence to ensure state is
        persisted before the node process is stopped.

        Returns True if the save completed successfully.
        """
        try:
            if not await self._node_client.is_alive():
                logger.warning(
                    "Node API not reachable, cannot save state"
                )
                return False

            tick_info = await self._node_client.get_tick_info()

            # If already saving, just wait for it
            if not tick_info.is_saving_snapshot:
                logger.info(
                    f"Requesting snapshot save before shutdown "
                    f"(epoch={tick_info.epoch}, tick={tick_info.tick})"
                )
                success = await self._node_client.request_save_snapshot()
                if not success:
                    logger.warning("Snapshot save request failed")
                    return False
            else:
                logger.info(
                    "Node is already saving a snapshot, waiting for it"
                )

            # Poll until the save finishes
            elapsed = 0
            while elapsed < _SAVE_WAIT_TIMEOUT:
                await asyncio.sleep(_SAVE_POLL_INTERVAL)
                elapsed += _SAVE_POLL_INTERVAL
                try:
                    info = await self._node_client.get_tick_info()
                    if not info.is_saving_snapshot:
                        self._last_save_time = time.monotonic()
                        logger.info("Snapshot save completed")
                        return True
                except Exception:
                    # Node may become unresponsive during heavy save
                    break

            logger.warning(
                f"Snapshot save did not complete within "
                f"{_SAVE_WAIT_TIMEOUT}s"
            )
            return False

        except Exception as e:
            logger.warning(f"Failed to save state: {e}")
            return False

    def _is_node_healthy(self) -> bool:
        """Check whether the watchdog considers the node healthy enough
        for a snapshot save.  Returns True when no watchdog is attached.
        """
        if self._watchdog is None:
            return True
        return self._watchdog.state.health not in _UNHEALTHY_STATES

    async def _trigger_save(self) -> None:
        """Request the node to save a snapshot."""
        try:
            # Skip save if the watchdog has flagged the node as unhealthy
            # (e.g. misaligned, stuck).  Saving would delay the restart.
            if not self._is_node_healthy():
                logger.debug(
                    f"Skipping snapshot save: node is "
                    f"{self._watchdog.state.health.value}"
                )
                return

            # Check if node is already saving
            tick_info = await self._node_client.get_tick_info()
            if tick_info.is_saving_snapshot:
                logger.debug("Node is already saving a snapshot, skipping")
                return

            logger.info(
                f"Triggering local snapshot save "
                f"(epoch={tick_info.epoch}, tick={tick_info.tick})"
            )

            success = await self._node_client.request_save_snapshot()
            if success:
                self._last_save_time = time.monotonic()
                logger.info("Local snapshot save requested successfully")
            else:
                logger.warning("Local snapshot save request returned non-ok status")

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Failed to trigger local snapshot save: {e}")

    @property
    def last_save_time(self) -> float:
        return self._last_save_time
