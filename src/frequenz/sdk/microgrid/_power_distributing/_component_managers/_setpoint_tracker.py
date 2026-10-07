# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Track whether inverters follow the power setpoints sent to them."""

import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from frequenz.client.common.microgrid.components import ComponentId
from frequenz.quantities import Power

_logger = logging.getLogger(__name__)


@dataclass
class _InverterState:
    setpoints: deque[tuple[datetime, Power]] = field(default_factory=deque)
    """The powers requested from the inverter, and when.

    Holds the setpoints requested within the settle window, and the last one before
    it, which was still in force when the window started.
    """

    deviating_since: datetime | None = None
    """When the inverter's power started to deviate from its setpoints."""

    not_following_since: datetime | None = None
    """When the inverter was found not to follow its setpoints."""


class SetpointTracker:
    """Track whether inverters follow the power setpoints sent to them.

    An inverter deviates from its setpoints when its measured power is outside the
    range of the setpoints it was given within the settle window, by more than the
    tolerance.  An inverter that is still on its way between its setpoints is
    within that range.

    An inverter that deviates for longer than the grace period is considered not to be
    following its setpoints.  It is considered to be following again when its power
    reaches them, or after the retry interval, when it gets a new grace period.

    Setpoints are clamped to the inverter's current inclusion bounds before comparing,
    so that inverters limited by their bounds (for example, because of their SoC) are
    not considered to be deviating.
    """

    def __init__(
        self,
        *,
        grace_period: timedelta,
        retry_interval: timedelta,
        relative_tolerance: float,
        min_tolerance: Power,
        settle_window: timedelta,
    ) -> None:
        """Initialize this instance.

        Args:
            grace_period: How long an inverter can deviate from its setpoints before
                it is considered not to be following them.
            retry_interval: How long to wait before giving an inverter that is not
                following its setpoints a new grace period.
            relative_tolerance: The allowed deviation, as a fraction of a setpoint.
            min_tolerance: The minimum allowed deviation, for small setpoints.
            settle_window: How long an inverter can take to reach a setpoint.
        """
        self._grace_period = grace_period
        self._retry_interval = retry_interval
        self._relative_tolerance = relative_tolerance
        self._min_tolerance = min_tolerance
        self._settle_window = settle_window
        self._states: defaultdict[ComponentId, _InverterState] = defaultdict(
            _InverterState
        )

    def set_setpoint(
        self, inverter_id: ComponentId, power: Power, *, now: datetime
    ) -> None:
        """Record the power requested from an inverter.

        Changing the setpoint doesn't restart the grace period, so that an inverter
        that doesn't follow any of its setpoints is detected even when they change
        often.

        Args:
            inverter_id: The inverter's component ID.
            power: The requested power.
            now: The current time.
        """
        setpoints = self._states[inverter_id].setpoints
        setpoints.append((now, power))
        self._drop_old_setpoints(setpoints, now)

    def reset(self, inverter_id: ComponentId) -> None:
        """Forget an inverter's setpoints and how long it has been deviating.

        Args:
            inverter_id: The inverter's component ID.
        """
        self._states.pop(inverter_id, None)

    def is_following(  # pylint: disable=too-many-arguments
        self,
        inverter_id: ComponentId,
        *,
        measured: Power,
        lower: Power,
        upper: Power,
        now: datetime,
    ) -> bool:
        """Check whether an inverter is following its setpoints.

        Args:
            inverter_id: The inverter's component ID.
            measured: The inverter's latest measured active power.
            lower: The inverter's current lower inclusion bound.
            upper: The inverter's current upper inclusion bound.
            now: The current time.

        Returns:
            Whether the inverter is following its setpoints.  Inverters without
                setpoints are always following.
        """
        state = self._states.get(inverter_id)
        if state is None:
            return True

        self._drop_old_setpoints(state.setpoints, now)
        clamped = [min(max(power, lower), upper) for _, power in state.setpoints]
        # The point of the setpoint range closest to the measured power.
        expected = min(max(measured, min(clamped)), max(clamped))
        tolerance = max(self._min_tolerance, abs(expected) * self._relative_tolerance)
        deviating = abs(measured - expected) > tolerance

        if not deviating:
            if state.not_following_since is not None:
                _logger.info(
                    "Inverter %s reached its setpoints at %s, using it again.",
                    inverter_id,
                    expected,
                )
            state.deviating_since = None
            state.not_following_since = None
            return True

        if state.not_following_since is not None:
            if now - state.not_following_since < self._retry_interval:
                return False
            _logger.info(
                "Retrying inverter %s, which was not following its setpoints.",
                inverter_id,
            )
            state.not_following_since = None
            state.deviating_since = None

        if state.deviating_since is None:
            state.deviating_since = now
            return True

        if now - state.deviating_since < self._grace_period:
            return True

        _logger.warning(
            "Inverter %s is not following its setpoints: measured %s, closest "
            "setpoint %s, since %s. Retrying it in %s.",
            inverter_id,
            measured,
            expected,
            state.deviating_since,
            self._retry_interval,
        )
        state.not_following_since = now
        return False

    def _drop_old_setpoints(
        self, setpoints: deque[tuple[datetime, Power]], now: datetime
    ) -> None:
        """Drop the setpoints that were replaced before the settle window started.

        Args:
            setpoints: The setpoints of an inverter, oldest first.
            now: The current time.
        """
        while len(setpoints) > 1 and setpoints[1][0] <= now - self._settle_window:
            setpoints.popleft()
