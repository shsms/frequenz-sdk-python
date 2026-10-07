# License: MIT
# Copyright © 2026 Frequenz Energy-as-a-Service GmbH

"""Tests for tracking whether inverters follow their power setpoints."""

from datetime import datetime, timedelta, timezone

from frequenz.client.common.microgrid.components import ComponentId
from frequenz.quantities import Power

from frequenz.sdk.microgrid._power_distributing._component_managers._setpoint_tracker import (  # noqa: E501
    SetpointTracker,
)

INV = ComponentId(8)
T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
LOWER = Power.from_kilowatts(-100)
UPPER = Power.from_kilowatts(100)


def _tracker() -> SetpointTracker:
    return SetpointTracker(
        grace_period=timedelta(seconds=10),
        retry_interval=timedelta(seconds=30),
        relative_tolerance=0.1,
        min_tolerance=Power.from_kilowatts(1),
        settle_window=timedelta(seconds=5),
    )


def _set(tracker: SetpointTracker, setpoint_kw: float, seconds: float) -> None:
    tracker.set_setpoint(
        INV, Power.from_kilowatts(setpoint_kw), now=T0 + timedelta(seconds=seconds)
    )


def _following(tracker: SetpointTracker, measured_kw: float, seconds: float) -> bool:
    return tracker.is_following(
        INV,
        measured=Power.from_kilowatts(measured_kw),
        lower=LOWER,
        upper=UPPER,
        now=T0 + timedelta(seconds=seconds),
    )


def test_without_setpoint_is_following() -> None:
    """An inverter that was never commanded is following."""
    tracker = _tracker()
    assert _following(tracker, 50, 0)
    assert _following(tracker, 50, 100)


def test_within_tolerance_is_following() -> None:
    """Deviations within the tolerance never count."""
    tracker = _tracker()
    _set(tracker, -50, 0)
    # 10% of 50 kW is 5 kW.
    for seconds in range(0, 60, 5):
        assert _following(tracker, -45.5, seconds)

    # The minimum tolerance applies to small setpoints.
    _set(tracker, 2, 60)
    assert _following(tracker, 2.9, 70)
    assert _following(tracker, 2.9, 100)


def test_deviating_past_grace_period_is_not_following() -> None:
    """An inverter deviating for longer than the grace period is not following."""
    tracker = _tracker()
    _set(tracker, -50, 0)
    assert _following(tracker, 0, 0)
    assert _following(tracker, 0, 9)
    assert not _following(tracker, 0, 10)
    assert not _following(tracker, 0, 20)


def test_new_setpoints_dont_restart_grace_period() -> None:
    """Changing the setpoint doesn't give a stuck inverter a new grace period."""
    tracker = _tracker()
    _set(tracker, -50, 0)
    assert _following(tracker, 0, 0)
    _set(tracker, -40, 5)
    assert _following(tracker, 0, 5)
    _set(tracker, -60, 9)
    assert not _following(tracker, 0, 10)


def test_catching_up_resets_grace_period() -> None:
    """Reaching the setpoint before the grace period ends resets it."""
    tracker = _tracker()
    _set(tracker, -50, 0)
    assert _following(tracker, 0, 0)
    assert _following(tracker, -50, 8)
    assert _following(tracker, 0, 9)
    assert _following(tracker, 0, 18)
    assert not _following(tracker, 0, 19)


def test_setpoint_is_clamped_to_bounds() -> None:
    """An inverter that stops at its bounds is following."""
    tracker = _tracker()
    _set(tracker, -50, 0)
    assert _following(tracker, 0, 0)
    for seconds in (10, 20):
        assert tracker.is_following(
            INV,
            measured=Power.from_kilowatts(-20),
            lower=Power.from_kilowatts(-20),
            upper=UPPER,
            now=T0 + timedelta(seconds=seconds),
        )


def test_not_following_recovers_when_reaching_setpoint() -> None:
    """An inverter that is not following recovers when it reaches its setpoint."""
    tracker = _tracker()
    _set(tracker, -50, 0)
    assert _following(tracker, 0, 0)
    assert not _following(tracker, 0, 10)
    assert _following(tracker, -50, 15)
    # It gets a full grace period again.
    assert _following(tracker, 0, 16)
    assert _following(tracker, 0, 25)
    assert not _following(tracker, 0, 26)


def test_not_following_is_retried_after_retry_interval() -> None:
    """An inverter that is not following gets retried with a new grace period."""
    tracker = _tracker()
    _set(tracker, -50, 0)
    assert _following(tracker, 0, 0)
    assert not _following(tracker, 0, 10)
    assert not _following(tracker, 0, 39)
    assert _following(tracker, 0, 40)
    assert _following(tracker, 0, 49)
    assert not _following(tracker, 0, 50)


def test_lagging_inverter_is_following() -> None:
    """An inverter that reaches each setpoint a request late is following."""
    tracker = _tracker()
    setpoints = [35.0, 65.0]
    _set(tracker, setpoints[0], -2)
    _set(tracker, setpoints[1], -1)
    for seconds in range(60):
        # The inverter is at the setpoint from two requests ago.
        assert _following(tracker, setpoints[-2], seconds)
        setpoints.append(65.0 if seconds % 2 else 35.0)
        _set(tracker, setpoints[-1], seconds)


def test_stuck_at_an_old_setpoint_is_not_following() -> None:
    """An inverter that stays at an old setpoint is not following the new ones."""
    tracker = _tracker()
    _set(tracker, 50, 0)
    assert _following(tracker, 50, 0)
    for seconds in range(1, 16):
        _set(tracker, -50 if seconds % 2 else -40, seconds)
        # The old setpoint leaves the settle window after 5 s, then the grace
        # period starts.
        assert _following(tracker, 50, seconds + 0.5)
    assert not _following(tracker, 50, 16.5)


def test_reset_forgets_setpoints_and_deviation() -> None:
    """After a reset, an inverter is following until it gets new setpoints."""
    tracker = _tracker()
    _set(tracker, -50, 0)
    assert _following(tracker, 0, 0)
    tracker.reset(INV)
    assert _following(tracker, 0, 300)
    _set(tracker, -50, 300)
    assert _following(tracker, 0, 301)
    assert _following(tracker, 0, 310)
    assert not _following(tracker, 0, 311)


def test_setpoints_are_bounded_without_checks() -> None:
    """Old setpoints are dropped even when the inverter is never checked."""
    tracker = _tracker()
    for seconds in range(100):
        _set(tracker, -50, seconds)
    # pylint: disable-next=protected-access
    assert len(tracker._states[INV].setpoints) <= 7


def test_replaced_setpoints_are_dropped_without_new_ones() -> None:
    """A setpoint replaced long ago is dropped even if no new setpoints arrive."""
    tracker = _tracker()
    _set(tracker, 50, 0)
    _set(tracker, 0, 1)
    # The inverter stays at the replaced setpoint, which leaves the settle window
    # after 5 s. Then the grace period starts.
    for seconds in range(2, 16):
        assert _following(tracker, 50, seconds)
    assert not _following(tracker, 50, 16)
