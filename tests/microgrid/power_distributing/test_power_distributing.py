# License: MIT
# Copyright © 2023 Frequenz Energy-as-a-Service GmbH

"""Tests power distributor."""

from __future__ import annotations

import asyncio
import math
from collections import abc
from dataclasses import replace
from datetime import timedelta
from typing import TypeVar, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.client.common.microgrid.components import ComponentId
from frequenz.client.microgrid.component import Battery, ComponentCategory
from frequenz.quantities import Power
from pytest_mock import MockerFixture

from frequenz.sdk.microgrid import connection_manager
from frequenz.sdk.microgrid._old_component_data import InverterData
from frequenz.sdk.microgrid._power_distributing import (
    ComponentPoolStatus,
    PowerDistributingActor,
    Request,
)
from frequenz.sdk.microgrid._power_distributing._component_managers._battery_manager import (
    BatteryManager,
)
from frequenz.sdk.microgrid._power_distributing._component_pool_status_tracker import (
    ComponentPoolStatusTracker,
)
from frequenz.sdk.microgrid._power_distributing.result import (
    Error,
    OutOfBounds,
    PartialFailure,
    PowerBounds,
    Result,
    Success,
)

from ...conftest import SAFETY_TIMEOUT
from ...microgrid.fixtures import _Mocks, _mocks
from ...timeseries.mock_microgrid import MockMicrogrid
from ...utils.graph_generator import GraphGenerator
from .test_battery_distribution_algorithm import (
    Bound,
    Metric,
    battery_msg,
    inverter_msg,
)

# pylint: disable=too-many-lines


T = TypeVar("T")  # Declare type variable


class TestPowerDistributingActor:
    # pylint: disable=protected-access
    # pylint: disable=too-many-public-methods
    """Test tool to distribute power."""

    _namespace = "power_distributor"

    async def _patch_battery_pool_status(
        self,
        mocks: _Mocks,
        mocker: MockerFixture,
        battery_ids: abc.Set[ComponentId] | None = None,
    ) -> None:
        """Patch the battery pool status.

        If `battery_ids` is not None, the mock will always return `battery_ids`.
        Otherwise, it will return the requested batteries.
        """
        if battery_ids:
            mock = MagicMock(spec=ComponentPoolStatusTracker)
            mock.get_working_components.return_value = battery_ids
            mocker.patch(
                "frequenz.sdk.microgrid._power_distributing._component_managers"
                "._battery_manager.ComponentPoolStatusTracker",
                return_value=mock,
            )
        else:
            mock = MagicMock(spec=ComponentPoolStatusTracker)
            mock.get_working_components.side_effect = set
            mocker.patch(
                "frequenz.sdk.microgrid._power_distributing._component_managers"
                "._battery_manager.ComponentPoolStatusTracker",
                return_value=mock,
            )
        await mocks.component_status_sender.send(
            ComponentPoolStatus(
                working=set(mocks.microgrid.battery_ids), uncertain=set()
            )
        )

    async def test_constructor_with_grid_meter(self, mocker: MockerFixture) -> None:
        """Test the constructor works with a grid meter."""
        mockgrid = MockMicrogrid(grid_meter=True, mocker=mocker)
        mockgrid.add_batteries(2)
        mockgrid.add_batteries(1, no_meter=True)

        async with mockgrid:
            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")
            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )
            async with PowerDistributingActor(
                component_type=Battery,
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ) as distributor:
                assert isinstance(distributor._component_manager, BatteryManager)
                assert distributor._component_manager._bat_invs_map == {
                    ComponentId(9): frozenset({ComponentId(8)}),
                    ComponentId(19): frozenset({ComponentId(18)}),
                    ComponentId(29): frozenset({ComponentId(28)}),
                }
                assert distributor._component_manager._inv_bats_map == {
                    ComponentId(8): frozenset({ComponentId(9)}),
                    ComponentId(18): frozenset({ComponentId(19)}),
                    ComponentId(28): frozenset({ComponentId(29)}),
                }

    async def test_constructor_without_grid_meter(self, mocker: MockerFixture) -> None:
        """Test the constructor works without a grid meter."""
        mockgrid = MockMicrogrid(grid_meter=False, mocker=mocker)
        mockgrid.add_batteries(1)
        mockgrid.add_batteries(2, no_meter=True)

        async with mockgrid:
            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")
            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )
            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ) as distributor:
                assert isinstance(distributor._component_manager, BatteryManager)
                assert distributor._component_manager._bat_invs_map == {
                    ComponentId(9): frozenset({ComponentId(8)}),
                    ComponentId(19): frozenset({ComponentId(18)}),
                    ComponentId(29): frozenset({ComponentId(28)}),
                }
                assert distributor._component_manager._inv_bats_map == {
                    ComponentId(8): frozenset({ComponentId(9)}),
                    ComponentId(18): frozenset({ComponentId(19)}),
                    ComponentId(28): frozenset({ComponentId(29)}),
                }

    async def init_component_data(
        self,
        mocks: _Mocks,
        *,
        skip_batteries: abc.Set[ComponentId] | None = None,
        skip_inverters: abc.Set[ComponentId] | None = None,
    ) -> None:
        """Send initial component data, for power distributor to start."""
        for battery_id in set(mocks.microgrid.battery_ids) - (skip_batteries or set()):
            mocks.streamer.start_streaming(
                battery_msg(
                    battery_id,
                    capacity=Metric(98000),
                    soc=Metric(40, Bound(20, 80)),
                    power=PowerBounds(
                        Power.from_watts(-1000),
                        Power.zero(),
                        Power.zero(),
                        Power.from_watts(1000),
                    ),
                ),
                0.05,
            )

        for inverter_id in set(mocks.microgrid.battery_inverter_ids) - (
            skip_inverters or set()
        ):
            mocks.streamer.start_streaming(
                inverter_msg(
                    inverter_id,
                    power=PowerBounds(
                        Power.from_watts(-500),
                        Power.zero(),
                        Power.zero(),
                        Power.from_watts(500),
                    ),
                ),
                0.05,
            )

    async def test_power_distributor_one_user(self, mocker: MockerFixture) -> None:
        """Test if power distribution works with a single user."""
        mocks = await _Mocks.new(ComponentCategory.BATTERY, mocker)
        requests_channel = Broadcast[Request](name="power_distributor requests")
        results_channel = Broadcast[Result](name="power_distributor results")

        request = Request(
            power=Power.from_kilowatts(1.2),
            component_ids={ComponentId(9), ComponentId(19)},
        )

        await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
        await self.init_component_data(mocks)

        battery_status_channel = Broadcast[ComponentPoolStatus](name="battery_status")
        async with PowerDistributingActor(
            Battery,
            requests_receiver=requests_channel.new_receiver(),
            results_sender=results_channel.new_sender(),
            component_pool_status_sender=battery_status_channel.new_sender(),
            api_power_request_timeout=SAFETY_TIMEOUT,
        ):
            await asyncio.sleep(0.1)  # wait for actor to collect data

            await requests_channel.new_sender().send(request)
            result_rx = results_channel.new_receiver()

            async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                result = await result_rx.receive()

        assert isinstance(result, Success)
        assert result.succeeded_power.isclose(Power.from_kilowatts(1.0))
        assert result.excess_power.isclose(Power.from_watts(200.0))
        assert result.request == request

        await mocks.stop()

    async def test_power_distributor_exclusion_bounds(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distributing actor rejects non-zero requests in exclusion bounds."""
        async with _mocks(mocker, ComponentCategory.BATTERY) as mocks:
            await self._patch_battery_pool_status(
                mocks, mocker, {ComponentId(9), ComponentId(19)}
            )
            await self.init_component_data(
                mocks, skip_batteries={ComponentId(9), ComponentId(19)}
            )

            mocks.streamer.start_streaming(
                battery_msg(
                    ComponentId(9),
                    soc=Metric(60, Bound(20, 80)),
                    capacity=Metric(98000),
                    power=PowerBounds(
                        Power.from_watts(-1000),
                        Power.from_watts(-300),
                        Power.from_watts(300),
                        Power.from_watts(1000),
                    ),
                ),
                0.05,
            )

            mocks.streamer.start_streaming(
                battery_msg(
                    ComponentId(19),
                    soc=Metric(60, Bound(20, 80)),
                    capacity=Metric(98000),
                    power=PowerBounds(
                        Power.from_watts(-1000),
                        Power.from_watts(-300),
                        Power.from_watts(300),
                        Power.from_watts(1000),
                    ),
                ),
                0.05,
            )

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")

            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )
            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                # zero power requests should pass through despite the exclusion bounds.
                request = Request(
                    power=Power.zero(),
                    component_ids={ComponentId(9), ComponentId(19)},
                )

                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

                assert isinstance(result, Success)
                assert result.succeeded_power.isclose(Power.zero(), abs_tol=1e-9)
                assert result.excess_power.isclose(Power.zero(), abs_tol=1e-9)
                assert result.request == request

                # non-zero power requests that fall within the exclusion bounds should be
                # rejected.
                request = Request(
                    power=Power.from_watts(300.0),
                    component_ids={ComponentId(9), ComponentId(19)},
                )

                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

                assert isinstance(
                    result, OutOfBounds
                ), f"Expected OutOfBounds, got {result}"
                assert result.bounds == PowerBounds(
                    Power.from_watts(-1000),
                    Power.from_watts(-600),
                    Power.from_watts(600),
                    Power.from_watts(1000),
                )
                assert result.request == request

    # pylint: disable=too-many-locals
    async def test_two_batteries_one_inverters(self, mocker: MockerFixture) -> None:
        """Test if power distribution works with two batteries connected to one inverter."""
        gen = GraphGenerator()
        bat_component1 = gen.component(ComponentCategory.BATTERY)
        bat_component2 = gen.component(ComponentCategory.BATTERY)

        graph = gen.to_graph(
            (
                ComponentCategory.METER,
                [
                    (
                        ComponentCategory.METER,
                        (
                            ComponentCategory.INVERTER,
                            [
                                bat_component1,
                                bat_component2,
                            ],
                        ),
                    ),
                ],
            )
        )

        async with _mocks(mocker, ComponentCategory.BATTERY, graph=graph) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")
            request = Request(
                power=Power.from_watts(1200.0),
                component_ids={
                    bat_component1.id,
                    bat_component2.id,
                },
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)

            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )

            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                results_sender=results_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

                assert isinstance(result, Success)
                # Inverter bounded at 500
                assert result.succeeded_power.isclose(Power.from_watts(500.0))
                assert result.excess_power.isclose(Power.from_watts(700.0))
                assert result.request == request

    async def test_two_batteries_one_broken_one_inverters(
        self, mocker: MockerFixture
    ) -> None:
        """Test power distribution with two batteries (one broken) connected to one inverter."""
        gen = GraphGenerator()
        bat_components = gen.components(*[ComponentCategory.BATTERY] * 2)

        graph = gen.to_graph(
            (
                ComponentCategory.METER,
                [
                    (
                        ComponentCategory.METER,
                        (
                            ComponentCategory.INVERTER,
                            [
                                bat_components[0],
                                bat_components[1],
                            ],
                        ),
                    )
                ],
            )
        )

        async with _mocks(mocker, ComponentCategory.BATTERY, graph=graph) as mocks:
            await self.init_component_data(mocks, skip_batteries={bat_components[0].id})

            mocks.streamer.start_streaming(
                battery_msg(
                    bat_components[0].id,
                    soc=Metric(math.nan, Bound(20, 80)),
                    capacity=Metric(98000),
                    power=PowerBounds(
                        Power.from_watts(-1000),
                        Power.zero(),
                        Power.zero(),
                        Power.from_watts(1000),
                    ),
                ),
                0.05,
            )

            requests_channel = Broadcast[Request](name="power_distributor")
            results_channel = Broadcast[Result](name="power_distributor results")

            request = Request(
                power=Power.from_watts(1200.0),
                component_ids=set(battery.id for battery in bat_components),
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )

            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                results_sender=results_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

                assert isinstance(result, Error)
                assert result.request == request
                assert (
                    result.msg
                    == "No data for at least one of the given batteries: CID9, CID19"
                )

    async def test_battery_two_inverters(self, mocker: MockerFixture) -> None:
        """Test if power distribution works with two inverters for one battery."""
        gen = GraphGenerator()
        bat_component = gen.component(ComponentCategory.BATTERY)
        graph = gen.to_graph(
            (
                ComponentCategory.METER,
                [gen.battery_with_inverter(bat_component, 2)],
            )
        )

        async with _mocks(mocker, ComponentCategory.BATTERY, graph=graph) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")

            request = Request(
                power=Power.from_watts(1200.0),
                component_ids={bat_component.id},
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )

            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                results_sender=results_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

                assert isinstance(result, Success)
                # Inverters each bounded at 500, together 1000
                assert result.succeeded_power.isclose(Power.from_watts(1000.0))
                assert result.excess_power.isclose(Power.from_watts(200.0))
                assert result.request == request

    async def test_two_batteries_three_inverters(self, mocker: MockerFixture) -> None:
        """Test if power distribution works with two batteries connected to three inverters."""
        gen = GraphGenerator()
        batteries = gen.components(*[ComponentCategory.BATTERY] * 2)

        graph = gen.to_graph(
            (ComponentCategory.METER, gen.batteries_with_inverter(batteries, 3))
        )

        async with _mocks(mocker, ComponentCategory.BATTERY, graph=graph) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")

            request = Request(
                power=Power.from_watts(1700.0),
                component_ids={batteries[0].id, batteries[1].id},
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )

            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                results_sender=results_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

                assert isinstance(result, Success)
                # each inverter is bounded at 500 and we have 3 inverters
                assert result.succeeded_power.isclose(Power.from_watts(1500.0))
                assert result.excess_power.isclose(Power.from_watts(200.0))
                assert result.request == request

    async def test_two_batteries_one_inverter_different_exclusion_bounds_2(
        self, mocker: MockerFixture
    ) -> None:
        """Test power distribution with two batteries having different exclusion bounds.

        Test if power distribution works with two batteries connected to one
        inverter, each having different exclusion bounds.
        """
        gen = GraphGenerator()
        batteries = gen.components(*[ComponentCategory.BATTERY] * 2)
        inverter = gen.component(ComponentCategory.INVERTER)

        graph = gen.to_graph(
            (
                ComponentCategory.METER,
                (ComponentCategory.METER, [(inverter, [*batteries])]),
            )
        )

        async with _mocks(mocker, ComponentCategory.BATTERY, graph=graph) as mocks:
            mocks.streamer.start_streaming(
                inverter_msg(
                    inverter.id,
                    power=PowerBounds(
                        Power.from_watts(-1000),
                        Power.from_watts(-500),
                        Power.from_watts(500),
                        Power.from_watts(1000),
                    ),
                ),
                0.05,
            )
            mocks.streamer.start_streaming(
                battery_msg(
                    batteries[0].id,
                    soc=Metric(40, Bound(20, 80)),
                    capacity=Metric(10_000),
                    power=PowerBounds(
                        Power.from_watts(-1000),
                        Power.from_watts(-200),
                        Power.from_watts(200),
                        Power.from_watts(1000),
                    ),
                ),
                0.05,
            )
            mocks.streamer.start_streaming(
                battery_msg(
                    batteries[1].id,
                    soc=Metric(40, Bound(20, 80)),
                    capacity=Metric(10_000),
                    power=PowerBounds(
                        Power.from_watts(-1000),
                        Power.from_watts(-100),
                        Power.from_watts(100),
                        Power.from_watts(1000),
                    ),
                ),
                0.05,
            )

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")

            request = Request(
                power=Power.from_watts(300.0),
                component_ids={batteries[0].id, batteries[1].id},
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )

            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                results_sender=results_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

                assert isinstance(result, OutOfBounds)
                assert result.request == request
                assert result.bounds == PowerBounds(
                    Power.from_watts(-1000),
                    Power.from_watts(-500),
                    Power.from_watts(500),
                    Power.from_watts(1000),
                )

    async def test_two_batteries_one_inverter_different_exclusion_bounds(
        self, mocker: MockerFixture
    ) -> None:
        """Test power distribution with two batteries having different exclusion bounds.

        Test if power distribution works with two batteries connected to one
        inverter, each having different exclusion bounds.
        """
        gen = GraphGenerator()
        batteries = gen.components(*[ComponentCategory.BATTERY] * 2)

        graph = gen.to_graph(
            (
                ComponentCategory.METER,
                (
                    ComponentCategory.METER,
                    [
                        (
                            ComponentCategory.INVERTER,
                            [*batteries],
                        ),
                    ],
                ),
            )
        )

        async with _mocks(mocker, ComponentCategory.BATTERY, graph=graph) as mocks:
            await self.init_component_data(
                mocks, skip_batteries={bat.id for bat in batteries}
            )
            mocks.streamer.start_streaming(
                battery_msg(
                    batteries[0].id,
                    soc=Metric(40, Bound(20, 80)),
                    capacity=Metric(10_000),
                    power=PowerBounds(
                        Power.from_watts(-1000),
                        Power.from_watts(-200),
                        Power.from_watts(200),
                        Power.from_watts(1000),
                    ),
                ),
                0.05,
            )
            mocks.streamer.start_streaming(
                battery_msg(
                    batteries[1].id,
                    soc=Metric(40, Bound(20, 80)),
                    capacity=Metric(10_000),
                    power=PowerBounds(
                        Power.from_watts(-1000),
                        Power.from_watts(-100),
                        Power.from_watts(100),
                        Power.from_watts(1000),
                    ),
                ),
                0.05,
            )

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")

            request = Request(
                power=Power.from_watts(300.0),
                component_ids={batteries[0].id, batteries[1].id},
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )

            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                results_sender=results_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

                assert isinstance(result, OutOfBounds)
                assert result.request == request
                # each inverter is bounded at 500
                assert result.bounds == PowerBounds(
                    Power.from_watts(-500),
                    Power.from_watts(-400),
                    Power.from_watts(400),
                    Power.from_watts(500),
                )

    async def test_connected_but_not_requested_batteries(
        self, mocker: MockerFixture
    ) -> None:
        """Test behavior when an inverter has more batteries connected than requested."""
        gen = GraphGenerator()
        batteries = gen.components(*[ComponentCategory.BATTERY] * 2)

        graph = gen.to_graph(
            (
                ComponentCategory.METER,
                [
                    (
                        ComponentCategory.METER,
                        [
                            (
                                ComponentCategory.INVERTER,
                                [*batteries],
                            ),
                        ],
                    ),
                ],
            )
        )

        async with _mocks(mocker, ComponentCategory.BATTERY, graph=graph) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")

            request = Request(
                power=Power.from_watts(600.0),
                component_ids={batteries[0].id},
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )

            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                results_sender=results_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

                assert isinstance(result, Error)
                assert result.request == request
                assert (
                    result.msg
                    == "Inverter(s) (CID48) are connected to battery(ies) (CID19) that"
                    " were not requested"
                )

    async def test_battery_soc_nan(self, mocker: MockerFixture) -> None:
        """Test if battery with SoC==NaN is not used."""
        async with _mocks(mocker, ComponentCategory.BATTERY, grid_meter=False) as mocks:
            await self.init_component_data(mocks, skip_batteries={ComponentId(9)})

            mocks.streamer.start_streaming(
                battery_msg(
                    ComponentId(9),
                    soc=Metric(math.nan, Bound(20, 80)),
                    capacity=Metric(98000),
                    power=PowerBounds(
                        Power.from_watts(-1000),
                        Power.zero(),
                        Power.zero(),
                        Power.from_watts(1000),
                    ),
                ),
                0.05,
            )

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")

            request = Request(
                power=Power.from_kilowatts(1.2),
                component_ids={ComponentId(9), ComponentId(19)},
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)
            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )
            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

            assert isinstance(result, Success)
            assert result.succeeded_components == {ComponentId(19)}
            assert result.succeeded_power.isclose(Power.from_watts(500.0))
            assert result.excess_power.isclose(Power.from_watts(700.0))
            assert result.request == request

    async def test_battery_capacity_nan(self, mocker: MockerFixture) -> None:
        """Test battery with capacity set to NaN is not used."""
        async with _mocks(mocker, ComponentCategory.BATTERY, grid_meter=False) as mocks:
            await self.init_component_data(mocks, skip_batteries={ComponentId(9)})

            mocks.streamer.start_streaming(
                battery_msg(
                    ComponentId(9),
                    soc=Metric(40, Bound(20, 80)),
                    capacity=Metric(math.nan),
                    power=PowerBounds(
                        Power.from_watts(-1000),
                        Power.zero(),
                        Power.zero(),
                        Power.from_watts(1000),
                    ),
                ),
                0.05,
            )

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")

            request = Request(
                power=Power.from_kilowatts(1.2),
                component_ids={ComponentId(9), ComponentId(19)},
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)

            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )
            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

            assert isinstance(result, Success)
            assert result.succeeded_components == {ComponentId(19)}
            assert result.succeeded_power.isclose(Power.from_watts(500.0))
            assert result.excess_power.isclose(Power.from_watts(700.0))
            assert result.request == request

    async def test_battery_power_bounds_nan(self, mocker: MockerFixture) -> None:
        """Test battery with power bounds set to NaN is not used."""
        async with _mocks(mocker, ComponentCategory.BATTERY, grid_meter=False) as mocks:
            await self.init_component_data(
                mocks,
                skip_batteries={ComponentId(9)},
                skip_inverters={ComponentId(8), ComponentId(18)},
            )

            mocks.streamer.start_streaming(
                inverter_msg(
                    ComponentId(18),
                    power=PowerBounds(
                        Power.from_watts(-1000),
                        Power.zero(),
                        Power.zero(),
                        Power.from_watts(1000),
                    ),
                ),
                0.05,
            )

            # Battery 9 should not work because both battery and inverter sends NaN
            mocks.streamer.start_streaming(
                inverter_msg(
                    ComponentId(8),
                    power=PowerBounds(
                        Power.from_watts(-1000),
                        Power.zero(),
                        Power.zero(),
                        Power.from_watts(math.nan),
                    ),
                ),
                0.05,
            )

            mocks.streamer.start_streaming(
                battery_msg(
                    ComponentId(9),
                    soc=Metric(40, Bound(20, 80)),
                    capacity=Metric(float(98000)),
                    power=PowerBounds(
                        Power.from_watts(math.nan),
                        Power.zero(),
                        Power.zero(),
                        Power.from_watts(math.nan),
                    ),
                ),
                0.05,
            )

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")

            request = Request(
                power=Power.from_kilowatts(1.2),
                component_ids={ComponentId(9), ComponentId(19)},
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)

            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )
            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

            assert isinstance(result, Success)
            assert result.succeeded_components == {ComponentId(19)}
            assert result.succeeded_power.isclose(Power.from_kilowatts(1.0))
            assert result.excess_power.isclose(Power.from_watts(200.0))
            assert result.request == request

    async def test_power_distributor_invalid_battery_id(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distribution raises error if any battery id is invalid."""
        async with _mocks(mocker, ComponentCategory.BATTERY, grid_meter=False) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")
            request = Request(
                power=Power.from_kilowatts(1.2),
                component_ids={ComponentId(9), ComponentId(100)},
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)

            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )
            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

            assert isinstance(result, Error)
            assert result.request == request
            assert (
                result.msg
                == "No battery CID100, available batteries: CID9, CID19, CID29"
            )

    async def test_power_distributor_one_user_adjust_power_consume(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distribution works with single user works."""
        async with _mocks(mocker, ComponentCategory.BATTERY, grid_meter=False) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request](name="power_distributor")
            results_channel = Broadcast[Result](name="power_distributor results")

            request = Request(
                power=Power.from_kilowatts(1.2),
                component_ids={ComponentId(9), ComponentId(19)},
                adjust_power=False,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)

            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )
            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

            assert isinstance(result, OutOfBounds)
            assert result is not None
            assert result.request == request
            assert result.bounds.inclusion_upper == Power.from_watts(1000)

    async def test_power_distributor_one_user_adjust_power_supply(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distribution works with single user works."""
        async with _mocks(mocker, ComponentCategory.BATTERY, grid_meter=False) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")

            request = Request(
                power=-Power.from_kilowatts(1.2),
                component_ids={ComponentId(9), ComponentId(19)},
                adjust_power=False,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)

            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )
            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

            assert isinstance(result, OutOfBounds)
            assert result is not None
            assert result.request == request
            assert result.bounds.inclusion_lower == Power.from_watts(-1000)

    async def test_power_distributor_one_user_adjust_power_success(
        self, mocker: MockerFixture
    ) -> None:
        """Test if power distribution works with single user works."""
        async with _mocks(mocker, ComponentCategory.BATTERY, grid_meter=False) as mocks:
            await self.init_component_data(mocks)

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")

            request = Request(
                power=Power.from_kilowatts(1.0),
                component_ids={ComponentId(9), ComponentId(19)},
                adjust_power=False,
            )

            await self._patch_battery_pool_status(mocks, mocker, request.component_ids)

            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )
            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

            assert isinstance(result, Success)
            assert result.succeeded_power.isclose(Power.from_kilowatts(1.0))
            assert result.excess_power.isclose(Power.zero(), abs_tol=1e-9)
            assert result.request == request

    async def test_not_all_batteries_are_working(self, mocker: MockerFixture) -> None:
        """Test if power distribution works if not all batteries are working."""
        async with _mocks(mocker, ComponentCategory.BATTERY, grid_meter=False) as mocks:
            await self.init_component_data(mocks)

            batteries = {ComponentId(9), ComponentId(19)}

            await self._patch_battery_pool_status(
                mocks, mocker, batteries - {ComponentId(9)}
            )

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")

            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )
            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                request = Request(
                    power=Power.from_kilowatts(1.2),
                    component_ids=batteries,
                )

                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

                assert isinstance(result, Success)
                assert result.succeeded_components == {ComponentId(19)}
                assert result.excess_power.isclose(Power.from_watts(700.0))
                assert result.succeeded_power.isclose(Power.from_watts(500.0))
                assert result.request == request

    def _patch_setpoint_tracking(self, mocker: MockerFixture) -> None:
        """Shorten the grace period and the tolerance of the setpoint tracking."""
        module = (
            "frequenz.sdk.microgrid._power_distributing._component_managers"
            "._battery_manager"
        )
        mocker.patch(f"{module}._SETPOINT_GRACE_PERIOD", timedelta(seconds=0.2))
        mocker.patch(f"{module}._SETPOINT_MIN_TOLERANCE", Power.from_watts(10))

    def _set_inverter_powers(self, mocks: _Mocks, powers: dict[int, float]) -> None:
        """Set the active power the given inverters report."""
        for inverter_id, active_power in powers.items():
            data = mocks.streamer.get_current_component_data(ComponentId(inverter_id))
            assert isinstance(data, InverterData)
            mocks.streamer.update_stream(replace(data, active_power=active_power))

    async def _distribute(
        self,
        requests_tx: Sender[Request],
        result_rx: Receiver[Result],
        request: Request,
    ) -> Result:
        """Send a request to the power distributor and return its result."""
        await requests_tx.send(request)
        async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
            return await result_rx.receive()

    @pytest.mark.parametrize(
        "battery_9_power_w, expected_batteries",
        [
            # Battery 9 follows its setpoint, so only battery 19 is skipped.
            (500.0, {ComponentId(9)}),
            # Neither follows, so both are still used.
            (0.0, {ComponentId(9), ComponentId(19)}),
        ],
    )
    async def test_batteries_not_following_setpoints_are_not_used(
        self,
        mocker: MockerFixture,
        battery_9_power_w: float,
        expected_batteries: set[ComponentId],
    ) -> None:
        """Test that batteries whose inverters don't follow their setpoints are skipped."""
        self._patch_setpoint_tracking(mocker)

        async with _mocks(mocker, ComponentCategory.BATTERY, grid_meter=False) as mocks:
            await self.init_component_data(mocks)

            batteries = {ComponentId(9), ComponentId(19)}
            await self._patch_battery_pool_status(mocks, mocker, batteries)

            # Both batteries get 500 W setpoints. The inverter of battery 19 stays
            # at 0 W.
            self._set_inverter_powers(mocks, {8: battery_9_power_w, 18: 0.0})

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")
            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )
            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                request = Request(
                    power=Power.from_kilowatts(1.0), component_ids=batteries
                )
                requests_tx = requests_channel.new_sender()
                result_rx = results_channel.new_receiver()

                # Both batteries are used during the grace period.
                for _ in range(2):
                    result = await self._distribute(requests_tx, result_rx, request)
                    assert isinstance(result, Success)
                    assert result.succeeded_components == batteries

                await asyncio.sleep(0.3)

                result = await self._distribute(requests_tx, result_rx, request)
                assert isinstance(result, Success)
                assert result.succeeded_components == expected_batteries
                assert result.succeeded_power.isclose(
                    Power.from_watts(500.0 * len(expected_batteries))
                )

    async def test_batteries_get_a_new_grace_period_after_not_working(
        self, mocker: MockerFixture
    ) -> None:
        """Test that batteries coming back from not working get a new grace period."""
        self._patch_setpoint_tracking(mocker)

        async with _mocks(mocker, ComponentCategory.BATTERY, grid_meter=False) as mocks:
            await self.init_component_data(mocks)

            batteries = {ComponentId(9), ComponentId(19)}
            # The status mock returns this set, so changing it changes the status.
            working = set(batteries)
            await self._patch_battery_pool_status(mocks, mocker, working)

            # Both batteries get 500 W setpoints. The inverter of battery 19 stays
            # at 0 W.
            self._set_inverter_powers(mocks, {8: 500.0, 18: 0.0})

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")
            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )
            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                request = Request(
                    power=Power.from_kilowatts(1.0), component_ids=batteries
                )
                requests_tx = requests_channel.new_sender()
                result_rx = results_channel.new_receiver()

                # The second request finds battery 19 deviating.
                for _ in range(2):
                    result = await self._distribute(requests_tx, result_rx, request)
                    assert isinstance(result, Success)
                    assert result.succeeded_components == batteries

                # Battery 19 stops working for longer than the grace period.
                working.discard(ComponentId(19))
                await self._distribute(requests_tx, result_rx, request)
                await asyncio.sleep(0.3)
                working.add(ComponentId(19))

                result = await self._distribute(requests_tx, result_rx, request)
                assert isinstance(result, Success)
                assert result.succeeded_components == batteries

    async def test_zero_power_is_resent_after_not_working(
        self, mocker: MockerFixture
    ) -> None:
        """Test that batteries coming back from not working get 0 W sent again."""
        async with _mocks(mocker, ComponentCategory.BATTERY, grid_meter=False) as mocks:
            await self.init_component_data(mocks)
            set_power = cast(
                AsyncMock,
                connection_manager.get().api_client.set_component_power_active,
            )

            batteries = {ComponentId(9), ComponentId(19)}
            # The status mock returns this set, so changing it changes the status.
            working = set(batteries)
            await self._patch_battery_pool_status(mocks, mocker, working)

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")
            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )
            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                request = Request(power=Power.zero(), component_ids=batteries)
                requests_tx = requests_channel.new_sender()
                result_rx = results_channel.new_receiver()

                await self._distribute(requests_tx, result_rx, request)
                working.discard(ComponentId(19))
                await self._distribute(requests_tx, result_rx, request)
                working.add(ComponentId(19))

                set_power.reset_mock()
                await self._distribute(requests_tx, result_rx, request)
                assert set_power.call_args_list == [mocker.call(ComponentId(18), 0.0)]

    async def test_partial_failure_result(self, mocker: MockerFixture) -> None:
        """Test power results when the microgrid failed to set power for one of the batteries."""
        async with _mocks(mocker, ComponentCategory.BATTERY, grid_meter=False) as mocks:
            await self.init_component_data(mocks)

            batteries = {ComponentId(9), ComponentId(19), ComponentId(29)}
            failed_batteries = {ComponentId(9)}
            failed_power = Power.from_watts(500.0)

            await self._patch_battery_pool_status(mocks, mocker, batteries)

            mocker.patch(
                "frequenz.sdk.microgrid._power_distributing._component_managers"
                "._battery_manager.BatteryManager._parse_result",
                return_value=(failed_power, failed_batteries),
            )

            requests_channel = Broadcast[Request](name="power_distributor requests")
            results_channel = Broadcast[Result](name="power_distributor results")

            battery_status_channel = Broadcast[ComponentPoolStatus](
                name="battery_status"
            )
            async with PowerDistributingActor(
                Battery,
                requests_receiver=requests_channel.new_receiver(),
                results_sender=results_channel.new_sender(),
                component_pool_status_sender=battery_status_channel.new_sender(),
                api_power_request_timeout=SAFETY_TIMEOUT,
            ):
                await asyncio.sleep(0.1)  # wait for actor to collect data

                request = Request(
                    power=Power.from_kilowatts(1.70),
                    component_ids=batteries,
                )

                await requests_channel.new_sender().send(request)
                result_rx = results_channel.new_receiver()

                async with asyncio.timeout(SAFETY_TIMEOUT.total_seconds()):
                    result = await result_rx.receive()

                assert isinstance(result, PartialFailure)
                assert result.succeeded_components == batteries - failed_batteries
                assert result.failed_components == failed_batteries
                assert result.succeeded_power.isclose(Power.from_watts(1000.0))
                assert result.failed_power.isclose(failed_power)
                assert result.excess_power.isclose(Power.from_watts(200.0))
                assert result.request == request
