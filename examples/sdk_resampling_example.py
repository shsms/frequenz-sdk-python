# License: MIT
# Copyright © 2022 Frequenz Energy-as-a-Service GmbH

"""Frequenz Python SDK resampling example."""

import asyncio

from frequenz.channels import Broadcast, MergeNamed

from frequenz.sdk.actor import ChannelRegistry
from frequenz.sdk.actor.data_sourcing import DataSourcingActor
from frequenz.sdk.actor.resampling import (
    ComponentMetricId,
    ComponentMetricRequest,
    ComponentMetricsResamplingActor,
)
from frequenz.sdk.microgrid import ComponentCategory, microgrid_api

HOST = "microgrid.sandbox.api.frequenz.io"
PORT = 61060


async def run() -> None:
    """Run main functions that initializes and creates everything."""
    await microgrid_api.initialize(HOST, PORT)

    channel_registry = ChannelRegistry(name="Microgrid Channel Registry")

    # Create a channels for sending/receiving subscription requests
    data_source_request_channel = Broadcast[ComponentMetricRequest](
        "Data Source Request Channel"
    )
    data_source_request_sender = data_source_request_channel.get_sender()
    data_source_request_receiver = data_source_request_channel.get_receiver()

    resampling_actor_request_channel = Broadcast[ComponentMetricRequest](
        "Resampling Actor Request Channel"
    )
    resampling_actor_request_sender = resampling_actor_request_channel.get_sender()
    resampling_actor_request_receiver = resampling_actor_request_channel.get_receiver()

    # Instantiate a data sourcing actor
    _data_sourcing_actor = DataSourcingActor(
        request_receiver=data_source_request_receiver, registry=channel_registry
    )

    # Instantiate a resampling actor
    _resampling_actor = ComponentMetricsResamplingActor(
        channel_registry=channel_registry,
        subscription_sender=data_source_request_sender,
        subscription_receiver=resampling_actor_request_receiver,
        resampling_period_s=1.0,
    )

    components = await microgrid_api.get().microgrid_api_client.components()
    battery_ids = [
        comp.component_id
        for comp in components
        if comp.category == ComponentCategory.BATTERY
    ]

    # Create subscription requests for each time series id
    subscription_requests = [
        ComponentMetricRequest(
            namespace="Resampling",
            component_id=component_id,
            metric_id=ComponentMetricId.SOC,
            start_time=None,
        )
        for component_id in battery_ids
    ]

    # Send the subscription requests
    await asyncio.gather(
        *[
            resampling_actor_request_sender.send(request)
            for request in subscription_requests
        ]
    )

    # Store sample receivers for each subscription
    sample_receiver = MergeNamed(
        **{
            channel_name: channel_registry.get_receiver(channel_name)
            for channel_name in map(
                lambda req: req.get_channel_name(), subscription_requests
            )
        }
    )

    async for channel_name, msg in sample_receiver:
        print(msg)


asyncio.run(run())
