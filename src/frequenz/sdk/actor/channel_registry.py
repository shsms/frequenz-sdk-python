"""A class that would dynamically create, own and provide access to channels.

Copyright
Copyright © 2022 Frequenz Energy-as-a-Service GmbH

License
MIT
"""

from typing import Any, Dict

from frequenz.channels import Broadcast, Receiver, Sender


class ChannelRegistry:
    """Dynamically creates, own and provide access to channels.

    It can be used by actors to dynamically establish a communication channel
    between each other.  Channels are identified by string names.
    """

    def __init__(self, *, name: str) -> None:
        """Create a `ChannelRegistry` instance.

        Args:
            name: A unique name for the registry.
        """
        self._name = name
        self._channels: Dict[str, Broadcast[Any]] = {}

    def get_sender(self, key: str) -> Sender[Any]:
        """Get a sender to a dynamically created channel with the given key.

        Args:
            key: A key to identify the channel.

        Returns:
            A sender to a dynamically created channel with the given key.
        """
        if key not in self._channels:
            self._channels[key] = Broadcast(f"{self._name}-{key}")
        return self._channels[key].get_sender()

    def get_receiver(self, key: str) -> Receiver[Any]:
        """Get a receiver to a dynamically created channel with the given key.

        Args:
            key: A key to identify the channel.

        Returns:
            A receiver for a dynamically created channel with the given key.
        """
        if key not in self._channels:
            self._channels[key] = Broadcast(f"{self._name}-{key}")
        return self._channels[key].get_receiver()
