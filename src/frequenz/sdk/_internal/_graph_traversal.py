# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Graph traversal helpers."""

from __future__ import annotations

from typing import Callable, Iterable

from frequenz.client.microgrid import (
    Component,
    ComponentCategory,
    Connection,
    InverterType,
)
from frequenz.component_graph import ComponentGraph


def is_pv_inverter(component: Component) -> bool:
    """Check if the component is a PV inverter.

    Args:
        component: The component to check.

    Returns:
        `True` if the component is a PV inverter, `False` otherwise.
    """
    return (
        component.category == ComponentCategory.INVERTER
        and component.type == InverterType.SOLAR
    )


def is_battery_inverter(component: Component) -> bool:
    """Check if the component is a battery inverter.

    Args:
        component: The component to check.

    Returns:
        `True` if the component is a battery inverter, `False` otherwise.
    """
    return (
        component.category == ComponentCategory.INVERTER
        and component.type == InverterType.BATTERY
    )


def is_chp(component: Component) -> bool:
    """Check if the component is a CHP.

    Args:
        component: The component to check.

    Returns:
        `True` if the component is a CHP, `False` otherwise.
    """
    return component.category == ComponentCategory.CHP


def is_ev_charger(component: Component) -> bool:
    """Check if the component is an EV charger.

    Args:
        component: The component to check.

    Returns:
        `True` if the component is an EV charger, `False` otherwise.
    """
    return component.category == ComponentCategory.EV_CHARGER


def is_battery_chain(
    graph: ComponentGraph[Component, Connection], component: Component
) -> bool:
    """Check if the specified component is part of a battery chain.

    A component is part of a battery chain if it is either a battery inverter or a
    battery meter.

    Args:
        graph: The component graph.
        component: component to check.

    Returns:
        Whether the specified component is part of a battery chain.
    """
    return is_battery_inverter(component) or graph.is_battery_meter(
        component.component_id
    )


def is_pv_chain(
    graph: ComponentGraph[Component, Connection], component: Component
) -> bool:
    """Check if the specified component is part of a PV chain.

    A component is part of a PV chain if it is either a PV inverter or a PV
    meter.

    Args:
        graph: The component graph.
        component: component to check.

    Returns:
        Whether the specified component is part of a PV chain.
    """
    return is_pv_inverter(component) or graph.is_pv_meter(component.component_id)


def is_ev_charger_chain(
    graph: ComponentGraph[Component, Connection], component: Component
) -> bool:
    """Check if the specified component is part of an EV charger chain.

    A component is part of an EV charger chain if it is either an EV charger or an
    EV charger meter.

    Args:
        graph: The component graph.
        component: component to check.

    Returns:
        Whether the specified component is part of an EV charger chain.
    """
    return is_ev_charger(component) or graph.is_ev_charger_meter(component.component_id)


def is_chp_chain(
    graph: ComponentGraph[Component, Connection], component: Component
) -> bool:
    """Check if the specified component is part of a CHP chain.

    A component is part of a CHP chain if it is either a CHP or a CHP meter.

    Args:
        graph: The component graph.
        component: component to check.

    Returns:
        Whether the specified component is part of a CHP chain.
    """
    return is_chp(component) or graph.is_chp_meter(component.component_id)


def dfs(
    graph: ComponentGraph[Component, Connection],
    current_node: Component,
    visited: set[Component],
    condition: Callable[[Component], bool],
) -> set[Component]:
    """
    Search for components that fulfill the condition in the Graph.

    DFS is used for searching the graph. The graph traversal is stopped
    once a component fulfills the condition.

    Args:
        graph: The component graph.
        current_node: The current node to search from.
        visited: The set of visited nodes.
        condition: The condition function to check for.

    Returns:
        A set of component ids where the corresponding components fulfill
        the condition function.
    """
    if current_node in visited:
        return set()

    visited.add(current_node)

    if condition(current_node):
        return {current_node}

    component: set[Component] = set()

    for successor in graph.successors(current_node.component_id):
        component.update(dfs(graph, successor, visited, condition))

    return component


def find_first_descendant_component(
    graph: ComponentGraph[Component, Connection],
    *,
    root_category: ComponentCategory,
    descendant_categories: Iterable[ComponentCategory],
) -> Component:
    """Find the first descendant component given root and descendant categories.

    This method searches for the root component within the provided root
    category. If multiple components share the same root category, the
    first found one is considered as the root component.

    Subsequently, it looks for the first descendant component from the root
    component, considering only the immediate descendants.

    The priority of the component to search for is determined by the order
    of the descendant categories, with the first category having the
    highest priority.

    Args:
        graph: The component graph.
        root_category: The category of the root component to search for.
        descendant_categories: The descendant categories to search for the
            first descendant component in.

    Raises:
        ValueError: when the root component is not found in the component
            graph or when no component is found in the given categories.

    Returns:
        The first descendant component found in the component graph,
        considering the specified root and descendant categories.
    """
    root_component = next(
        (
            comp
            for comp in filter(
                lambda c: c.category == root_category, graph.components()
            )
        ),
        None,
    )

    if root_component is None:
        raise ValueError(f"Root component not found for {root_category.name}")

    # Sort by component ID to ensure consistent results.
    successors = sorted(
        graph.successors(root_component.component_id),
        key=lambda comp: comp.component_id,
    )

    def find_component(component_category: ComponentCategory) -> Component | None:
        return next(
            (comp for comp in successors if comp.category == component_category),
            None,
        )

    # Find the first component that matches the given descendant categories
    # in the order of the categories list.
    component = next(filter(None, map(find_component, descendant_categories)), None)

    if component is None:
        raise ValueError("Component not found in any of the descendant categories.")

    return component
