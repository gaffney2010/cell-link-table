"""Implements a standard topological sort.

Author: T.J. Gaffney (gaffneytj@google.com)

Copyright 2019 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from enum import Enum
from typing import Dict, List, Optional

import attr


class Color(Enum):
    """Used to show step in the DFS.

    When a node is WHITE, it hasn't been visited yet.
    When a node is GRAY, it has been visited, and we are working on its children.
    When a node is BLACK, it has been visited, and so has all its children.
    """
    
    WHITE = 1
    GRAY = 2
    BLACK = 3


@attr.s
class Node(object):
    """Properties of nodes that we calculate during a DFS.

    Attributes:
        start: The step (time) that we first visit a node.
        finish: The step (time) that we last visit a node, after visiting all
            its children.
        color: We color nodes at different points to track where our algorithm
            is at.  See description above for more details about coloring.
    """

    start: Optional[int] = attr.ib(default=None)
    finish: Optional[int] = attr.ib(default=None)
    color: Color = attr.ib(default=Color.WHITE)


def topological(graph: Dict) -> List:
    """Implements a standard topological sort.

    Arguments:
        graph: A dict where the keys are names for the nodes of the graph, and
            the values are sets of the children of that node.  There should be
            one key for each node in the graph; any dependencies that are not
            one of these keys are ignored.
    
    Returns:
        A list of the node names ordered so that each node's child appears
            later in the list than that node.
    
    Raises:
        ValueError: If a loop is detected.
    """

    nodes = {v: Node() for v in graph.keys()}
    time = 0

    def set_start(v):
        """Set the start time on the passed node, and colors GRAY."""

        # Share these variables.
        nonlocal nodes
        nonlocal time

        time += 1
        nodes[v].start = time
        nodes[v].color = Color.GRAY

    def set_finish(v):
        """Set the finish time on the passed node, and colors BLACK."""

        # Share these variables.
        nonlocal nodes
        nonlocal time

        time += 1
        nodes[v].finish = time
        nodes[v].color = Color.BLACK

    def dfs(v):
        """A standard depth-first-search, which sets start and finish times.
        (Only finish time is needed, really.) """
        # Share these variables.
        nonlocal graph
        nonlocal nodes
        nonlocal time

        if nodes[v].color == Color.GRAY:
            raise ValueError("Loop found.")

        set_start(v)
        for dep in graph[v]:
            if dep not in nodes:
                # This is means that the dependency is not a recognized node.
                continue
            if nodes[dep].color != Color.BLACK:
                dfs(dep)
        set_finish(v)

    for v in graph.keys():
        if nodes[v].color == Color.WHITE:
            dfs(v)

    # Sort keys by finish time in descending order.
    key_finishes = [(k, v.finish) for k, v in nodes.items()]
    key_finishes.sort(key=lambda x: -x[1])
    return [x[0] for x in key_finishes]
