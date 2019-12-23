"""Tests topological_sort.

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

import unittest

from helpers.topological_sort import *

TEST_PREFIX = "test_prefix"
TEST_COL = "test_col"
KEY = "test_key"


class TestTopologicalSort(unittest.TestCase):

    def assert_partial_order(self, graph: Dict, result: List):
        """Asserts that each node/child appear in that order in result."""

        def is_order(x_label, y_label):
            """Returns true only if x_label comes before y_label."""
            for node in result:
                if node == y_label:
                    return False
                if node == x_label:
                    return True
            # Neither label found.  Should never happen.
            raise

        for node, deps in graph.items():
            for dep in deps:
                self.assertTrue(is_order(node, dep))

    def test_cycle_detections(self):
        graph = {
            "a": {"b"},
            "b": {"c"},
            "c": {"a"},
        }

        self.assertRaises(ValueError, lambda: topological(graph))

    def test_socks(self):
        # This is the example given in CLRS.
        graph = {
            "shirt": {"belt", "tie"},
            "tie": {"jacket"},
            "jacket": {},
            "belt": {"jacket"},
            "watch": {},
            "undershorts": {"pants", "shoes"},
            "pants": {"belt", "shoes"},
            "shoes": {},
            "socks": {"shoes"}
        }

        sorted_nodes = topological(graph)

        self.assertSetEqual(set(sorted_nodes),
                            {"shirt", "tie", "jacket", "belt", "watch",
                             "undershorts", "pants", "shoes", "socks"})

        self.assert_partial_order(graph, sorted_nodes)
