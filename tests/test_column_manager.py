"""Tests for column_manager.

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

from tests.mock_objects import *


class TestDateSet(unittest.TestCase):

    def setUp(self) -> None:
        self.fake_fs = dict()
        self.table = MockTable(TEST_PREFIX, self.fake_fs)
        self.table.open()

    def assertDictEqualMod(self, x, y):
        """Strip away primary_key and convert to regular dict."""
        self.assertDictEqual(
            {k: v for k, v in x.items() if k != "primary_key"},
            {k: v for k, v in y.items() if k != "primary_key"}
        )

    def assertListEqualMod(self, x, y):
        """Strip away primary_key."""
        self.assertListEqual(
            [t for t in x if t != "primary_key"],
            [t for t in y if t != "primary_key"]
        )

    def test_basic_add_column(self):
        self.col_a = Column("A", self.table)
        self.assertSetEqual(self.table.cm.save_needed, {"A"})
        self.assertEqual(self.table.cm.prefix, TEST_PREFIX)
        self.assertDictEqualMod(self.table.cm._columns, {"A": self.col_a})
        self.assertDictEqualMod(self.table.cm.dependency_graph, {"A": set()})
        self.assertListEqualMod(self.table.cm.refresh_order, ["A"])

    def test_basic_recall(self):
        self.col_a = Column("A", self.table)
        self.table.cm.save_needed = set()

        self.assertEqual(self.table.cm.get_column("A"), self.col_a)

        # Accessing should mark it as save_needed.
        self.assertSetEqual(self.table.cm.save_needed, {"A"})

    def test_save_logic(self):
        self.col_a = Column("A", self.table)
        self.assertSetEqual(self.table.cm.save_needed, {"A"})

        self.table.close()

        self.assertListEqual(self.table.cm.save_log,
                             ["data/column_data/test_prefix-A"])
        self.assertSetEqual(self.table.cm.save_needed, set())

    def test_load_after_save(self):
        self.col_a = Column("A", self.table)
        self.table.close()

        alt_table = MockTable(TEST_PREFIX, self.fake_fs)
        alt_table.open()
        self.assertEqual(alt_table.cm.get_column("A").name, self.col_a.name)

    def test_delay_update_logic(self):
        col_x = Column("X", self.table)
        col_y = Column("Y", self.table)
        col_z = Column("Z", self.table)

        col_z._column_dependencies = {"Y", "X"}
        col_x._column_dependencies = {"Y"}

        # Because the dependencies were added after the columns were attached,
        # we need to update the dependencies. 
        self.assertDictEqualMod(self.table.cm.dependency_graph,
                                {'X': set(), 'Y': set(), 'Z': set()})
        self.table.cm._update_column_dependencies()

        self.assertDictEqualMod(
            self.table.cm.dependency_graph,
            {"X": {"Y"}, "Y": set(), "Z": {"X", "Y"}}
        )
        self.assertListEqualMod(self.table.cm.refresh_order, ["Z", "X", "Y"])
