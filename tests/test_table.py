"""Tests table.py.

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

from pandas.util.testing import assert_frame_equal

from tests.mock_objects import *


class RecordRefreshColumn(FlatColumn):
    """Just a flat column but records calls to refresh for testing."""

    def __init__(self, name: ColumnName, table: Table):
        super().__init__(name, table)

        self.refresh_calls = list()

    def refresh(self) -> None:
        self.refresh_calls.append(self.table.need_refresh[self.name])


class TestTable(unittest.TestCase):

    def setUp(self) -> None:
        # Make a basic table, with columns A and B, with B dependent on A.
        self.table = MockTable(TEST_PREFIX)
        self.col_a = RecordRefreshColumn("A", self.table)
        self.col_b = RecordRefreshColumn("B", self.table)
        self.col_a._column_dependencies.add("B")
        self.table.add_column(self.col_a)
        self.table.add_column(self.col_b)

    def test_basic_setup(self):
        self.assertDictEqual(self.table.need_refresh, {"A": set(), "B": set()})
        self.assertCountEqual(self.table.cm.refresh_order, ["A", "B"])

    def test_add_column_marks_refresh(self):
        # Make new cell addresses; no dependencies
        self.table.set_cell_value(CellAddr(1, "B"), "key", 100)
        self.table.set_cell_value(CellAddr(2, "B"), "key", 100)

        # Then add a column
        col_c = FlatColumn("C", self.table)
        self.table.add_column(col_c)
        self.assertDictEqual(self.table.need_refresh, {
            "A": set(),
            "B": set(),
            "C": {CellAddr(1, "C"), CellAddr(2, "C")}
        })

    def test_set_cell_value_mark_refresh(self):
        self.table.set_cell_value(CellAddr(1, "A"), "key", 100)
        self.table.set_cell_value(CellAddr(2, "A"), "key", 100)

        self.assertDictEqual(self.table.need_refresh, {
            "A": set(),
            "B": {CellAddr(1, "B"), CellAddr(2, "B")}
        })

    def test_set_cell_value_should_update_components(self):
        self.table.set_cell_value(CellAddr(1, "A"), "key", 100)
        self.table.set_cell_value(CellAddr(2, "A"), "key", 100)

        self.assertListEqual(self.table.ds.dates, [1, 2])
        self.assertDictEqual(self.table.ds.dates_keys, {1: {"key"}, 2: {"key"}})

        self.assertEqual(self.table.get_cell_value(CellAddr(1, "A"), "key"),
                         100)

    def test_set_cell_value_on_non_column_fails(self):
        with self.assertRaises(KeyError):
            self.table.set_cell_value(CellAddr(1, "C"), "key", 100)

    def test_get_cell_value_should_fail_if_not_available(self):
        self.table.set_cell_value(CellAddr(500, "A"), "key", 100)

        # This should work
        self.assertEqual(self.table.get_cell_value(CellAddr(500, "A"), "key",
                                                   check_date_availability=True),
                         100)
        # And these.
        self.assertEqual(
            self.table.get_cell_value(CellAddr(500, "A"), "key",
                                      assert_available_on=500,
                                      check_date_availability=True), 100)
        self.assertEqual(
            self.table.get_cell_value(CellAddr(500, "A"), "key",
                                      assert_available_on=501,
                                      check_date_availability=True), 100)
        # But this fails
        with self.assertRaises(KeyError):
            self.table.get_cell_value(CellAddr(500, "A"), "key",
                                      assert_available_on=499,
                                      check_date_availability=True)

    def test_refresh_refreshes(self):
        self.table.set_cell_value(CellAddr(1, "A"), "key", 100)
        self.table.set_cell_value(CellAddr(2, "A"), "key", 100)

        # B needs a refresh
        self.assertDictEqual(self.table.need_refresh, {
            "A": set(),
            "B": {CellAddr(1, "B"), CellAddr(2, "B")}
        })

        # So refresh
        self.table.refresh()

        # Column B should have gotten update calls.
        self.assertDictEqual(self.table.need_refresh, {"A": set(), "B": set()})
        self.assertListEqual(self.col_a.refresh_calls, [])
        self.assertListEqual(self.col_b.refresh_calls,
                             [{CellAddr(1, "B"), CellAddr(2, "B")}])

    def test_make_df(self):
        # Add a third column first.
        col_c = FlatColumn("C", self.table)
        self.table.add_column(col_c)

        self.table.set_cell_value(CellAddr(1, "A"), "key", 100)
        self.table.set_cell_value(CellAddr(2, "A"), "key", 200)
        self.table.set_cell_value(CellAddr(3, "A"), "key", 300)
        self.table.set_cell_value(CellAddr(1, "B"), "key", 400)
        self.table.set_cell_value(CellAddr(2, "B"), "key", 500)
        self.table.set_cell_value(CellAddr(3, "B"), "key", 600)
        self.table.set_cell_value(CellAddr(1, "C"), "key", 700)
        self.table.set_cell_value(CellAddr(2, "C"), "key", 800)
        self.table.set_cell_value(CellAddr(3, "C"), "key", 900)

        expected_df = pd.DataFrame({
            "A": [100, 200, 300],
            "B": [400, 500, 600],
            "C": [700, 800, 900],
        })

        assert_frame_equal(self.table.make_df(columns=["A", "B", "C"]),
                           expected_df)

    def test_make_partial_df(self):
        # Add a third column first.
        col_c = FlatColumn("C", self.table)
        self.table.add_column(col_c)

        self.table.set_cell_value(CellAddr(1, "A"), "key", 100)
        self.table.set_cell_value(CellAddr(2, "A"), "key", 200)
        self.table.set_cell_value(CellAddr(3, "A"), "key", 300)
        self.table.set_cell_value(CellAddr(1, "B"), "key", 400)
        self.table.set_cell_value(CellAddr(2, "B"), "key", 500)
        self.table.set_cell_value(CellAddr(3, "B"), "key", 600)
        self.table.set_cell_value(CellAddr(1, "C"), "key", 700)
        self.table.set_cell_value(CellAddr(2, "C"), "key", 800)
        self.table.set_cell_value(CellAddr(3, "C"), "key", 900)

        expected_df = pd.DataFrame({
            "A": [100, 300],
            "C": [700, 900],
        })

        assert_frame_equal(self.table.make_df(columns=["A", "C"], dates=[1, 3]),
                           expected_df)
