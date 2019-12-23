"""Tests for page_master.

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

TEST_PREFIX = "test_prefix"
TEST_COL = "test_col"
KEY = "test_key"


class TestCellManager(unittest.TestCase):

    def setUp(self) -> None:
        self.cell_addr_1 = CellAddr(20110101, TEST_COL)
        self.cell_addr_2 = CellAddr(20110202, TEST_COL)
        self.cell_addr_2b = CellAddr(20110203, TEST_COL)  # Same page
        self.cell_addr_3 = CellAddr(20110303, TEST_COL)
        self.cell_addr_4 = CellAddr(20110404, TEST_COL)

        self.mcm: MockCellMaster = MockCellMaster(TEST_PREFIX, cache_size=3)

    def test_basic_setup(self):
        self.assertEqual(self.mcm.cache_size, 3)
        self.assertEqual(self.mcm.cache, [])

    def test_cell_key(self):
        self.assertEqual(self.mcm._addr_key(self.cell_addr_1, "key"),
                         "CellAddr(date=20110101, col='test_col'): key")
        self.assertEqual(self.mcm._addr_key(self.cell_addr_2, "alt key"),
                         "CellAddr(date=20110202, col='test_col'): alt key")

    def test_addr_to_page(self):
        self.assertEqual(self.mcm._addr_to_page(self.cell_addr_1),
                         "201101-test_col")
        self.assertEqual(self.mcm._addr_to_page(self.cell_addr_2),
                         "201102-test_col")

    def test_default_value_is_none(self):
        self.assertIsNone(self.mcm.get_value(self.cell_addr_1, KEY))

    def test_repeated_keys_on_single_page(self):
        # Shouldn't overwrite
        self.mcm.set_value(self.cell_addr_2, KEY, "A")
        self.mcm.set_value(self.cell_addr_2b, KEY, "B")
        self.assertEqual(self.mcm.get_value(self.cell_addr_2, KEY), "A")
        self.assertEqual(self.mcm.get_value(self.cell_addr_2b, KEY), "B")

        # But only one page load.
        self.assertListEqual(self.mcm.load_log, [
            'data/cell_files/test_prefix_201102-test_col'
        ])

    def test_distinct_keys_on_single_address(self):
        # Shouldn't overwrite
        self.mcm.set_value(self.cell_addr_1, "KEY1", "A")
        self.mcm.set_value(self.cell_addr_1, "KEY2", "B")
        self.assertEqual(self.mcm.get_value(self.cell_addr_1, "KEY1"), "A")
        self.assertEqual(self.mcm.get_value(self.cell_addr_1, "KEY2"), "B")

    def test_pulling_from_cache(self):
        # Just keep rewriting the same three pages on a length-3
        self.mcm.set_value(self.cell_addr_1, KEY, "A")
        self.mcm.set_value(self.cell_addr_2, KEY, "B")
        self.mcm.set_value(self.cell_addr_3, KEY, "C")
        self.mcm.set_value(self.cell_addr_1, KEY, "D")
        self.mcm.set_value(self.cell_addr_2, KEY, "E")
        self.mcm.set_value(self.cell_addr_3, KEY, "F")
        self.mcm.set_value(self.cell_addr_2, KEY, "G")
        self.mcm.set_value(self.cell_addr_3, KEY, "H")

        # Values match as expected
        self.assertEqual(self.mcm.get_value(self.cell_addr_2, KEY), "G")
        self.assertEqual(self.mcm.get_value(self.cell_addr_3, KEY), "H")
        self.assertEqual(self.mcm.get_value(self.cell_addr_1, KEY), "D")

        self.assertListEqual(self.mcm.load_log, [
            'data/cell_files/test_prefix_201101-test_col',
            'data/cell_files/test_prefix_201102-test_col',
            'data/cell_files/test_prefix_201103-test_col'
        ])
        self.assertListEqual(self.mcm.save_log, [])

    def test_save_and_open(self):
        # Write and overwrite while cycling through cache.
        self.mcm.set_value(self.cell_addr_1, KEY, "A")
        self.mcm.set_value(self.cell_addr_2, KEY, "B")
        self.mcm.set_value(self.cell_addr_3, KEY, "C")
        self.mcm.set_value(self.cell_addr_4, KEY, "D")  # 1 falls out
        self.mcm.set_value(self.cell_addr_1, KEY, "E")  # 2 falls out
        self.mcm.set_value(self.cell_addr_2, KEY, "F")  # 3 falls out
        self.mcm.set_value(self.cell_addr_3, KEY, "G")  # 4 falls out

        self.assertListEqual(self.mcm.load_log, [
            'data/cell_files/test_prefix_201101-test_col',
            'data/cell_files/test_prefix_201102-test_col',
            'data/cell_files/test_prefix_201103-test_col',
            'data/cell_files/test_prefix_201104-test_col',
            'data/cell_files/test_prefix_201101-test_col',
            'data/cell_files/test_prefix_201102-test_col',
            'data/cell_files/test_prefix_201103-test_col'
        ])
        self.assertListEqual(self.mcm.save_log, [
            ('data/cell_files/test_prefix_201101-test_col',
             {"CellAddr(date=20110101, col='test_col'): test_key": "A"}),
            ('data/cell_files/test_prefix_201102-test_col',
             {"CellAddr(date=20110202, col='test_col'): test_key": "B"}),
            ('data/cell_files/test_prefix_201103-test_col',
             {"CellAddr(date=20110303, col='test_col'): test_key": "C"}),
            ('data/cell_files/test_prefix_201104-test_col',
             {"CellAddr(date=20110404, col='test_col'): test_key": "D"})
        ])

        # Even reading should trigger an update
        self.assertEqual(self.mcm.get_value(self.cell_addr_4, KEY), "D")

        self.assertEqual(self.mcm.load_log[-1],
                         'data/cell_files/test_prefix_201104-test_col')
        self.assertEqual(
            self.mcm.save_log[-1],
            ('data/cell_files/test_prefix_201101-test_col',
             {"CellAddr(date=20110101, col='test_col'): test_key": "E"}))
