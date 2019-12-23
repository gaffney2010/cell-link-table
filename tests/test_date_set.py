"""Tests for date_set.

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

    def test_basic_add(self):
        key = "key"

        date_set = MockDateSet(TEST_PREFIX)
        date_set.push_date(2, key)
        date_set.push_date(1, key)
        date_set.push_date(5, key)
        date_set.push_date(4, key)
        date_set.push_date(3, key)
        date_set.push_date(0, key)

        self.assertListEqual(date_set.dates, [0, 1, 2, 3, 4, 5])
        self.assertDictEqual(date_set.dates_keys,
                             {0: {key}, 1: {key}, 2: {key}, 3: {key}, 4: {key},
                              5: {key}})

    def test_ignore_repeats(self):
        key = "key"

        date_set = MockDateSet(TEST_PREFIX)
        date_set.push_date(2, key)
        date_set.push_date(1, key)
        date_set.push_date(2, key)
        date_set.push_date(3, key)
        date_set.push_date(2, key)
        date_set.push_date(2, key)

        self.assertListEqual(date_set.dates, [1, 2, 3])
        self.assertDictEqual(date_set.dates_keys,
                             {1: {key}, 2: {key}, 3: {key}})

    def test_saves_and_loads(self):
        key = "key"

        fake_files = dict()

        date_set = MockDateSet(TEST_PREFIX, fake_files=fake_files)
        date_set.open()
        date_set.push_date(100, key)
        date_set.push_date(200, key)
        self.assertDictEqual(date_set.dates_keys, {100: {key}, 200: {key}})
        date_set.close()

        other_date_set = MockDateSet(TEST_PREFIX, fake_files=fake_files)
        other_date_set.open()
        self.assertDictEqual(other_date_set.dates_keys,
                             {100: {key}, 200: {key}})
        other_date_set.close()

    def test_distinct_keys(self):
        key = "key"

        date_set = MockDateSet(TEST_PREFIX)
        date_set.push_date(1, "abc")
        date_set.push_date(1, "def")
        date_set.push_date(2, "xxx")
        date_set.push_date(1, "ghi")
        date_set.push_date(2, "yyy")
        date_set.push_date(2, "xxx")  # Should do nothing.

        self.assertDictEqual(date_set.dates_keys,
                             {1: {"abc", "def", "ghi"}, 2: {"xxx", "yyy"}})

    def test_slice(self):
        date_set = MockDateSet(TEST_PREFIX)
        self.assertListEqual(date_set.slice(), [])

        date_set.dates = [100, 200, 400, 800, 1600, 3200]
        self.assertListEqual(date_set.slice(), date_set.dates)

        self.assertListEqual(date_set.slice(en_date=500), [100, 200, 400])
        self.assertListEqual(date_set.slice(st_date=500), [800, 1600, 3200])
        self.assertListEqual(date_set.slice(500, 2000), [800, 1600])
        self.assertListEqual(date_set.slice(2000, 99999), [3200])
        self.assertListEqual(date_set.slice(90000, 99999), [])

        # Should include start and end dates.
        self.assertListEqual(date_set.slice(400, 1600), [400, 800, 1600])
