"""Implements DateSet

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

Description
===========

DateSet is a simple wrapper class to a sorted list and a dictionary with date
keys and CellKey values, with push_date adding to both.  The list (dates) and
dict (dates_keys) are publicly available.  There's also logic for how to load
and save from a "prefix" name, passed at initialization.

    Typical usage example:

    >>> my_date_set = DateSet("test_filepath_prefix")
    >>> my_date_set.open()
    >>> my_date_set.push_date(20010102, "key1")
    >>> my_date_set.push_date(20010103, "key2")
    >>> my_date_set.push_date(20010101, "key2")
    >>> my_date_set.push_date(20010101, "key2")  # No effect
    >>> print(my_date_set.dates_keys)
    defaultdict(<class 'set'>, {20010102: {'key1'}, 20010103: {'key2'}, 20010101: {'key2'}})
    >>> print(my_date_set.dates)  # Ordered maintained
    [20010101, 20010102, 20010103]
    >>> my_date_set.close()  # Saves to files with "test_filepath_prefix"

    >>> # Loads files upon opening
    >>> other_date_set = DateSet("test_filepath_prefix")
    >>> other_date_set.open()
    >>> print(other_date_set.dates_keys)
    defaultdict(<class 'set'>, {20010102: {'key1'}, 20010103: {'key2'}, 20010101: {'key2'}})
    >>> other_date_set.close()

    >>> # Clean up files
    >>> delete_dataset("test_filepath_prefix")
"""

import os
import pickle
from collections import defaultdict
from typing import Optional

from cell_header import *


def delete_dataset(prefix: Text) -> None:
    """Remove dateset data files with the given prefix.

    If no such files are found, do nothing.

    Arguments:
        prefix: The prefix of the files we want to delete.
    """
    # Look in DATE_SET_DIR for any files corresponding to the passed prefix.
    for root, _, files in os.walk(os.path.join("..", DATE_SET_DIR)):
        for file in files:
            if file.find("{}_".format(prefix)) == -1:
                continue
            os.remove(os.path.join(root, file))


class DateSet(object):
    """A simple wrapper to a set and a sorted list, both holding Dates.

    There is a function to add a Date to both these structures, called
    push_date.  Both the set (dates_keys) and the list (dates) are
    intended to be accessed directly.

    On initialization, looks for files with the given prefix, and loads them
    if they exist.  On close (called explicitly), it will save to files with
    the given prefix, overwriting if they exist.

    Attributes:
        prefix: Text that tells us how to save / load files.
        dates_keys: A dict where the keys are distinct dates, and the values are
            set of the CellKeys for that date.
        dates: An ordered list with all the Dates we've added.  We maintain
            the order as we add new dates
    """

    def __init__(self, prefix: Text):
        self.prefix = prefix

        self.dates_keys: DefaultDict[Date, Set[CellKey]] = defaultdict(set)
        self.dates: List[Date] = list()

    def smallest_ind_gt_date(self, date: Date, st: Optional[Date] = None,
                              en: Optional[Date] = None) -> int:
        """A simple binary search, which returns the index of the smallest date
        which is greater than the passed date.

        Arguments:
            date: The date that we're targeting.
            st: Used in the binary search, should not be set on call.
            en: Used in the binary search, should not be set on call.

        Returns:
            The index of the smallest date that is greater than the passed date.
        """
        if st is None:
            st = 0
        if en is None:
            en = len(self.dates)

        if st == en:
            return st

        m = (st + en) // 2
        if self.dates[m] > date:
            return self.smallest_ind_gt_date(date, st, m)
        return self.smallest_ind_gt_date(date, m + 1, en)

    def slice(self, st_date: Optional[Date] = None,
              en_date: Optional[Date] = None) -> List[Date]:
        """Gets all the dates between st_date and en_date, inclusive.

        Returns a copy of these.

        Arguments:
            st_date: Lower bound of dates in response.
            en_date: Upper bound of dates in response.

        Returns:
            A (sorted) list of the dates between st_date and en_date (inclusive)
        """
        if len(self.dates) == 0:
            return []

        if st_date is None:
            st_ind = 0
        else:
            st_ind = self.smallest_ind_gt_date(st_date)
            # This may miss a date that is equal, since gt_date is a strict
            # inequality.
            if st_ind > 0 and self.dates[st_ind - 1] == st_date:
                st_ind -= 1

        if en_date is None:
            en_ind = len(self.dates)
        else:
            en_ind = self.smallest_ind_gt_date(en_date)

        return self.dates[st_ind:en_ind]

    def push_date(self, date: Date, cell_key: CellKey) -> None:
        """Inserts a date into both dates_keys and dates (in order).
        
        Arguments:
            date: The date that we want to add to our date set.
        """
        date_already_encountered = (date in self.dates_keys)
        self.dates_keys[date].add(cell_key)
        if date_already_encountered:
            # Stop here.
            return

        if len(self.dates) == 0:
            self.dates = [date]
            return

        ind = self.smallest_ind_gt_date(date)
        self.dates = self.dates[:ind] + [date] + self.dates[ind:]

    def _walk_files(self):
        """Pass through to os.walk on DATE_SET_DIR."""
        for root, _, files in os.walk(os.path.join("..", DATE_SET_DIR)):
            yield (root, files)

    def _load_file(self, path: Text) -> Any:
        """Pass through to pickle.load"""
        with open(path, "rb") as f:
            result = pickle.load(f)
        return result

    def _save_file(self, object: Any, path: Text) -> None:
        """Pass through to pickle.dump"""
        with open(path, "wb") as f:
            pickle.dump(object, f)

    def open(self) -> None:
        """Loads the dates and dates_keys dict from disk.

        Looks in DATE_SET_DIR for any files corresponding to the passed prefix.
        """
        for root, files in self._walk_files():
            for file in files:
                if file.find("{}_".format(self.prefix)) == -1:
                    continue
                if file.find("_{}".format(DATES_FILE)) != -1:
                    self.dates = self._load_file(os.path.join(root, file))
                if file.find("_{}".format(DATES_SET_FILE)) != -1:
                    self.dates_keys = self._load_file(
                        os.path.join(root, file))

    def close(self) -> None:
        """Upon closing, save the files for the function.
        
        Saves both a primary and a backup for both the dates list and for
        the dates_keys.  Uses the stored prefix to decide the files' paths.
        """
        dates_path = os.path.join(
            DATE_SET_DIR, "{}_{}".format(self.prefix, DATES_FILE))
        dates_set_path = os.path.join(
            DATE_SET_DIR, "{}_{}".format(self.prefix, DATES_SET_FILE))

        # Save backup copy
        self._save_file(self.dates, "{}_{}".format(dates_path, BACKUP))
        self._save_file(self.dates_keys,
                        "{}_{}".format(dates_set_path, BACKUP))

        # Save primary copy
        self._save_file(self.dates, dates_path)
        self._save_file(self.dates_keys, dates_set_path)

        # Clear
        self.dates_keys = defaultdict(set)
        self.dates = list()
