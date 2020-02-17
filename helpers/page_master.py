"""Implements PageMaster, which maps keys to smaller maps.

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

PageMaster maintains a bunch of smaller maps.  When trying to get or set a
value from a key, a user could send the key to the PageMaster, which then
knows to which of the smaller maps to send the key.  PageMaster is also in
charge of reading and writing these small maps to files.  It does not key all
small maps open at once, but rather keeps a fixed number open at a time,
deciding when to save and close a file based on a least-recently used rule.

Also defines two specific derived verisons of this:  CellMaster and
SnapshotMaster.  CellMaster is used to hold the data in the cell-link table;
it's addressed by CellAddr.  SnapshotMaster is used to cache data used in the
Waterfall column; it's addressed by special dates in the Waterfall
implementation.

    Typical usage example:

    >>> addr_1 = CellAddr(20110101, "test_col")
    >>> addr_2 = CellAddr(20110202, "test_col")
    >>> km = CellMaster("test_prefix")
    >>> km.open()

    >>> km.set_value(addr_1, "KEY1", "A")
    >>> km.set_value(addr_1, "KEY2", "B")
    >>> km.set_value(addr_2, "KEY2", "C")

    >>> print(km.get_value(addr_1, "KEY1"))
    A
    >>> print(km.get_value(addr_1, "KEY2"))
    B
    >>> print(km.get_value(addr_2, "KEY2"))
    C

    >>> km.set_value(addr_1, "KEY1", "D")  # Overwrites
    >>> print(km.get_value(addr_1, "KEY1"))
    D
    >>> km.close()

    >>> delete_pagemanager("test_prefix")  # Clean up
"""

import os
import pickle
from typing import Generic, Dict, TypeVar, Optional

from cell_header import *

# Must have __str__ implemented.
Addr = TypeVar("Addr")


def delete_pagemanager(prefix: Text) -> None:
    """Remove PageMaster data files with the given prefix.

    If no such files are found, do nothing.

    Arguments:
        prefix: The prefix of the files we want to delete.
    """
    # Look in DATE_SET_DIR for any files corresponding to the passed prefix.
    for root, _, files in os.walk(os.path.join(CELL_FILES_DIR)):
        for file in files:
            if file.find("{}_".format(prefix)) == -1:
                continue
            os.remove(os.path.join(root, file))


class PageMaster(Generic[Addr]):
    """Manages setting and getting values to keys, spread over multiple files.

    The class has a get and a set method both of which take a key along with
    the address for the key.  An internal function maps the address to a
    "page."  The page is loaded up from disk, and is modified locally. The
    most recently-used pages are kept and modified locally.  After a page
    hasn't been used for a long enough time (determined by cache_size),
    the modified local version is saved back to disk, overwriting, and it is
    forgotten.  When close is called, all pages are saved to disk.

    Attributes:
        prefix: How we identify the PageMaster.  Matches to the prefix on the
            table.  This is attached to the file names.
        readonly: If raised, may only read from the table.  Saves time on
            closing.
        cache: This stores the most-recently used pages locally.
        casche_size: The number of pages that we want to keep in the cache.
    """

    def __init__(self, prefix: Text, cache_size: int = 80, readonly: bool = False):
        self.prefix = prefix
        self.readonly = readonly
        self.cache = list()
        self.cache_size = cache_size

    def get_value(self, addr: Addr, key: Text) -> Optional[Any]:
        """Returns the value stored for the key at the address.

        Swallows any failures, returning None instead.

        Arguments:
            addr: The address tells the locality.
            key: The key for which to return the value.

        Returns:
            The value assigned to the key.  Or None, if no value exists, or
                there was an error.
        """
        try:
            page = self._get_page(addr)
            result = page.get(self._addr_key(addr, key), None)
        except:
            result = None
        return result

    def set_value(self, addr: Addr, key: Text, value: Any) -> None:
        """Sets the passed value to the key at the address.

        Overwrites if the key already has a value set.

        Arguments:
            addr: The address tells the locality.
            key: The key for which to assign the value.
            value: The value to assign
        """
        if self.readonly:
            raise PermissionError("Cannot modify a readonly.")
            
        page = self._get_page(addr)
        page[self._addr_key(addr, key)] = value

    def _addr_key(self, addr: Addr, key: Text) -> Text:
        """Builds a key from the address and key.

        We only require that a key be unique with a page address,
        but multiple addresses may be saved on a single page.  So we will use
        this function to build a new key from both the address and the key.

        Arguments:
            addr: The address within which we require unique keys.
            key: The key we want to extend.

        Returns:
            The extended key built from address and key.
        """
        return "{}: {}".format(str(addr), key)

    def _addr_to_page(self, addr: Addr) -> Text:
        """Maps the address to a page name.
        
        The page name is used throughout to know where to look for a key stored
        to a addr.

        This function is expected to be overwritten in inheritance if using
        non-default paging logic.

        Arguments:
            addr: The address that we want to map to a page name.
        Returns:
            The page name for that.
        """
        return str(addr)

    def _get_page(self, addr: Addr) -> Dict:
        """Get the dict (page) where the addr resides.

        This will be looked up from the cache if it exists there, otherwise
        loaded and added to the cache.

        Arguments:
           addr: The addr for which we want to load the page.

        Returns:
             The dict (page) where the addr resides.
        """
        page_name = self._addr_to_page(addr)

        # Check to see if it's in the cache
        for ci, c in enumerate(self.cache):
            if c[0] == page_name:
                # Move that page to the front of the cache, to maintain that the
                # cache orders by the most-recently used.
                self.cache = (
                        [self.cache[ci]] + self.cache[:ci] + self.cache[ci + 1:]
                )
                return c[1]

        # The page is not already in the cache, so open it up.
        return self._load_new_page(page_name)

    def _load_new_page(self, page_name: Text) -> Dict:
        """Load the dict (page) from the file name.
        
        Load from the file name, and insert the page at the beginning of the
        cache, pushing the least-recently used page out of the cache (if
        cache size exceeded).  Then returns that page.

        Assumes that the page is not already in the cache, leaving the caller
        responsible for checking this.

        Arguments:
            page_name: The name of the page that we want to load.
        """
        full_path = os.path.join(CELL_FILES_DIR,
                                 "{}_{}".format(self.prefix, page_name))
        new_dict = self._load_file(full_path)
        new_cache_entry = (page_name, new_dict)

        # Put the new entry at the front, and delete any overflow
        self.cache = [new_cache_entry] + self.cache

        # If the cache is full then save the last cache, then forget it
        if len(self.cache) > self.cache_size:
            self._save_single_page()
            self.cache = self.cache[:self.cache_size]

        return new_dict

    def _load_file(self, path: Text) -> Dict:
        """Load the dict from the passed path.  Return an empty dict if path
        doesn't exist."""
        result = dict()
        if os.path.exists(path):
            with open(path, "rb") as f:
                result = pickle.load(f)
        return result

    def _save_file(self, object: Dict, path: Text) -> None:
        """Save the passed dict to the passed path."""
        if self.readonly:
            raise PermissionError("Cannot modify a readonly.")
            
        with open(path, "wb") as f:
            pickle.dump(object, f)

    def _save_single_page(self, cache_index: Optional[int] = None) -> None:
        """Save the dict (page) from the passed slot, into its file.

        If no index is set, then save the least-recently used page (last
        index); this is used when we're getting ready to forget that page.

        Arguments:
            cache_index: If set, save the page in that spot of the index; this
                is used when saving all pages.
        """
        if self.readonly:
            raise PermissionError("Cannot modify a readonly.")
            
        if cache_index is None:
            # If not specified, save the last one.  The most common case.
            cache_index = len(self.cache) - 1

        save_path = os.path.join(
            CELL_FILES_DIR,
            "{}_{}".format(self.prefix, self.cache[cache_index][0])
        )
        self._save_file(self.cache[cache_index][1], save_path)

    def save_all_and_empty(self) -> None:
        """Save all open pages, and clear the cache."""
        if self.readonly:
            raise PermissionError("Cannot modify a readonly.")
            
        for i in range(len(self.cache)):
            self._save_single_page(i)
        self.cache = []

    def open(self):
        """Don't do anything, lazy load."""
        pass

    def close(self):
        """Upon closing, just save all the pages."""
        if self.readonly:
            return
        
        self.save_all_and_empty()


class CellMaster(PageMaster[CellAddr]):
    """A PageMaster for Cells.

    This is used to load and save the main data in the cell link table.
    """

    def __init__(self, prefix: Text, cache_size=80, readonly: bool = False):
        super().__init__(prefix, cache_size=cache_size, readonly=readonly)

    def _addr_to_page(self, cell_addr: CellAddr) -> Text:
        """Set the address so that cells in the same month, same column will be
        on the same page."""
        return "{}-{}".format(cell_addr.date // 100, cell_addr.col)


class SnapshotMaster(PageMaster[Date]):
    """A PageMaster for Snapshots.

    This is used to load and save Snapshots used in Waterfall columns.  For
    this application, there is just one "key" per address, so we wrap the get
    and set to only require the one argument.
    """

    def __init__(self, column_name: ColumnName, readonly: bool = False):
        # Use a smaller cache, because Snapshots are so big!
        super().__init__("SNAPSHOT-{}".format(column_name),
                         cache_size=5, readonly=readonly)

    def _addr_to_page(self, addr: Date) -> Text:
        """Set the address to the start_date."""
        return str(addr)

    def get_date_value(self, date: Date) -> Optional[Snapshot]:
        """Returns the value stored for the date.

        Wraps get_value, because for Snapshots, we have only one key per page.

        Arguments:
            date: Specifies the address of the page we want to read from.

        Returns:
            The value assigned to the date.  Or None, if no value exists, or
                there was an error.
        """
        return self.get_value(addr=date, key=SNAPSHOT_KEY)

    def set_date_value(self, date: Date, value: Snapshot) -> None:
        """Sets the passed value to the date.

        Wraps set_value, because for Snapshots, we have only one key per page.

        Arguments:
            date: Specifies the address of the page we want to write to.
            value: The value to assign.
        """
        page = self._get_page(date)
        page[self._addr_key(date, SNAPSHOT_KEY)] = value