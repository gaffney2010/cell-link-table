"""Implements the ColumnManager class, a suite of column management methods.

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

There will be a single column manager instance for each table, which will
share the prefix that uniquly identifies the table.  This will store the
single, canonically copy of each of the columns   It is responsible for
loading up all the columns from disk, when the table is created, and for
saving all the altered columns when (explicitly) closed.

ColumnManager keeps track of the refresh order, a list of column names
specifying the order that the columns should be updated so that any column"s
dependencies are updated after that column.

    Typical usage example:

    >>> cm = ColumnManager("test_filepath_prefix")
    >>> cm.open()

    >>> col_a = Column("A")
    >>> col_b = Column("B")
    >>> col_b.other_field = "xyz"
    >>> col_c = Column("C")
    >>> col_c._column_dependencies = {"B", "A"}
    >>> col_a._column_dependencies = {"B"}
    >>> cm.add_column(col_a)  # Returns name of column
    'A'
    >>> cm.add_column(col_b)
    'B'
    >>> cm.add_column(col_c)
    'C'

    >>> print(cm.get_column("B").other_field)
    xyz
    >>> print([x for x in cm.refresh_order if x != "primary_key"])
    ['C', 'A', 'B']

    >>> cm.close()  # Saves to files with "test_filepath_prefix"

    >>> # Loads files upon opening
    >>> other_cm = ColumnManager("test_filepath_prefix")
    >>> other_cm.open()
    >>> print(other_cm.get_column("B").other_field)
    xyz
    >>> cm.close()

    >>> # Clean up files
    >>> delete_dataset("test_filepath_prefix")
"""

import os
from collections import defaultdict
from typing import Iterator, Tuple

import dill

from cell_header import *
from helpers.topological_sort import *


def delete_dataset(prefix: Text) -> None:
    """Remove dateset data files with the given prefix.

    If no such files are found, do nothing.

    Arguments:
        prefix: The prefix of the files we want to delete.
    """
    # Look in DATE_SET_DIR for any files corresponding to the passed prefix.
    for root, _, files in os.walk(os.path.join("..", COLUMN_FILES_DIR)):
        for file in files:
            if file.find("{}_".format(prefix)) == -1:
                continue
            os.remove(os.path.join(root, file))


def recover_from_backup(prefix: Text) -> None:
    """A function to recover column files from backups.

    As we save column data, we save copies with the suffix BACKUP.  This
    function copies those backups into the main files, overwriting.  This
    must be called manually.

    Arguments:
        prefix: Identifies the table that the column data belongs to.  Also the
            prefix to all the files.
    """
    for root, _, files in os.walk(os.path.join("..", COLUMN_FILES_DIR)):
        for file in files:
            if file.find(prefix) == -1:
                continue
            new_path = "{}_{}".format(os.path.join(root, file), BACKUP)
            old_path = os.path.join(root, file)
            with open(new_path, "rb") as f:
                this_column = dill.load(f)
            with open(old_path, "wb") as f:
                dill.dump(this_column, f)


class ColumnManager(object):
    """Keeps track of the columns in a table, along with their refresh order.

    Upon initialization, the class loads all the columns (with the given
    prefix) from disk, and upon explicitly calling close (or save), it will
    save to disk (overwrite) any column that has been accessed.

    New columns can be added with add_column(column).  Existing columns can
    be accessed with get_column(column_name).

    As you add more columns, the refresh order is maintained.  Refresh is the
    the order that the columns should be updated so that any column"s
    dependencies are updated after that column.  The refresh_order can be
    accessed directly.

    Attributes:
        prefix: How we identify the ColumnManager.  Matches to the prefix on the
            table.  This is attached to the file names.
        _columns: A dict whose values are the columns covered by the manager,
            keyed by the name of those columns.  Should be accessed through
            get_column.
        dependency_graph: A dict whose keys are column names, and the values are
            sets containing the names of the columns which depend on the column
            named in the key.
        save_needed: A set of columns which have been altered since the last
            save.
        refresh_order: A list of column names specifying the order that the
            columns should be updated so that any column"s dependencies are
            updated after that column.  Intended to be directly accessed
            externally.
    """

    def __init__(self, prefix: Text):
        self.prefix = prefix

        self._columns: Dict[ColumnName, Column] = dict()

        self.dependency_graph: DefaultDict[ColumnName, Set[ColumnName]] = \
            defaultdict(set)
        self.save_needed: Set[bool] = set()
        self.refresh_order: List[ColumnName] = list()

    def __contains__(self, col: ColumnName) -> bool:
        """True if col is in self._columns"""
        return col in self._columns

    # TODO: Lazy load.
    def get_column(self, key: ColumnName) -> Column:
        """Use accessor so that we can mark as save_needed."""
        self.save_needed.add(key)
        return self._columns[key]

    def items(self) -> Iterator[Tuple[ColumnName, Column]]:
        """Forwards the .items() function from _columns."""
        for k, v in self._columns.items():
            self.save_needed.add(k)
            yield k, v

    def add_column(self, column: Column) -> ColumnName:
        """Adds a column.

        Keep a reference to the column, indexed by the name of the column.
        Additionally marks the column as needing to be saved.

        Arguments:
            column: The column that we want to add.

        Returns:
            The new columns name.
        """
        if column.name in self._columns:
            # Already added.  Do nothing.  Trust user to not create multiple
            # different columns with the same name.
            return column.name
        self._columns[column.name] = column  # Store by name

        # Calculate the refresh order by given the dependency graph.
        self._update_column_dependencies()

        self.save_needed.add(column.name)
        return column.name

    def _update_column_dependencies(self) -> None:
        """Calculate the refresh order by given the dependency graph."""
        for k, v in self._columns.items():
            # Need to reset all columns because the dependencies may have
            # changed.
            self.dependency_graph[k] = v.dependencies()
        self.refresh_order = topological(self.dependency_graph)

    def _walk_files(self):
        """Pass through to os.walk on DATE_SET_DIR."""
        for root, _, files in os.walk(os.path.join("..", COLUMN_FILES_DIR)):
            yield (root, files)

    def _load_file(self, path: Text) -> Column:
        """Load the dict from the passed path.  Return an empty dict if path
        doesn"t exist."""
        result = dict()
        if os.path.exists(path):
            with open(path, "rb") as f:
                result = dill.load(f)
        return result

    def _save_file(self, object: Column, path: Text) -> None:
        """Save the passed dict to the passed path."""
        with open(path, "wb") as f:
            dill.dump(object, f)

    def open(self, table: 'Table') -> None:
        """Load up any existing columns.

        Looks through the column files to see if any match the prefix, and loads
        them.  Then calls open on those columns to do any supplemental loading
        if needed.
        """
        for root, files in self._walk_files():
            for file in files:
                if file.find(self.prefix) == -1:
                    continue
                new_col = self.add_column(
                    self._load_file(os.path.join(root, file)))
                self._columns[new_col].open(table)
        self._update_column_dependencies()

    def close(self) -> None:
        """Save off columns.

        Calls close() on the columns, which will save and clear supplemental
        data, then saves to the column files with the prefix.
        """
        if not self.save_needed:
            return

        # Close then save all the columns, overwriting.  Closing will clear
        # non-trivial data structures.
        for k, v in self._columns.items():
            if k not in self.save_needed:
                continue
            v.close()
            write_path = os.path.join(
                COLUMN_FILES_DIR, "{}-{}".format(self.prefix, k))
            self._save_file(v, write_path)

        # Clear
        self.save_needed = set()
        self._columns = dict()
