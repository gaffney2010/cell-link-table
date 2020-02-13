"""A header file that includes common constants and thin classes for use
throughout the library.

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

from typing import Any, DefaultDict, List, Set, Text

import attr
import numpy as np

ABSOLUTE_DIR = "/home/gaffney/psdata"

CELL_FILES_DIR = ABSOLUTE_DIR + "/cell_files"
COLUMN_FILES_DIR = ABSOLUTE_DIR + "/column_data"
DATE_SET_DIR = ABSOLUTE_DIR + "/dateset_data"
DATES_FILE = "dates_file"
DATES_SET_FILE = "dates_set_file"
BACKUP = "backup"

# TODO: More robust date availability checking
CHECK_DATE_AVAILABILITY = False
SNAPSHOT_KEY = "SNAPSHOT"
MAX_DATE = 99999999

# Throughout the library, Dates are expected to be integers of format YYYYMMDD.
Date = int
CellKey = Text
ColumnName = Text
Snapshot = DefaultDict[CellKey, int]


def nonish(x):
    """Encapsulates the many ways that a cell can be missing a value."""
    if x is None:
        return True
    try:
        if np.isnan(x):
            return True
    except:
        pass
    if x != x:
        return True
    return False


def default_or(x: Any, default_value=0) -> Any:
    """Returns the passed value, unless it's nonish, in which case, return the
    default_value.

    Arguments:
        x: The value that we're checking, and returning if not nonish.
        default_value: The value to return if x is nonish.

    Returns:
        x if not nonish, and default_value otherwise.
    """
    if nonish(x):
        return default_value
    return x


@attr.s(frozen=True)
class CellAddr(object):
    """Used to reference the row (date) and column of a cell.

    This class is immutable.

    Attributes:
        date: The date of this data point.
        col: The name of the column of this data point.
    """

    date: Date = attr.ib()
    col: ColumnName = attr.ib()


# TODO: Add a repr
class Column(object):
    """
    A class that describes a column in our table, by providing logic about
    how to refresh the cell values in the column.

    This is a base class.  Derived classes will overwrite the refresh
    function with non-standard logic.  Instances of the class will store a
    name for the column a list of downstream, dependent columns (by name).

    Column is loaded and saved by name, together with the table name.

    Attributes:
        table: A pointer to the table that this column is associated with.
        name: Columns are very often identified by their name only.
        _column_dependencies: A set of names of columns that depend on this
            column.
    """

    def __init__(self, name: ColumnName, table: 'Table'):
        self.name = name
        self._column_dependencies = set()

        self.open(table)

        # Add this column to the table.
        table.add_column(self)

    def dependencies(self) -> Set:
        """Return column dependencies by name.

        A column dependency is a column that should be refreshed whenever
        this column is updated.

        Intended to be final.

        Returns:
            A set of column names that depend on this column.
        """
        return self._column_dependencies

    def cell_dependencies(self, cell_addr: CellAddr) -> List[CellAddr]:
        """Return addresses of dependent cells.

        This is just the dependent columns paired with the date on the passed
        cell address.

        Intended to be final.

        Arguments:
            cell_addr: The address of the cell for which we want to find the
                dependent cells.  Used just for the date.

        Return:
            A list of cell addresses of the dependent cells.
        """
        return [CellAddr(cell_addr.date, col) for col in self.dependencies()]

    def refresh(self) -> None:
        """
        This should refresh all the cells on the table that need refreshing
        for this column.

        Any update should be done with table.set_cell(), so that new
        dependencies will be triggered.

        This is intended to be overridden in derived Column classes.
        """
        pass

    def available_on_date(self, cell_addr: CellAddr) -> Date:
        """The earliest date that we're able to use a cell with the passed
        address.

        By default a cell is available on its own date.

        Arguments:
            cell_addr: The cell which we want to know which date it's available
                on.

        Return:
            The date that the cell is available.
        """
        return cell_addr.date

    def key_init(self, cell_addr: CellAddr, key: CellKey) -> None:
        """Called when a new key is encountered.

        By default, does nothing.
        """
        pass

    def open(self, table: 'Table') -> None:
        """Called when column is first obtained, to load relevant data.

        For the base column, this will do nothing because the column is
        stateless.  Generally we want to use this to load any non-trivial
        component, specifically any helpers on the column (ColumnManager,
        DateSet, PageMaster).
        """
        self.table = table

    def close(self) -> None:
        """Called when done with the column, to ensure proper saving.

        For the base column, we only set the table to None.  This is so that
        we don't try to save a copy of the table when we save the column;
        saving columns will always follow closing.

        Generally we want to use this to save any non-trivial
        component, specifically any helpers on the column (ColumnManager,
        DateSet, PageMaster).

        After saving, it should clear any saved objects.  Anything left
        unclear will be (attempted to) saved and loaded with the column.
        """
        self.table = None


class FlatColumn(Column):
    """Just a simple column."""

    def __init__(self, name: ColumnName, table: 'Table'):
        super().__init__(name, table)


class ProtectedColumn(Column):
    """A column that can only be used in calculations that are available at
    later dates.

    This is intended for target values.  For example, if you're trying to
    predict some event based on the frequency of the event in the past,
    then you should make the event column protected so that there's no chance
    of having it bleed through to the predictor columns later.
    """

    def __init__(self, name: ColumnName, table: 'Table'):
        super().__init__(name, table)

    def available_on_date(self, cell_addr: CellAddr) -> int:
        return cell_addr.date + 1


class ConstColumn(Column):
    """A column where every value is equal to a constant, which is provided at
    the time of initialization."""

    def __init__(self, name: ColumnName, table: 'Table', const: Any):
        self.const = const  # Must come first
        super().__init__(name, table)

    def key_init(self, cell_addr: CellAddr, key: CellKey) -> None:
        """When a new key is encountered, just store the column constant."""
        self.table.set_cell_value(cell_addr, key, self.const)
