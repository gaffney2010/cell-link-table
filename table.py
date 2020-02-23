"""Implements Table, the main class for the cell_link library.

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

import pandas as pd

from helpers.column_manager import *
from helpers.date_set import *
from helpers.page_master import *


class Table(object):
    """A cell-link table.

    This table allows you to add Columns, then set and get values from
    entries in those columns via the set_cell_value and get_cell_value
    functions.  Both of these store values by their date and column (together
    called CellAddr) and a key.

    Additionally this table allows you to call refresh(), which will update
    the columns in order of their dependencies, so that if any column X
    depends on a column Y, then Y will get updated before X.  The refresh is
    performed by calling the refresh function on the column (and passing a
    copy of this table), which will contain logic on how to update values
    given the current state of the table.  The columns are also responisble
    for knowing their dependents.

    Before using, call open() to load all the relevant data.  When done,
    call close() to save the data.  (Some of the data will save as it goes,
    but not everything.)  All data gets saved by prefix.

    Attributes:
        prefix: How we identify the Table.  This is attached to the file names,
            and used to link to the components (CellMaster, ColumnManager, and
            Dateset).
        readonly: If raised, may only read from the table.  Saves time on
            closing.
        cells: A CellMaster which is used to save and load cells.
        cm: A ColumnManager used to save and load columns, and maintain an
            update order.
        ds: A DateSet used to store all the dates we have data for.
        need_refresh: A dictionary telling which cell addresses need refreshing.
            The keys are column names, and the values are sets of cell addresses
            (for that column) that need refreshing.
    """

    def __init__(self, prefix: Text, readonly: bool = False) -> None:
        self.prefix = prefix
        self.readonly = readonly

        self.cells = CellMaster(self.prefix, readonly=readonly)
        self.cm = ColumnManager(self.prefix, readonly=readonly)
        self.ds = DateSet(self.prefix, readonly=readonly)

        self.need_refresh: DefaultDict[ColumnName, Set[CellAddr]] = defaultdict(
            set)

    def add_column(self, column: Column) -> None:
        """Adds the column to the table.

        Forwards request to the ColumnManager on this table, and marks the
        column as needing a refresh, which will calculate all the values on
        next refresh.

        Arguments:
            column: The columns we want to add.
            delay_update: If false, will update column manager immediately.
        """
        if self.readonly:
            raise PermissionError("Cannot modify a readonly.")

        self.cm.add_column(column)

        for date, row_keys in self.ds.dates_keys.items():
            # Mark everything in this column for refresh for refresh
            self.need_refresh[column.name].add(CellAddr(date, column.name))

    def get_cell_value(self, cell_addr: CellAddr, key: CellKey,
                       assert_available_on: int = MAX_DATE,
                       check_date_availability: bool = CHECK_DATE_AVAILABILITY) -> \
    Any:
        """Get value corresponding to the key at the cell address.

        Mostly a straight-forward pass-through to the get_value function on
        this table's CellMaster.

        Checks that the available_on_date for the CellAddr (as implemented in
        the column) is at most assert_available_on (passed).  This is a
        safety check, to be extra cautious that we don't bleed dependencies.

        Arguments:
            cell_addr: Which address to lookup the key in.
            key: The key for which to return the value.
            assert_available_on: Date that we're pulling the cell value for.
                Will fail if the cell isn't available on this date.  If unset,
                will not check.
            check_date_availability: If this is disabled, then don't make
                assertion about availability date.  Expected to mostly take the
                value of the global flag (the default value), but can be passed
                for testing.

        Returns:
            The value at the cell_addr / key.  Or NoneClass instance, if not found.
        """
        if check_date_availability and assert_available_on < self.cm.get_column(
                cell_addr.col).available_on_date(cell_addr):
            raise KeyError("Not available on date.")

        result = self.cells.get_value(cell_addr, key)

        # Initialize a value then.
        if result is None:
            result = self.cm.get_column(cell_addr.col).key_init(cell_addr, key)
            # To prevent key_init() from running again.
            if result is None:
                result = NoneClass()
            self.cells.set_value(cell_addr, key, result)

        return result

    def set_cell_value(self, cell_addr: CellAddr, key: CellKey,
                       value: Any) -> None:
        """Updates the cell to the value in the passed cell, creating a new
        one if needed.

        Passes the request along to this table's CellManager, while adding
        the date / key to this table's DateSet.

        Marks dependents as needing refreshing.  If this function has been
        called during a refresh chain, the dependents have not yet been
        processed, and will get updated in this loop.

        Arguments:
            addr: Which address to store the key in.
            key: The key for which to assign the value.
            value: The value to assign.
        """
        if self.readonly:
            raise PermissionError("Cannot modify a readonly.")

        # Throw an error if the column hasn't been loaded
        if cell_addr.col not in self.cm:
            raise KeyError("Column not found.")

        # Add an entry for the date / key in ds if it doesn't exists.
        new_key_encountered = self.ds.push_date(cell_addr.date, key)

        # push_date returns true if the key is previously unseen.
        if new_key_encountered:
            for col, column in self.cm:
                # Make initialization actions, if any.
                column.key_init(cell_addr, key)
                # And mark as needing updating.
                self.need_refresh[col].add(CellAddr(cell_addr.date, col))

        # Write to the right place, potentially overwriting.
        self.cells.set_value(cell_addr, key, value)

        # Mark dependents as needing an update.
        for dep in self.cm.get_column(cell_addr.col).cell_dependencies(
                cell_addr):
            self.need_refresh[dep.col].add(dep)

    def all_keys_for_address(self, cell_addr: CellAddr) -> Set[CellKey]:
        """Get all the keys for a given address.

        Pulls from the table's DateSet.

        Arguments:
            cell_addr: The address for the cell for which we want to pull all of
                the available keys.  Will only use it for the date.

        Returns:
            A set of cell keys at the given address.
        """
        return self.ds.dates_keys[cell_addr.date]

    def refresh(self) -> None:
        """Refresh all the columns that need refreshing.

        Loops through the columns in such an order that if column X depends
        on column Y, then Y will get called before X.  Refreshes the columns,
        sending the columns a copy of this table; the columns will refer back
        to the table to see which addresses need refreshing.
        """
        if self.readonly:
            return

        for column_name in self.cm.refresh_order:
            if self.need_refresh[column_name]:
                self.cm.get_column(column_name).refresh()
                self.need_refresh[column_name] = set()

    def make_df(self, columns: List[ColumnName],
                dates: Optional[List[Date]] = None) -> pd.DataFrame:
        """Makes a dataframe with the given dates and columns.

        Arguments:
            columns: A list of the names of the columns that we want to include.
            dates: A list of the dates to include.  If unset, will include all
                of the dates.

        Returns:
            A pandas dataframe.
        """
        if dates is None:
            dates = self.ds.dates

        rows = list()
        for d in dates:
            for k in self.all_keys_for_address(CellAddr(d, "")):
                this_row = dict()
                for c in columns:
                    this_row[c] = self.get_cell_value(CellAddr(d, c), k)
                rows.append(this_row)
        return pd.DataFrame(rows, columns=columns)

    def open(self) -> None:
        """Open each of the core components."""
        self.cells.open()
        self.ds.open()
        self.cm.open(self)

    def close(self) -> None:
        """Close each of the core components."""
        if self.readonly:
            return

        # If anything need to be refreshed then refresh.
        for _, v in self.need_refresh.items():
            if v:
                self.refresh()
                break

        self.cells.close()
        self.ds.close()
        self.cm.close()
