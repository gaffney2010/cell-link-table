"""Defines SimpleFormula and Waterfall columns.

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

SimpleFormula is a column that applies a formula to other columns in the same
row.

Waterfall is a column that keeps a running sum of some other row.

Examples of these are in README.rst.
"""

from copy import deepcopy
from typing import Callable

from table import *

SimpleFunc = Callable[[Dict], Any]


def _calculate_f(table: Table, f: SimpleFunc,
                 required_columns: List[ColumnName],
                 cell_addr: CellAddr, key: Text) -> Any:
    """Calculate f, using values from the table, for a given key.

    This is a helper function for the simple formula column.

    First looks up (in the passed table) the values for the given date (from
    cell_addr) / key along each of the required_columns.  It stores thes into
    a dict, which then gets passed to f, and returns the result.

    The call to table should assert that the date of the cells that we're
    updating (read from cell_addr) is at most the date that the dependent
    cells are available on.

    Arguments:
        table: The cell-link table that we use to look-up values.
        f: The function that we will run on a dict of values to get a scalar
            value as result.
        required_columns: The columns that f depends on.
        key: The key that we want to find in each of the columns in our lookup
            to pass to f.
    """
    dispatcher = dict()
    for col in required_columns:
        dispatcher[col] = table.get_cell_value(
            CellAddr(cell_addr.date, col), key,
            assert_available_on=cell_addr.date)

    for _, v in dispatcher.items():
        if nonish(v):
            return None

    return f(dispatcher)


class SimpleFormula(Column):
    """A column in which each cell can be computed via a simple formula on
    the other columns, same row.

    Attributes:
        name: The name of the column.
        table: A pointer to the table that this column is associated with.
        f: A function which acts on the values in the dependent columns for a
            given cell address/key, and returns a single value.  The values in
            the dependent columns are expected in a dict, keyed by the column
            name.
        required_columns: A set of names for the columns upon which this column
            depends.
    """

    def __init__(self, name: Text, table: Table, f: SimpleFunc,
                 required_columns: List[Text]):
        self.f = f
        self.required_columns = required_columns
        for col in self.required_columns:
            table.cm.get_column(col)._column_dependencies.add(name)

        # Should set the name and the table.
        super().__init__(name, table)

    def refresh(self) -> None:
        """Triggers a refresh for the column.

        We may assume that all dependent columns are up-to-date.

        This function loops through all the cell addresses that need
        refreshing, and calculates a new value for each key in those
        addresses.  Then it marks the dependent cell addresses, being those
        addresses of the same date in a dependent column.

        Arguments:
            table: The table that this column lives in, will use to update
                values and to mark dependents.
        """
        if self.readonly:
            return

        for cell_addr in self.table.need_refresh[self.name]:
            for key in self.table.all_keys_for_address(cell_addr):
                new_value = _calculate_f(self.table, self.f,
                                         self.required_columns, cell_addr, key)
                self.table.set_cell_value(cell_addr, key, new_value)

            # Schedule updates.  The refresh order guarantees that these haven't
            # hit yet
            for dep in self.cell_dependencies(cell_addr):
                self.table.need_refresh[dep.col].add(dep)


@attr.s(frozen=True)
class WaterfallMap(object):
    """Describes how data will get stored into a Snapshot as we go through
    the a Table.

    The Waterfall class (defined below) reads the rows of a table in order.
    As it goes along, it sums a metric (stored in value_column), but saves
    according to a key (given by key_value).

    For example if you had a table that included two columns Account and
    Amount, you could sum Amount by Account:

    +----------+-----+--------+-----+
    | Account  | ... | Amount | ... |
    +----------+-----+--------+-----+
    | Checking | ... | 100    | ... |
    +----------+-----+--------+-----+
    | Savings  | ... | 5      | ... |
    +----------+-----+--------+-----+
    | Checking | ... | -70    | ... |
    +----------+-----+--------+-----+
    | Other    | ... | 25     | ... |
    +----------+-----+--------+-----+
    | Savings  | ... | 5      | ... |
    +----------+-----+--------+-----+

    Would yield a Snapshot that looks like:
    {"Checking": 30, "Savings": 10, "Other": 15}

    Waterfall allows for multiple key/values pairs.  For example, if you had
    a record of 2-player games, and you wanted to save points, by player,
    you could pass input_maps=[WaterfallMap("Player1", "Points1"),
    WaterfallMap("Player2", "Points2")] in the following example:

    +---------+---------+---------+---------+
    | Player1 | Player2 | Points1 | Points2 |
    +---------+---------+---------+---------+
    | X       | Y       | 1       | 2       |
    +---------+---------+---------+---------+
    | Y       | Z       | 30      | 40      |
    +---------+---------+---------+---------+
    | Z       | X       | 500     | 600     |
    +---------+---------+---------+---------+

    Would yield a Snapshot that looks like:
    {"X": 601, "Y": 32, "Z": 540}

    Attributes:
        key_column: The name of the column whose value will be used as a key
            when storing to a Snapshot.
        value_column: The name of the column whose value will added to the
            Snapshot at the key.
    """
    key_column: ColumnName = attr.ib()
    value_column: ColumnName = attr.ib()


class Waterfall(Column):
    """A column which keeps a running total of some metric.

    For each row in the table, the entry of this column will be a running sum
    of previous rows that match the specified label, output_key.  To sum for
    this label, we look at previous rows to find a row where the entry in the
    key_column of one (or more) of the input_maps matches that label.  Where
    that happens, we sum the corresponding value_column.

    For example if we had a table with two input_maps, WaterfallMap(
    "Player1", "Points1") and WaterfallMap("Player2", "Points2"), along with
    the output_key, OutKey, like this:

    +---------+---------+---------+---------+--------+--------+
    | Player1 | Player2 | Points1 | Points2 | OutKey | WF_Col |
    +---------+---------+---------+---------+--------+--------+
    | X       | Y       | 1       | 2       | X      | None   |
    +---------+---------+---------+---------+--------+--------+
    | Y       | Z       | 30      | 40      | X      | 1      |
    +---------+---------+---------+---------+--------+--------+
    | Z       | X       | 500     | 600     | Z      | 40     |
    +---------+---------+---------+---------+--------+--------+
    | None    | None    | None    | None    | X      | 601    |
    +---------+---------+---------+---------+--------+--------+

    The WF_Col (this column) does the following calculations:
     - For the first row, there are no previoud rows, so it returns None.
     - The second row looks for Xs in the previous rows and finds the one with 1
       point, so the total is 1.
     - The third row looks for Z on previous rows, and finds only on the second
       row, so the total is 40.
     - The fourth row looks for X on previous rows (the fact that Player and
       Points are blank on this row doesn't affect the calculation, because it
       only computes previous rows).  It finds two Xs with a total of 601.

    This column also takes a tail_length arguments, which says how many years
    worth of data should we use to calculate the Waterfall column.  It looks
    at the date of the CellAddr and only uses the most recent cells (within
    tail_length years) that preceed it.

    As another example if we had a table with one input_map, WaterfallMap(
    "Stock", "Return"), along with the output_key, Stock (re-use is fine),
    and a tail_length of 3 years, like this:

    +----------+-------+--------+-----------+
    | Date     | Stock | Return | MovingAvg |
    +----------+-------+--------+-----------+
    | 20110101 | XYZ   | 109    | None      |
    +----------+-------+--------+-----------+
    | 20120101 | XYZ   | 91     | 109       |
    +----------+-------+--------+-----------+
    | 20130101 | XYZ   | 16     | 200       |
    +----------+-------+--------+-----------+
    | 20140101 | XYZ   | 53     | 216       |
    +----------+-------+--------+-----------+
    | 20150101 | XYZ   | 43     | 160       |
    +----------+-------+--------+-----------+
    | 20160101 | XYZ   | 71     | 112       |
    +----------+-------+--------+-----------+

    Note that Date is not a column like the others, but rather built into the
    CellAddr.

    Then MovingAvg (this column) does the following calculations:
     - For the first row, there are no previous calculations.
     - For the second, third, and fourth rows, you can add all previous rows.
     - For the fifth and sixth rows, you should only add the Return from the
       three rows immediately preceeding.

    In the implementation, the calculation is done with a running total,
    which has values added and subtracted as we move down the list.  To avoid
    having to recompute the entire column every time that we want to
    recompute part of it, we save off the running total (or Snapshot)
    periodically.  These Snapshots, along with a log of all dates encounted,
    are state kept on this column that gets loaded / saved on open / close.

    None and np.nan are read as zero.

    Our type annotations insist that Waterfall only operate on integer columns.

    Attributes:
        name: The name of the column.
        table: A pointer to the table that this column is associated with.
        required_columns: The columns that this one dependents on.
        input_maps: A list of maps describing how to read from the table into
        output_key: When we store to this column at a given date/key, we need to
            know which value from our snapshot to store.  So we look up from
            column=output_key at the same time/key to get the snapshot key to
            lookup.  That value is then stored.
        tail_length: Denote how far back we want to go in order to add up the
            metric.  This is a time difference, given as an int; so that 10000
            means 1 year.  Should only be set to a multiple of years.
        snapshot_master: Responsible for saving and loading snapshots.  We save
            a snapshot at the beginning of each quarter.
    """

    def __init__(self, name: ColumnName, table: Table,
                 required_columns: List[ColumnName],
                 input_maps: List[WaterfallMap],
                 output_key: ColumnName, tail_length_years: int = 1):
        self.required_columns = required_columns
        for col in self.required_columns:
            table.cm.get_column(col)._column_dependencies.add(name)

        self.input_maps = input_maps
        self.output_key = output_key
        self.tail_length = tail_length_years * 10000

        # Should set the name and the table.
        super().__init__(name, table)

    def _date_to_snapshot_date(self, date: Date) -> Date:
        """Maps dates to the start of a snapshot.

        We save snapshots at the start of each quarter, so this maps to the
        start of a quarter.

        Arguments:
            date: The date that we want to map.

        Returns:
            The resulting date from the map.
        """
        year_month = date // 100
        year, month = year_month // 100, year_month % 100
        quarter = (month - 1) // 3  # 0 indexed
        st_date = year * 10000 + (quarter * 3 + 1) * 100 + 1

        return st_date

    def _get_snapshot_increment(self, date: Date) -> Snapshot:
        """From the passed table, get all the data from the date in Snapshot
        form.

        Prepares a dictionary (Snapshot).  This pulls all data stored to the
        given table on the given date.  Then for each map in this Waterfall,
        pulls the map's value and adds it to Snapshot on the map's key.

        For example, if the passed table had three rows on a the passed date:

        +---------+---------+---------+---------+
        | Player1 | Player2 | Points1 | Points2 |
        +---------+---------+---------+---------+
        | X       | Y       | 1       | 2       |
        +---------+---------+---------+---------+
        | Y       | Z       | 30      | 40      |
        +---------+---------+---------+---------+
        | Z       | X       | 500     | 600     |
        +---------+---------+---------+---------+

        This function would return:
        {"X": 601, "Y": 32, "Z": 540}

        Arguments:
            date: The date for which we look up data.

        Returns:
            Snapshot representing this single date in the table.
        """
        result = defaultdict(int)
        # Get all rows for date
        for key in self.table.all_keys_for_address(CellAddr(date, self.name)):
            # All the input maps
            for in_map in self.input_maps:
                key_in_result = self.table.get_cell_value(
                    CellAddr(date, in_map.key_column), key)
                if key_in_result is None:
                    continue
                result[key_in_result] += default_or(
                    self.table.get_cell_value(
                        CellAddr(date, in_map.value_column), key))
        return result

    def _save_snapshot(self, date: Date, snapshot: Snapshot):
        """A pass-through that saves the snapshot and records the date."""
        self.snapshot_dates.push_date(date, SNAPSHOT_KEY)
        self.snapshot_master.set_date_value(date, deepcopy(snapshot))

    def refresh(self) -> None:
        """Recalculates the rows for this column.

        For each row in this column that needs a refresh, we recompute this
        value:
         - Look at all rows with an older date that is less than tail_lenth old.
         - For each input_map in self.input_maps, if the input_map's key_column
           matches the label in the target row / output_key column, then sum the
           input_map's value (number in value_column).

        In implementation, we save time by keeping a running total.  As well,
        we periodically save off the dictionary of running totals, called
        Snapshots, using the SnapshotManager.  When we only need to update
        part of the column, we save time by starting with the latest snapshot
        from before the first cell that needs updating.

        Arguments:
            table: The table that we use to read and write cells, and to see
                which cells need refreshing.  Should match the table used to
                save snapshots.
        """
        if self.readonly:
            return

        # Look for the most recent snapshot, and start working with that.
        min_date = min(ca.date for ca in self.table.need_refresh[self.name])
        first_snapshot_ind = self.snapshot_dates.smallest_ind_gt_date(
            min_date) - 1
        if first_snapshot_ind == -1:
            # No previously-saved snapshot found.
            first_snapshot_date = self._date_to_snapshot_date(min_date)
            working_snapshot: Snapshot = defaultdict(int)
        else:
            first_snapshot_date = self.snapshot_dates.dates[first_snapshot_ind]
            working_snapshot = self.snapshot_master.get_date_value(
                first_snapshot_date)  # type: Snapshot
            if working_snapshot is None:
                # This should never happen.
                raise LookupError(
                    "Couldn't load snapshot that is expected to exist.")

        # Defines the window for which the snapshot represents the sum.  Defined
        # by spanning dates in [window_st, window_en).
        window_st = first_snapshot_date - self.tail_length
        window_en = first_snapshot_date
        window_dates = self.table.ds.slice(window_st, window_en - 1)

        # These are the dates for which we need to update this cell.
        cell_dates_to_update = self.table.ds.slice(st_date=first_snapshot_date)

        # Make a sorted list of the snapshots we will encounter along the way.
        # These will have to be updated as we pass them.
        snapshot_dates_to_update = sorted(
            list(set(map(self._date_to_snapshot_date, cell_dates_to_update))))

        def _expire_less_than(d):
            """Deletes all the entries in window_dates that have a date less
            than d."""
            nonlocal window_dates
            while window_dates and window_dates[0] < d:
                for k, v in self._get_snapshot_increment(
                        window_dates[0]).items():
                    working_snapshot[k] -= v
                window_dates = window_dates[1:]

        while cell_dates_to_update:
            # Check if any snapshots need to be saved off.
            while snapshot_dates_to_update and snapshot_dates_to_update[0] <= \
                    cell_dates_to_update[0]:
                next_snapshot_date = snapshot_dates_to_update[0]
                snapshot_dates_to_update = snapshot_dates_to_update[1:]
                _expire_less_than(next_snapshot_date - self.tail_length)
                self._save_snapshot(next_snapshot_date, working_snapshot)

            # Update next cell
            next_cell_date = cell_dates_to_update[0]
            cell_dates_to_update = cell_dates_to_update[1:]
            window_dates.append(next_cell_date)

            # Reduce for dates falling out of range.
            _expire_less_than(next_cell_date - self.tail_length)

            # Update this column.
            next_cell_addr = CellAddr(next_cell_date, self.name)
            for k in self.table.all_keys_for_address(next_cell_addr):
                key_in_snapshot = self.table.get_cell_value(
                    CellAddr(next_cell_date, self.output_key), k)
                self.table.set_cell_value(
                    next_cell_addr, k, working_snapshot[key_in_snapshot])

            # Increase working_snapshot for the new date.
            for k, v in self._get_snapshot_increment(next_cell_date).items():
                working_snapshot[k] += v

    def open(self, table: Table, readonly: bool = False) -> None:
        """Load up the snapshot data, from disk."""
        super().open(table, readonly=readonly)

        self.snapshot_master = SnapshotMaster(
            "SNAPSHOT_{}:{}".format(self.table.prefix, self.name), readonly)
        self.snapshot_dates = DateSet(
            "SNAPSHOT_{}:{}".format(self.table.prefix, self.name), readonly)

        self.snapshot_master.open()
        self.snapshot_dates.open()

    def close(self) -> None:
        """Save off the snapshot data. from disk."""
        if self.readonly:
            return

        self.snapshot_master.close()
        self.snapshot_dates.close()

        # Clear.
        self.snapshot_master = None
        self.snapshot_dates = None

        # Should clear reference to table.
        super().close()
