"""Tests for derived_columns.

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

from derived_columns import *
from tests.test_table import *


class MockSnapshotMaster(SnapshotMaster):
    def __init__(self, prefix: Optional[Text],
                 fake_files: Optional[Dict] = None):
        self.load_log = list()
        self.save_log = list()
        self.fake_files = dict()
        if fake_files is not None:
            self.fake_files = fake_files

        super().__init__(prefix)

    def _load_file(self, path: Text) -> Any:
        self.load_log.append(path)
        if path in self.fake_files:
            return self.fake_files[path]
        return dict()

    def _save_file(self, object: Any, path: Text) -> None:
        self.save_log.append((path, deepcopy(object)))
        self.fake_files[path] = deepcopy(object)

    def open(self) -> None:
        """Don't touch any files."""
        pass

    def close(self) -> None:
        """Don't touch any files."""
        pass


class MockWaterfall(Waterfall):
    def __init__(self, name: ColumnName, table: Table,
                 required_columns: List[ColumnName], maps: List[WaterfallMap],
                 output_key: ColumnName, tail_length_years: int = 1):
        super().__init__(name, table, required_columns, maps,
                         output_key, tail_length_years)
        self.snapshot_master = MockSnapshotMaster(
            "SNAPSHOT_MASTER_{}".format(name))
        self.snapshot_dates = MockDateSet("SNAPSHOT_MASTER_{}".format(name))

    def _date_to_snapshot_date(self, date: Date) -> Date:
        """Round down to 100000, so that this won't affect most tests."""
        return date // 100000 * 100000

    def open(self) -> None:
        """Don't touch any files."""
        pass

    def close(self) -> None:
        """Don't touch any files."""
        pass


class TestSimpleFormula(unittest.TestCase):

    def setUp(self) -> None:
        # Create a table with basic columns, X and Y
        self.table = MockTable(TEST_PREFIX)
        self.col_x = FlatColumn("X", self.table)
        self.col_y = FlatColumn("Y", self.table)

        # A basic function that we will use through out
        self.sq = lambda dct: dct["X"] ** 2

    def test_setup(self):
        column = SimpleFormula(name="X_sq", table=self.table,
                               f=self.sq, required_columns=["X"])

        self.assertEqual(column.f({"X": 2}), 4)
        self.assertEqual(column.f({"X": 3}), 9)
        self.assertEqual(column.f({"X": 4, "Y": 100}),
                         16)  # y should have no effect
        self.assertListEqual(column.required_columns, ["X"])
        self.assertSetEqual(self.col_x.dependencies(), {"X_sq"})

    def test_refresh(self):
        self.table.set_cell_value(CellAddr(20010101, "X"), "key_1", 10)
        self.table.set_cell_value(CellAddr(20010101, "X"), "key_2", 20)
        self.table.set_cell_value(CellAddr(20010102, "X"), "key_1", 30)
        self.table.set_cell_value(CellAddr(20010103, "X"), "key_1", 40)

        x_sq = SimpleFormula(name="X_sq", table=self.table,
                             f=self.sq, required_columns=["X"])

        # These are just here to depend on x_sq.
        dep_1 = SimpleFormula(name="dep_1", table=self.table,
                              f=lambda dct: 1, required_columns=["X_sq"])
        dep_2 = SimpleFormula(name="dep_2", table=self.table,
                              f=lambda dct: 1, required_columns=["X_sq"])
        # Typically, the table refreshes new columns; override for the test.
        self.table.need_refresh["dep_1"] = set()
        self.table.need_refresh["dep_2"] = set()

        # Manually set need_refresh.  Don't update the middle date.
        self.table.need_refresh["X_sq"] = {CellAddr(20010101, "X_sq"),
                                           CellAddr(20010103, "X_sq")}

        x_sq.refresh()
        # This is usually done by the table's refresh function, but I want to
        # pause before it updates dependencies.
        self.table.need_refresh["X_sq"] = set()

        # Check that the cells are all set the way we expect.
        self.assertEqual(
            self.table.get_cell_value(CellAddr(20010101, "X_sq"), "key_1"),
            100)
        self.assertEqual(
            self.table.get_cell_value(CellAddr(20010101, "X_sq"), "key_2"),
            400)
        self.assertEqual(
            self.table.get_cell_value(CellAddr(20010102, "X_sq"), "key_1"),
            None)  # Never updated.
        self.assertEqual(
            self.table.get_cell_value(CellAddr(20010103, "X_sq"), "key_1"),
            1600)

        self.maxDiff = None
        # The dependencies should be marked for update now.
        self.assertDictEqual(self.table.need_refresh, {
            "X_sq": set(), "X": set(), "Y": set(),
            "dep_1": {CellAddr(20010101, "dep_1"),
                      CellAddr(20010103, "dep_1")},
            "dep_2": {CellAddr(20010101, "dep_2"),
                      CellAddr(20010103, "dep_2")}})


class TestWaterfall(unittest.TestCase):

    def test_one_map(self) -> None:
        table = MockTable(TEST_PREFIX)
        account_col = FlatColumn("account", table)
        amount_col = FlatColumn("amount", table)

        table.set_cell_value(CellAddr(1, "account"), "row_1", "Checking")
        table.set_cell_value(CellAddr(1, "amount"), "row_1", 100)
        table.set_cell_value(CellAddr(1, "account"), "row_2", "Savings")
        table.set_cell_value(CellAddr(1, "amount"), "row_2", 5)
        table.set_cell_value(CellAddr(1, "account"), "row_3", "Checking")
        table.set_cell_value(CellAddr(1, "amount"), "row_3", -70)
        table.set_cell_value(CellAddr(1, "account"), "row_4", "Other")
        table.set_cell_value(CellAddr(1, "amount"), "row_4", 25)
        table.set_cell_value(CellAddr(1, "account"), "row_5", "Savings")
        table.set_cell_value(CellAddr(1, "amount"), "row_5", 5)

        table.set_cell_value(CellAddr(2, "account"), "row_1", "Checking")
        table.set_cell_value(CellAddr(2, "account"), "row_2", "Savings")
        table.set_cell_value(CellAddr(2, "account"), "row_3", "Other")

        waterfall_col = MockWaterfall(name="balance", table=table,
                                      required_columns=["account", "amount"],
                                      maps=[WaterfallMap("account", "amount")],
                                      output_key="account")

        table.refresh()

        # At time 1, no running total.
        self.assertEqual(
            table.get_cell_value(CellAddr(1, "balance"), "row_1"), 0)
        self.assertEqual(
            table.get_cell_value(CellAddr(1, "balance"), "row_2"), 0)
        self.assertEqual(
            table.get_cell_value(CellAddr(1, "balance"), "row_3"), 0)
        self.assertEqual(
            table.get_cell_value(CellAddr(1, "balance"), "row_4"), 0)
        self.assertEqual(
            table.get_cell_value(CellAddr(1, "balance"), "row_5"), 0)

        # Totals from time 1.
        self.assertEqual(
            table.get_cell_value(CellAddr(2, "balance"), "row_1"),
            30)  # Checking
        self.assertEqual(
            table.get_cell_value(CellAddr(2, "balance"), "row_2"),
            10)  # Savings
        self.assertEqual(
            table.get_cell_value(CellAddr(2, "balance"), "row_3"),
            25)  # Other

    def test_one_map_multiple_dates(self) -> None:
        table = MockTable(TEST_PREFIX)
        account_col = FlatColumn("account", table)
        amount_col = FlatColumn("amount", table)

        table.set_cell_value(CellAddr(1, "account"), "row_1", "Checking")
        table.set_cell_value(CellAddr(1, "amount"), "row_1", 100)
        table.set_cell_value(CellAddr(1, "account"), "row_2", "Savings")
        table.set_cell_value(CellAddr(1, "amount"), "row_2", 5)
        table.set_cell_value(CellAddr(1, "account"), "row_3", "Checking")
        table.set_cell_value(CellAddr(1, "amount"), "row_3", -70)
        table.set_cell_value(CellAddr(1, "account"), "row_4", "Other")
        table.set_cell_value(CellAddr(1, "amount"), "row_4", 25)
        table.set_cell_value(CellAddr(1, "account"), "row_5", "Savings")
        table.set_cell_value(CellAddr(1, "amount"), "row_5", 5)

        # Each type shows up once.
        table.set_cell_value(CellAddr(2, "account"), "row_1", "Checking")
        table.set_cell_value(CellAddr(2, "amount"), "row_1", 10000)
        table.set_cell_value(CellAddr(2, "account"), "row_2", "Savings")
        table.set_cell_value(CellAddr(2, "amount"), "row_2", 20000)
        table.set_cell_value(CellAddr(2, "account"), "row_3", "Other")
        table.set_cell_value(CellAddr(2, "amount"), "row_3", 30000)

        # Checking shows up three times.
        table.set_cell_value(CellAddr(3, "account"), "row_1", "Checking")
        table.set_cell_value(CellAddr(3, "amount"), "row_1", 1)
        table.set_cell_value(CellAddr(3, "account"), "row_2", "Checking")
        table.set_cell_value(CellAddr(3, "amount"), "row_2", 2)
        table.set_cell_value(CellAddr(3, "account"), "row_3", "Checking")
        table.set_cell_value(CellAddr(3, "amount"), "row_3", 3)

        # New account type should also be fine.
        table.set_cell_value(CellAddr(4, "account"), "row_1", "New Acct")
        table.set_cell_value(CellAddr(4, "amount"), "row_1", 12345)

        table.set_cell_value(CellAddr(5, "account"), "row_1", "Checking")
        table.set_cell_value(CellAddr(5, "account"), "row_2", "Savings")
        table.set_cell_value(CellAddr(5, "account"), "row_3", "Other")
        table.set_cell_value(CellAddr(5, "account"), "row_4", "New Acct")

        waterfall_col = MockWaterfall(name="balance", table=table,
                                      required_columns=["account", "amount"],
                                      maps=[WaterfallMap("account", "amount")],
                                      output_key="account")

        table.refresh()

        # At time 1, no running total.
        self.assertEqual(
            table.get_cell_value(CellAddr(1, "balance"), "row_1"), 0)
        self.assertEqual(
            table.get_cell_value(CellAddr(1, "balance"), "row_2"), 0)
        self.assertEqual(
            table.get_cell_value(CellAddr(1, "balance"), "row_3"), 0)
        self.assertEqual(
            table.get_cell_value(CellAddr(1, "balance"), "row_4"), 0)
        self.assertEqual(
            table.get_cell_value(CellAddr(1, "balance"), "row_5"), 0)

        # Totals from time 1.
        self.assertEqual(
            table.get_cell_value(CellAddr(2, "balance"), "row_1"),
            30)  # Checking
        self.assertEqual(
            table.get_cell_value(CellAddr(2, "balance"), "row_2"),
            10)  # Savings
        self.assertEqual(
            table.get_cell_value(CellAddr(2, "balance"), "row_3"),
            25)  # Other

        # Totals from time 2 for checking stored to each row.
        self.assertEqual(
            table.get_cell_value(CellAddr(3, "balance"), "row_1"),
            10030)  # Checking
        self.assertEqual(
            table.get_cell_value(CellAddr(3, "balance"), "row_2"),
            10030)  # Checking
        self.assertEqual(
            table.get_cell_value(CellAddr(3, "balance"), "row_3"),
            10030)  # Checking

        # New account has no totals yet
        self.assertEqual(
            table.get_cell_value(CellAddr(4, "balance"), "row_1"),
            0)  # Checking

        # Accumulation of everything.
        self.assertEqual(
            table.get_cell_value(CellAddr(5, "balance"), "row_1"),
            10036)  # Checking
        self.assertEqual(
            table.get_cell_value(CellAddr(5, "balance"), "row_2"),
            20010)  # Savings
        self.assertEqual(
            table.get_cell_value(CellAddr(5, "balance"), "row_3"),
            30025)  # Other
        self.assertEqual(
            table.get_cell_value(CellAddr(5, "balance"), "row_4"),
            12345)  # New Acct

    def test_multiple_maps(self) -> None:
        table = MockTable(TEST_PREFIX)
        player1_col = FlatColumn("Player1", table)
        player2_col = FlatColumn("Player2", table)
        points1_col = FlatColumn("Points1", table)
        points2_col = FlatColumn("Points2", table)

        table.set_cell_value(CellAddr(1, "Player1"), "row_1", "X")
        table.set_cell_value(CellAddr(1, "Player2"), "row_1", "Y")
        table.set_cell_value(CellAddr(1, "Points1"), "row_1", 1)
        table.set_cell_value(CellAddr(1, "Points2"), "row_1", 2)
        table.set_cell_value(CellAddr(1, "Player1"), "row_2", "Y")
        table.set_cell_value(CellAddr(1, "Player2"), "row_2", "Z")
        table.set_cell_value(CellAddr(1, "Points1"), "row_2", 30)
        table.set_cell_value(CellAddr(1, "Points2"), "row_2", 40)
        table.set_cell_value(CellAddr(1, "Player1"), "row_3", "Z")
        table.set_cell_value(CellAddr(1, "Player2"), "row_3", "X")
        table.set_cell_value(CellAddr(1, "Points1"), "row_3", 500)
        table.set_cell_value(CellAddr(1, "Points2"), "row_3", 600)

        table.set_cell_value(CellAddr(2, "Player1"), "row_1", "X")
        table.set_cell_value(CellAddr(2, "Player2"), "row_1", "Y")
        table.set_cell_value(CellAddr(2, "Player1"), "row_2", "X")
        table.set_cell_value(CellAddr(2, "Player2"), "row_2", "Z")

        player1_wf = MockWaterfall(name="player1_wf", table=table,
                                   required_columns=["Player1", "Player2",
                                                     "Points1", "Points2"],
                                   maps=[WaterfallMap("Player1", "Points1"),
                                         WaterfallMap("Player2", "Points2")],
                                   output_key="Player1")
        player2_wf = MockWaterfall(name="player2_wf", table=table,
                                   required_columns=["Player1", "Player2",
                                                     "Points1", "Points2"],
                                   maps=[WaterfallMap("Player1", "Points1"),
                                         WaterfallMap("Player2", "Points2")],
                                   output_key="Player2")

        table.refresh()

        self.assertEqual(
            table.get_cell_value(CellAddr(2, "player1_wf"), "row_1"),
            601)  # Player X
        self.assertEqual(
            table.get_cell_value(CellAddr(2, "player1_wf"), "row_2"),
            601)  # Player X
        self.assertEqual(
            table.get_cell_value(CellAddr(2, "player2_wf"), "row_1"),
            32)  # Player Y
        self.assertEqual(
            table.get_cell_value(CellAddr(2, "player2_wf"), "row_2"),
            540)  # Player Z

    def test_multiple_maps_missing_data(self) -> None:
        table = MockTable(TEST_PREFIX)
        player1_col = FlatColumn("Player1", table)
        player2_col = FlatColumn("Player2", table)
        points1_col = FlatColumn("Points1", table)
        points2_col = FlatColumn("Points2", table)

        table.set_cell_value(CellAddr(1, "Player1"), "row_1", "X")
        table.set_cell_value(CellAddr(1, "Player2"), "row_1", "Y")
        table.set_cell_value(CellAddr(1, "Points1"), "row_1", 1)
        table.set_cell_value(CellAddr(1, "Points2"), "row_1", 2)
        # Date = 2 is missing Player2, should be okay.
        table.set_cell_value(CellAddr(2, "Player1"), "row_2", "Y")
        table.set_cell_value(CellAddr(2, "Points1"), "row_2", 30)
        # Date = 3 is missing points for both players, still okay.
        table.set_cell_value(CellAddr(3, "Player1"), "row_3", "Z")
        table.set_cell_value(CellAddr(3, "Player2"), "row_3", "X")

        table.set_cell_value(CellAddr(4, "Player1"), "row_1", "X")
        table.set_cell_value(CellAddr(4, "Player2"), "row_1", "Y")
        table.set_cell_value(CellAddr(4, "Player1"), "row_2", "X")
        table.set_cell_value(CellAddr(4, "Player2"), "row_2", "Z")

        player1_wf = MockWaterfall(name="player1_wf", table=table,
                                   required_columns=["Player1", "Player2",
                                                     "Points1", "Points2"],
                                   maps=[WaterfallMap("Player1", "Points1"),
                                         WaterfallMap("Player2", "Points2")],
                                   output_key="Player1")
        player2_wf = MockWaterfall(name="player2_wf", table=table,
                                   required_columns=["Player1", "Player2",
                                                     "Points1", "Points2"],
                                   maps=[WaterfallMap("Player1", "Points1"),
                                         WaterfallMap("Player2", "Points2")],
                                   output_key="Player2")

        table.refresh()

        self.assertEqual(
            table.get_cell_value(CellAddr(4, "player1_wf"), "row_1"),
            1)  # Player X
        self.assertEqual(
            table.get_cell_value(CellAddr(4, "player1_wf"), "row_2"),
            1)  # Player X
        self.assertEqual(
            table.get_cell_value(CellAddr(4, "player2_wf"), "row_1"),
            32)  # Player Y
        self.assertEqual(
            table.get_cell_value(CellAddr(4, "player2_wf"), "row_2"),
            0)  # Player Z

    def test_expiry(self) -> None:
        table = MockTable(TEST_PREFIX)
        player1_col = FlatColumn("Player1", table)
        points1_col = FlatColumn("Points1", table)

        table.set_cell_value(CellAddr(10000, "Player1"), "row_1", "X")
        table.set_cell_value(CellAddr(10000, "Points1"), "row_1", 1)
        table.set_cell_value(CellAddr(20000, "Player1"), "row_1", "X")
        table.set_cell_value(CellAddr(20000, "Points1"), "row_1", 20)
        table.set_cell_value(CellAddr(30000, "Player1"), "row_1", "X")
        table.set_cell_value(CellAddr(30000, "Points1"), "row_1", 300)
        table.set_cell_value(CellAddr(40000, "Player1"), "row_1", "X")
        table.set_cell_value(CellAddr(40000, "Points1"), "row_1", 4000)
        table.set_cell_value(CellAddr(50000, "Player1"), "row_1", "X")
        table.set_cell_value(CellAddr(50000, "Points1"), "row_1", 50000)
        table.set_cell_value(CellAddr(60000, "Player1"), "row_1", "X")

        # Set a 3 year tail length
        player1_wf = MockWaterfall(name="player1_wf", table=table,
                                   required_columns=["Player1", "Points1"],
                                   maps=[WaterfallMap("Player1", "Points1")],
                                   output_key="Player1", tail_length_years=3)

        table.refresh()

        # Unset
        self.assertEqual(
            table.get_cell_value(CellAddr(10000, "player1_wf"), "row_1"), 0)
        # Usual addition
        self.assertEqual(
            table.get_cell_value(CellAddr(20000, "player1_wf"), "row_1"), 1)
        self.assertEqual(
            table.get_cell_value(CellAddr(30000, "player1_wf"), "row_1"), 21)
        self.assertEqual(
            table.get_cell_value(CellAddr(40000, "player1_wf"), "row_1"), 321)
        # Now older data starts expiring.
        self.assertEqual(
            table.get_cell_value(CellAddr(50000, "player1_wf"), "row_1"), 4320)
        self.assertEqual(
            table.get_cell_value(CellAddr(60000, "player1_wf"), "row_1"), 54300)

    def test_snapshot_saving(self) -> None:
        table = MockTable(TEST_PREFIX)
        player1_col = FlatColumn("Player1", table)
        points1_col = FlatColumn("Points1", table)

        table.set_cell_value(CellAddr(50000, "Player1"), "row_1", "X")
        table.set_cell_value(CellAddr(50000, "Points1"), "row_1", 1)
        # Snapshot save at 100000
        table.set_cell_value(CellAddr(150000, "Player1"), "row_1", "X")
        table.set_cell_value(CellAddr(150000, "Points1"), "row_1", 20)
        table.set_cell_value(CellAddr(180000, "Player1"), "row_1", "X")
        table.set_cell_value(CellAddr(180000, "Points1"), "row_1", 300)
        # Snapshot save at 100000
        table.set_cell_value(CellAddr(220000, "Player1"), "row_1", "X")
        table.set_cell_value(CellAddr(220000, "Points1"), "row_1", 4000)
        table.set_cell_value(CellAddr(250000, "Player1"), "row_1", "X")
        table.set_cell_value(CellAddr(250000, "Points1"), "row_1", 50000)
        # Snapshot save at 300000
        table.set_cell_value(CellAddr(300000, "Player1"), "row_1", "X")
        table.set_cell_value(CellAddr(300000, "Points1"), "row_1", 600000)

        # Set a very long tail length
        player1_wf = MockWaterfall(name="player1_wf", table=table,
                                   required_columns=["Player1", "Points1"],
                                   maps=[WaterfallMap("Player1", "Points1")],
                                   output_key="Player1", tail_length_years=100)

        table.refresh()

        # Unset
        self.assertEqual(
            table.get_cell_value(CellAddr(50000, "player1_wf"), "row_1"), 0)
        # Usual addition
        self.assertEqual(
            table.get_cell_value(CellAddr(150000, "player1_wf"), "row_1"), 1)
        self.assertEqual(
            table.get_cell_value(CellAddr(180000, "player1_wf"), "row_1"), 21)
        self.assertEqual(
            table.get_cell_value(CellAddr(220000, "player1_wf"), "row_1"), 321)
        self.assertEqual(
            table.get_cell_value(CellAddr(250000, "player1_wf"), "row_1"), 4321)

        # Make sure components are being set correctly.
        self.assertListEqual(player1_wf.snapshot_dates.dates,
                             [0, 100000, 200000, 300000])
        self.assertDictEqual(player1_wf.snapshot_master.get_date_value(0),
                             dict())
        self.assertDictEqual(player1_wf.snapshot_master.get_date_value(100000),
                             {"X": 1})
        self.assertDictEqual(player1_wf.snapshot_master.get_date_value(200000),
                             {"X": 321})
        self.assertDictEqual(player1_wf.snapshot_master.get_date_value(300000),
                             {"X": 54321})
