Introduction
============

DISCLAIMER:  This is not an officially supported Google product.

This library implements a table, where the cells are referenced by row and 
column, and stored independently; with cells being dynamically loaded from 
and saved to disk as needed.  Additionally, the table keeps track of 
"dependent" cells, so that when a cell gets updated, its dependent cells will
get recalculated. 

The main method for managing dependencies and formulas for cells is through 
Columns, classes that contain logic for updating, or "refreshing," cells.  
This library implements two such Columns:  A SimpleFormula to apply 
operations on values in the same row, and a Waterfall, which is akin to a 
SUMIF on earlier rows. 

Table
=====

The basic tool from this library is a class called Table, which is 
initialized with a "prefix".  A prefix is a name for the table; a lot of 
files get saved with the table, and the prefix literally prefixes these file 
names. 

The Table class has basic set and get functions for the cells in the table, 
called set_cell_value and get_cell_value.  A cell is identified by date 
(which acts as a group of rows) and the column (identified by column name) 
and by key, which subdivides date, identifying a single row.  Date and 
column_name are stored together as a cell address (CellAddr), which 
represents a block of cells that should get updated together. 

Note: Date may be any integer, but is assumed to be in the format YYYYMMDD. 

The APIs for set_cell_value and get_cell_value are:

*  set_cell_value(cell_addr: CellAddr, key: CellKey, value: Any) -> None
*  get_cell_value(cell_addr: CellAddr, key: CellKey) -> Optional[Any]

If for example, we wanted to store the value 123.45 to the key "Checking" on 
the date Apr 24, 2017, in the deposit column, we could call: 

*  set_cell_value(CellAddr(20170424, "deposit"), key="Checking", value=1232.45)

As cells get updated, this will change the value of cells that depend on them.
This doesn't get done automatically; the user needs to call refresh() on 
the table.  The table's internals keep track of what needs to be updated and 
in what order, and will update cells accordingly. 

You can export the contents of the table to a pandas-style dataframe by 
calling make_df on the table.  This takes two arguments:  The columns that 
you want to export, and the dates that you want to export.  (Though if you 
may leave dates unset, and it will return all dates.) 

A table is loaded and saved using open() and close(), which by practice 
should be called in all case when starting and finishing working with a table.
In addition to these functions, the table will dynamically save and load 
some data in order to avoid ever having too much data held in memory.  This 
should be scalable to arbitrarily-large data sets. 

There's a long example below.

Columns
=======

To store data, we need to build Columns.

Columns are holders of logic, and sometimes state.  They don't hold any data;
that's done in the table.   At a minimum, columns need a name (a string used
to reference them; this was used on the set/get methods for example) and a 
reference to the table they're being built on. 

Our library defines 4 columns:

1.  FlatColumn
2.  ProtectedColumn
3.  SimpleFormula
4.  Waterfall

The first two are basic.  They contain no logic; they're just canvases for 
storing data.  The only difference between these is that ProtectedColumns 
hold data that wouldn't be known at the start of that date; for example a 
target variable on a model.  In the current implementation, there is no 
functional difference between FlatColumns and ProtectedColumns; the user 
might choose to differentiate as a convention. 

A SimpleFormula column uses columns from the same row to calculate the value.
It takes four arguments at initialization:

1.  A name for the column,
2.  The table that we want to add the column to,
3.  A function, f, which takes as its only argument a dict representing a row
    in the table, whose keys are the required columns and whose values are the 
    values of those columns in a row.  The output of the function is what will 
    be stored in this column for that row.
4.  Required columns - that is any column that, when refreshed, should trigger 
    a refresh for this column.

A Waterfall column adds up (recent) values from a given column, that match 
keys in another column (similar to a SUMIF from other software).  Actually we
can add multiple columns (WaterfallMap values) conditioned on multiple 
different columns (WaterfallMap keys); and the match condition we set may 
come from yet another column (output key).  The initializer takes six 
arguments: 

1.  A name for the column, 
2.  The table that we want to add the column to, 
3.  Required columns - that is any column that when refreshed should trigger 
    a refresh for this column, 
4.  Input maps - These are WaterfallMap objects that say which value to add, 
    and which key to store it to.
5.  An output key, which says which key we lookup when we actually go to save.
6.  Tail length (in years), which says how much time to use in the running 
    sum.

Example
=======

For an example, we look at some high-level Canadian Football (CFB) data.

I have some data about the final match of the Grey Cup (playoffs) from each 
year, along with scores, saved to a csv in the data folder. 

>>> import pandas as pd
>>> cfb_df = pd.read_csv("data/cfb_test_data.csv")
>>> pd.set_option('display.max_columns', None)  # Show all columns
>>> cfb_df.head()
       Date           Winning Team  Winning Points          Losing Team  \
0  20191201  Winnipeg Blue Bombers              33  Hamilton Tiger-Cats   
1  20181201     Calgary Stampeders              27     Ottawa RedBlacks   
2  20171201      Toronto Argonauts              27   Calgary Stampeders   
3  20161201       Ottawa RedBlacks              39   Calgary Stampeders   
4  20151201       Edmonton Eskimos              26     Ottawa RedBlacks   
<BLANKLINE>
   Losing Points  
0             12  
1             16  
2             24  
3             33  
4             20  

Note: I changed all the date to Nov 1st, so that if I add the last X years, 
it will do finals from the last X years, rather than X-1 to X+1 depending on 
the specific dates. 

Throughout this example, we pretend that we want to build a model that 
predicts winners, and build features to that end. 

First let's store all the data into a new table.

>>> from cell_link import *
>>> PREFIX = "CFBTEST"
>>> cfb = Table(PREFIX)
>>> cfb.open()

Make three flat columns, Date, Team1, and Team2.  These are the data that we 
know before the game happens; flat means that they aren't based on a formula.
And make two protected columns, Points1 and Points2.  These are the 
corresponding points scored.  We make these columns protected because they 
aren't available to make predictions about this game. 

>>> date_col = FlatColumn("Date", table=cfb)
>>> team_1 = FlatColumn("Team1", table=cfb)
>>> team_2 = FlatColumn("Team2", table=cfb)
>>> points_1 = ProtectedColumn("Points1", table=cfb)
>>> points_2 = ProtectedColumn("Points2", table=cfb)

Now we store data to the table.  As we do we'll randomly assign winners to be
team 1 or team 2.  This is a modeling decision not related to our CellLink 
design. 

>>> import random
>>> random.seed(1)
>>> key = "SINGLE_KEY"
>>> for _, row in cfb_df.iterrows():
...   date = row["Date"]
...   team1_map = "Winning" if random.random() < 0.5 else "Losing"
...   team2_map = "Winning" if team1_map == "Losing" else "Losing"
...   cfb.set_cell_value(CellAddr(date, "Date"), key, value=date)
...   cfb.set_cell_value(CellAddr(date, "Team1"), key, row["{} Team".format(
...                                                        team1_map)])
...   cfb.set_cell_value(CellAddr(date, "Team2"), key, row["{} Team".format(
...                                                        team2_map)])
...   cfb.set_cell_value(CellAddr(date, "Points1"), key, row["{} Points".format(
...                                                          team1_map)])
...   cfb.set_cell_value(CellAddr(date, "Points2"), key, row["{} Points".format(
...                                                          team2_map)])

Note:  As we save data to the table, we use a single key.  This is only 
because we have only one row per date.  In general, there can be multiple 
entries per date, and the key is what tells the program which row to edit. 

By setting up our data this way, we've lost track of which team won, and we 
want to add that back.  One way to do this is to build a SimpleFormula column:

>>> def winner_f(row):
...   if row["Points1"] > row["Points2"]:
...     return "1"
...   return "2"  # May assume no ties.
>>> winner = SimpleFormula("Winner", table=cfb, f=winner_f, required_columns=[
...     "Points1", "Points2"])
>>> cfb.refresh()

We can see the state of the table so far:

>>> cfb.make_df(columns=["Date", "Points1", "Points2", "Winner"],
...             dates=range(20141201, 20201201, 10000))
       Date  Points1  Points2 Winner
0  20141201       20       16      1
1  20151201       26       20      1
2  20161201       39       33      1
3  20171201       24       27      2
4  20181201       16       27      2
5  20191201       33       12      1

The advantage of the cell-link table is that we can update cells, and 
dependent fields will update in turn. 

>>> cfb.set_cell_value(CellAddr(20141201, "Points2"), key, 100)
>>> cfb.refresh()
>>> cfb.make_df(columns=["Date", "Points1", "Points2", "Winner"],
...             dates=range(20141201, 20201201, 10000))
       Date  Points1  Points2 Winner
0  20141201       20      100      2
1  20151201       26       20      1
2  20161201       39       33      1
3  20171201       24       27      2
4  20181201       16       27      2
5  20191201       33       12      1

Now let's build some features.  Maybe a predictive feature would be how many 
times the team has made it to the final game in the last ten years.  We can 
calculate this with the Waterfall column: 

First we will need to make a column of ones, because Waterfall always 
operates by adding another column.  For us, we will add 1 if the team is 
present in prior rows.  To fill out a row, we can take advantage of 
dictionary of dates/keys stored on the table, called cfb.ds.dates_keys. 

>>> ones = FlatColumn("Ones", table=cfb)
>>> for date, keys in cfb.ds.dates_keys.items():
...   for key in keys:  # Should only be one for us.
...     cfb.set_cell_value(CellAddr(date, "Ones"), key, 1)

Now we build Waterfall columns for both Player1 and Player2.  For this column, we set:

*  Input maps - We sum the Ones column for each row (the number 1), but we add
   this for both the "Team1" key and the "Team2" key.
*  Output key - For Team1 10yr Appearances, we look at Team1 to know which sum
   (of those we calculated) we should look at.
*  Tail length - to count pariticipation in the previous ten years, we set this
   to 10. 

>>> team1_10yr_appearances = Waterfall(
...     "Team1 10yr Appearances", table=cfb,
...     required_columns=["Team1", "Team2", "Ones"],
...     input_maps=[WaterfallMap("Team1", "Ones"),
...                 WaterfallMap("Team2", "Ones")],
...     output_key="Team1",
...     tail_length_years=10)
>>> team2_10yr_appearances = Waterfall(
...     "Team2 10yr Appearances", table=cfb,
...     required_columns=["Team1", "Team2", "Ones"],
...     input_maps=[WaterfallMap("Team1", "Ones"),
...                 WaterfallMap("Team2", "Ones")],
...     output_key="Team2",
...     tail_length_years=10)
>>> cfb.refresh()

We can now see these columns:

>>> cfb.make_df(columns=["Date", "Team1", "Team2", "Team1 10yr Appearances",
...                      "Team2 10yr Appearances"],
...             dates=range(20041201, 20201201, 10000))
        Date                     Team1                     Team2  \
0   20041201                  BC Lions         Toronto Argonauts   
1   20051201          Edmonton Eskimos        Montreal Alouettes   
2   20061201                  BC Lions        Montreal Alouettes   
3   20071201     Winnipeg Blue Bombers  Saskatchewan Roughriders   
4   20081201        Calgary Stampeders        Montreal Alouettes   
5   20091201  Saskatchewan Roughriders        Montreal Alouettes   
6   20101201        Montreal Alouettes  Saskatchewan Roughriders   
7   20111201                  BC Lions     Winnipeg Blue Bombers   
8   20121201        Calgary Stampeders         Toronto Argonauts   
9   20131201       Hamilton Tiger-Cats  Saskatchewan Roughriders   
10  20141201        Calgary Stampeders       Hamilton Tiger-Cats   
11  20151201          Edmonton Eskimos          Ottawa RedBlacks   
12  20161201          Ottawa RedBlacks        Calgary Stampeders   
13  20171201        Calgary Stampeders         Toronto Argonauts   
14  20181201          Ottawa RedBlacks        Calgary Stampeders   
15  20191201     Winnipeg Blue Bombers       Hamilton Tiger-Cats   
<BLANKLINE>
    Team1 10yr Appearances  Team2 10yr Appearances  
0                        2                       2  
1                        3                       3  
2                        2                       4  
3                        1                       1  
4                        3                       5  
5                        1                       6  
6                        7                       2  
7                        2                       2  
8                        1                       1  
9                        0                       3  
10                       2                       1  
11                       1                       0  
12                       1                       3  
13                       4                       1  
14                       2                       5  
15                       1                       2  

In 2017, Team 1 was the Stampeders, and we say that they've had 4 recent 
appearances.  We can see that these are from years 08, 12, 14, and 16.  On 
the other hand, in 2015, the RedBlacks have no recent appearances.  (This is 
actually their first time in the final round since their founding in 2010.) 

Another potentially predictive variable is point differential (PD), defined 
as points scored (in past games) minus points opponent scored.  To calculate 
this, we first calculate points for (PF) and points against (PA) for the last
ten years. 

>>> pf1 = Waterfall("PF1", table=cfb,
...     required_columns=["Team1", "Team2", "Points1", "Points2"],
...     input_maps=[WaterfallMap("Team1", "Points1"),
...                 WaterfallMap("Team2", "Points2")],
...     output_key="Team1", tail_length_years=10)
>>> pf2 = Waterfall("PF2", table=cfb,
...     required_columns=["Team1", "Team2", "Points1", "Points2"],
...     input_maps=[WaterfallMap("Team1", "Points1"),
...                 WaterfallMap("Team2", "Points2")],
...     output_key="Team2", tail_length_years=10)
>>> pa1 = Waterfall("PA1", table=cfb,
...     required_columns=["Team1", "Team2", "Points1", "Points2"],
...     input_maps=[WaterfallMap("Team1", "Points2"),
...                 WaterfallMap("Team2", "Points1")],
...     output_key="Team1", tail_length_years=10)
>>> pa2 = Waterfall("PA2", table=cfb,
...     required_columns=["Team1", "Team2", "Points1", "Points2"],
...     input_maps=[WaterfallMap("Team1", "Points2"),
...                 WaterfallMap("Team2", "Points1")],
...     output_key="Team2", tail_length_years=10)

Now we put SimpleFormulas on top of the new fields.

>>> pd1 = SimpleFormula("PD1", table=cfb, f=lambda row: row["PF1"]-row["PA1"],
...                     required_columns=["PF1", "PA1"])
>>> pd2 = SimpleFormula("PD2", table=cfb, f=lambda row: row["PF2"]-row["PA2"],
...                     required_columns=["PF2", "PA2"])
>>> cfb.refresh()

And display:

>>> cfb.make_df(columns=["Date", "Team1", "PD1", "Team2", "PD2"],
...             dates=range(20091201, 20201201, 10000))
        Date                     Team1  PD1                     Team2  PD2
0   20091201  Saskatchewan Roughriders    4        Montreal Alouettes  -27
1   20101201        Montreal Alouettes  -26  Saskatchewan Roughriders    3
2   20111201                  BC Lions    3     Winnipeg Blue Bombers  -12
3   20121201        Calgary Stampeders    8         Toronto Argonauts    8
4   20131201       Hamilton Tiger-Cats    0  Saskatchewan Roughriders    0
5   20141201        Calgary Stampeders   -5       Hamilton Tiger-Cats  -22
6   20151201          Edmonton Eskimos    3          Ottawa RedBlacks    0
7   20161201          Ottawa RedBlacks   -6        Calgary Stampeders  -85
8   20171201        Calgary Stampeders  -91         Toronto Argonauts   13
9   20181201          Ottawa RedBlacks    0        Calgary Stampeders  -94
10  20191201     Winnipeg Blue Bombers  -11       Hamilton Tiger-Cats   58

We could go even further and calculate points differential per game (PDPG).  
Notice we need to handle the denominator = 0. 

>>> def pdpg_f(row, player_no):
...   appearances = row["Team{} 10yr Appearances".format(player_no)]
...   if appearances == 0:
...     return None
...   return row["PD{}".format(player_no)] / appearances
>>> pdpg1 = SimpleFormula("PDPG1", table=cfb, f=lambda row: pdpg_f(row, "1"),
...                       required_columns=["Team1 10yr Appearances", "PD1"])
>>> pdpg2 = SimpleFormula("PDPG2", table=cfb, f=lambda row: pdpg_f(row, "2"),
...                       required_columns=["Team2 10yr Appearances", "PD2"])
>>> cfb.refresh()
>>> cfb.make_df(columns=["Date", "Team1", "PDPG1", "Team2", "PDPG2"],
...             dates=range(20091201, 20201201, 10000))
        Date                     Team1      PDPG1                     Team2  \
0   20091201  Saskatchewan Roughriders   4.000000        Montreal Alouettes   
1   20101201        Montreal Alouettes  -3.714286  Saskatchewan Roughriders   
2   20111201                  BC Lions   1.500000     Winnipeg Blue Bombers   
3   20121201        Calgary Stampeders   8.000000         Toronto Argonauts   
4   20131201       Hamilton Tiger-Cats        NaN  Saskatchewan Roughriders   
5   20141201        Calgary Stampeders  -2.500000       Hamilton Tiger-Cats   
6   20151201          Edmonton Eskimos   3.000000          Ottawa RedBlacks   
7   20161201          Ottawa RedBlacks  -6.000000        Calgary Stampeders   
8   20171201        Calgary Stampeders -22.750000         Toronto Argonauts   
9   20181201          Ottawa RedBlacks   0.000000        Calgary Stampeders   
10  20191201     Winnipeg Blue Bombers -11.000000       Hamilton Tiger-Cats   
<BLANKLINE>
        PDPG2  
0   -4.500000  
1    1.500000  
2   -6.000000  
3    8.000000  
4    0.000000  
5  -22.000000  
6         NaN  
7  -28.333333  
8   13.000000  
9  -18.800000  
10  29.000000  

Let's say that we found out that there's a data error, and that in 2009 the 
Alouettes (Team1) actually scored 100,000 points.  We can update that field, 
and this will trigger an update of Points For, Points Against, Points 
Differential, and PDPG in that order: 

>>> cfb.set_cell_value(CellAddr(20091201, "Points1"), key, 100000)
>>> cfb.refresh()
>>> cfb.make_df(columns=["Date", "Team1", "PDPG1", "Team2", "PDPG2"],
...             dates=range(20091201, 20201201, 10000))
        Date                     Team1         PDPG1  \
0   20091201  Saskatchewan Roughriders      4.000000   
1   20101201        Montreal Alouettes -14285.571429   
2   20111201                  BC Lions      1.500000   
3   20121201        Calgary Stampeders      8.000000   
4   20131201       Hamilton Tiger-Cats           NaN   
5   20141201        Calgary Stampeders     -2.500000   
6   20151201          Edmonton Eskimos      3.000000   
7   20161201          Ottawa RedBlacks     -6.000000   
8   20171201        Calgary Stampeders    -22.750000   
9   20181201          Ottawa RedBlacks      0.000000   
10  20191201     Winnipeg Blue Bombers    -11.000000   
<BLANKLINE>
                       Team2         PDPG2  
0         Montreal Alouettes     -4.500000  
1   Saskatchewan Roughriders  49988.000000  
2      Winnipeg Blue Bombers     -6.000000  
3          Toronto Argonauts      8.000000  
4   Saskatchewan Roughriders  33324.333333  
5        Hamilton Tiger-Cats    -22.000000  
6           Ottawa RedBlacks           NaN  
7         Calgary Stampeders    -28.333333  
8          Toronto Argonauts     13.000000  
9         Calgary Stampeders    -18.800000  
10       Hamilton Tiger-Cats     29.000000  


Note:  We see that this negatively affects the Roughriders, who were their 
opponent that year. 

Another nice feature of this is that we can add a new row easily.  If we find
out that Ottawa RedBlacks are slated to play the Hamilton Tiger-Cats in 
2020, then we can add this row: 

>>> cfb.set_cell_value(CellAddr(20201201, "Team1"), key, "Ottawa RedBlacks")
>>> cfb.set_cell_value(CellAddr(20201201, "Team2"), key, "Hamilton Tiger-Cats")
>>> cfb.refresh()

This triggers a calculation for all of our fields, we can then see:

>>> cfb.make_df(columns=["Date", "Team1", "Team2", "Team1 10yr Appearances",
...     "Team2 10yr Appearances", "PD1", "PD2", "PDPG1", "PDPG2"], dates=[
...     20201201])
   Date             Team1                Team2  Team1 10yr Appearances  \
0  None  Ottawa RedBlacks  Hamilton Tiger-Cats                       3   
<BLANKLINE>
   Team2 10yr Appearances  PD1  PD2     PDPG1      PDPG2  
0                       3  -11   37 -3.666667  12.333333  

Clean-up

>>> cfb.close()
>>> for root,_, files in os.walk("data"):
...   for file in files:
...     if file.find(PREFIX) != -1:
...       os.remove(os.path.join(root, file))
