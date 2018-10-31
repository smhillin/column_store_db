-- Load+create Data and shut down of tbl1 which has 1 attribute only
create(db,"db1")
create(tbl,"tbl2",db1,4)
create(col,"col1",db1.tbl2)
create(col,"col2",db1.tbl2)
create(col,"col3",db1.tbl2)
create(col,"col4",db1.tbl2)
relational_insert(db1.tbl2,-1,-11,-111,-1111)
relational_insert(db1.tbl2,-2,-22,-222,-2222)
shutdown


