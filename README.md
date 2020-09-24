# Mock_SQLite
This is open source simulated SQLite code. Some of the basic functions of SQLite are simulated and applied in Python.

# Project Database Simulation
SQLite is a lightweight database system similar to Access. But smaller, faster, larger capacity, and higher concurrency. Why SQLite is best for CMS? Not that other databases are bad, oracle, MySQL, and SQLServer are also excellent DBS. They just have different design goals and different features. So only more applicable to an application scenario, there is no absolute good or bad.

## Name: Tianrui Liu

Comments: Based on the Database completion.
          Add DESC functions in database, table and class respectively.
          Add drop functions in database, table and class respectively.
          Iterator functions are created using the itertools efficient loop.
          Similar to the Database implementation.
          Use regular expressions to split strings. Pay attention to special cases.
          Multiple loop nesting and use.
          I think my split string is a little bit clearer than in the video. Accordingly,
          the statement will also become very concise.
          Insertion and creation are straightforward. But lookups are not optimized.
          I wanted to optimize the sort algorithm, but I didn't think of anything new. You can only use loops.
          The distinction between double and single quotes allows you to format strings.
          Identify the columns that match the name of the table.
          Create a Qualified class to analyze columns with table names.
          Create a Join class to analyze whether to use the Join keyword in SQL.
          Modify the insert so that multiple inserts can be inserted.
          And you can insert multiple entries according to different columns.
          Add delete and update functions in database, table and class respectively.
          Create the begin, commit, rollback function to complete the transaction number.
          The transaction mode selects three transactions to occur.
          For rollback recovery operations.
          Create the reserved_lock function for insert, delete, update
          Create the shared_lock function for select
          A view is a read-only statement called SELECT. They act like a table.
          Create the executemany, create_collat function to complete.
          Parameterized queries make it easy to reuse wildcards with variables
          extracted from the programming language interface
          The custom collation feature allows the user to provide the ability to specify how columns are sorted.
          Support for aggregation: minimum and maximum.
          
          
# Sources:

http://www.voidcn.com/article/p-mmeadbuf-byw.html
http://cn.voidcc.com/question/p-brfuseqk-baa.html
https://docs.python.org/3/
https://www.w3schools.com/python/python_mysql_getstarted.asp
https://www.python-course.eu/sql_python.php
https://docs.python.org/zh-cn/3/library/itertools.html
