"""
Project # Database Simulation
Name: Tianrui Liu
Time to completion: 10 Hour
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

Sources: http://www.voidcn.com/article/p-mmeadbuf-byw.html
         http://cn.voidcc.com/question/p-brfuseqk-baa.html
         https://docs.python.org/3/
         https://www.w3schools.com/python/python_mysql_getstarted.asp
         https://www.python-course.eu/sql_python.php
         https://docs.python.org/zh-cn/3/library/itertools.html
"""

import string
from itertools import chain
from itertools import repeat
import copy
import functools
import re

_ALL_DATABASES = {}
transaction_modes = {}
transaction_table = {}
collation_table = {}


def syntax(statement):
    """
    Analyze the input statement and determine whether it is a specification.
    Returns the value of the containing statement.
    """
    statement = statement.strip().replace(';', '')
    regexs = re.compile(r'[(](.*)[)]', re.S)
    parentheses = re.findall(regexs, statement)
    # If there are parentheses
    if parentheses:
        statement = re.sub(r'\(.*\)', '', statement).strip()
        statement = list(statement.split(' '))
        # Case of create
        if statement[0] == "CREATE" and statement[1] == "TABLE":
            statement.append([para.split(' ') for para in parentheses[0].strip().replace("'", '').split(", ")])
        # Case of insertion
        elif statement[0] == "INSERT" and statement[1] == "INTO":
            statement.append([para.split(', ') for para in parentheses[0].strip().replace("'", '').split(", ")])
    else:
        statement = statement.strip().replace(',', '').split(' ')
    return statement


class Connection(object):
    def __init__(self, filename, timeout, isolation_level):
        """
        Takes a filename, but doesn't do anything with it.
        (The filename will be used in a future project).
        """
        self.filename = filename
        self.copy_database = Database(filename)
        self.transaction = False
        self.table_mode = dict()
        self.transaction_index = len(transaction_modes.keys())
        transaction_modes[self.transaction_index] = ""
        transaction_table[self.transaction_index] = self.table_mode

        if filename in _ALL_DATABASES:
            self.database = _ALL_DATABASES[filename]
        else:
            self.database = Database(filename)
            _ALL_DATABASES[filename] = self.database

    def executemany(self, statement, rows):
        tokens = tokenize(statement)
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"
        values = tokens[tokens.index("VALUES") + 2:len(tokens) - 1]
        for item in range(len(values) - 1, -1, -1):
            if values[item] == ",":
                values.pop(item)
        value_lists = []
        for row in rows:
            index = 0
            value_list = []
            for value in values:
                if value is "?":
                    value_list.append(row[index])
                    index += 1
                else:
                    value_list.append(value)
            value_str = "(" + ", ".join(str(x) for x in value_list) + ")"
            value_lists.append(value_str)
        return self.execute(" ".join(tokens[:tokens.index("VALUES") + 1]) + ", ".join(value_lists) + ";")

    def execute(self, statement):
        """
        Takes a SQL statement.
        Returns a list of tuples (empty unless select statement
        with rows to return).
        """
        def create_table(input_create_table):
            """
            Determines the name and column information from tokens add
            has the database create a new table within itself.
            """
            pop_and_check(input_create_table, "CREATE")
            pop_and_check(input_create_table, "TABLE")

            if input_create_table[0] == "IF":
                pop_and_check(input_create_table, "IF")
                pop_and_check(input_create_table, "NOT")
                pop_and_check(input_create_table, "EXISTS")
                if input_create_table[0] in self.database.tables:
                    return []

            table_name = input_create_table.pop(0)
            pop_and_check(input_create_table, "(")
            column_name_type_pairs = []
            while True:
                column_name = input_create_table.pop(0)
                column_name = Qualified(column_name, table_name)
                column_type = input_create_table.pop(0)
                assert column_type in {"TEXT", "INTEGER", "REAL"}
                if input_create_table[0] == "DEFAULT":
                    input_create_table.pop(0)
                    column_name.default_case = input_create_table[0]
                    input_create_table.pop(0)
                column_name_type_pairs.append((column_name, column_type))
                comma_or_close = input_create_table.pop(0)
                if comma_or_close == ")":
                    break
                assert comma_or_close == ','
            self.database.create_new_table(table_name, column_name_type_pairs)

        def create_view(input_create_view):
            pop_and_check(input_create_view, "CREATE")
            pop_and_check(input_create_view, "VIEW")
            view_name = input_create_view.pop(0)
            pop_and_check(input_create_view, "AS")
            self.database.create_view(view_name, input_create_view)

        def insert(input_insert):
            """
            Determines the table name and row values to add.
            """
            pop_and_check(input_insert, "INSERT")
            pop_and_check(input_insert, "INTO")
            table_name = input_insert.pop(0)
            column_name = None
            if input_insert[0] != "VALUES" and input_insert[0] != "DEFAULT":
                column_contents = []
                pop_and_check(input_insert, "(")
                while True:
                    item = input_insert.pop(0)
                    column_contents.append(item)
                    comma_or_close = input_insert.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ','
                column_name = []
                for column in column_contents:
                    column_name.append(Qualified(column, table_name))
            if input_insert[0] == "DEFAULT":
                if reserved_lock(table_name):
                    self.database.insert_into(table_name, "DEFAULT", "DEFAULT")
                else:
                    self.copy_database.insert_into(table_name, "DEFAULT", "DEFAULT")
                return []
            pop_and_check(input_insert, "VALUES")
            while True:
                pop_and_check(input_insert, "(")
                row_contents = []
                while True:
                    item = input_insert.pop(0)
                    row_contents.append(item)
                    comma_or_close = input_insert.pop(0)
                    if comma_or_close == ")":
                        break
                    assert comma_or_close == ','
                if reserved_lock(table_name):
                    self.database.insert_into(table_name, row_contents, column_name)
                else:
                    self.copy_database.insert_into(table_name, row_contents, column_name)
                if not input_insert:
                    break
                pop_and_check(input_insert, ",")

        def select(input_select):
            """
            Determines the table name, output_columns, and order_by_columns.
            """
            min_max_lists = []
            for input_sel in input_select:
                if input_sel == "max":
                    min_max_lists.append([input_select[input_select.index("max") + 2], "max"])
                    input_select.remove("max")
                    input_select.remove("(")
                    input_select.remove(")")
                if input_sel == "min":
                    min_max_lists.append([input_select[input_select.index("min") + 2], "min"])
                    input_select.remove("min")
                    input_select.remove("(")
                    input_select.remove(")")

            pop_and_check(input_select, "SELECT")
            is_distinct = False
            if input_select[0] == "DISTINCT":
                input_select.pop(0)
                is_distinct = True
            output_columns = []
            while True:
                col = input_select.pop(0)
                if input_select[0] == ".":
                    input_select.pop(0)
                    column_name = input_select.pop(0)
                    table_name = col
                    col = Qualified(column_name, table_name)
                else:
                    col = Qualified(col)
                output_columns.append(col)
                comma_or_from = input_select.pop(0)
                if comma_or_from == "FROM":
                    break
                assert comma_or_from == ','
            join = Join(input_select)
            table_name = join.left_table
            where_case = []
            if input_select[0] == "WHERE":
                pop_and_check(input_select, "WHERE")
                col = input_select.pop(0)
                if input_select[0] == ".":
                    input_select.pop(0)
                    column = input_select.pop(0)
                    table = col
                    col = Qualified(column, table)
                else:
                    col = Qualified(col, table_name)
                where_case.append(col)
                operate = input_select.pop(0)
                if input_select[0] == "NOT":
                    input_select.pop(0)
                    operate += " NOT"
                where_case.append(operate)
                value = input_select.pop(0)
                where_case.append(value)
            pop_and_check(input_select, "ORDER")
            pop_and_check(input_select, "BY")
            order_by_columns = []
            while True:
                col = input_select.pop(0)
                if input_select and input_select[0] == ".":
                    input_select.pop(0)
                    column_name = input_select.pop(0)
                    table_name = col
                    col = Qualified(column_name, table_name)
                else:
                    col = Qualified(col)
                if input_select and input_select[0] == "DESC":
                    input_select.pop(0)
                    col.desc_case = True
                elif input_select and input_select[0] == "COLLATE":
                    input_select.pop(0)
                    col.collation_case = input_select.pop(0)
                    if input_select and input_select[0] == "DESC":
                        input_select.pop(0)
                        col.collation_desc_case = True
                order_by_columns.append(col)
                if not input_select:
                    break
                pop_and_check(input_select, ",")
            if shared_lock(table_name):
                if is_distinct:
                    not_distinct_list = self.database.select(output_columns, table_name,
                                                             order_by_columns, where_case, join)
                    is_distinct_list = []
                    for is_distinct_column in not_distinct_list:
                        if is_distinct_column not in is_distinct_list:
                            is_distinct_list.append(is_distinct_column)
                    return is_distinct_list
                return_rows = self.database.select(output_columns, table_name, order_by_columns, where_case, join)
            else:
                if is_distinct:
                    not_distinct_list = self.copy_database.select(output_columns, table_name,
                                                                  order_by_columns, where_case, join)
                    is_distinct_list = []
                    for is_distinct_column in not_distinct_list:
                        if is_distinct_column not in is_distinct_list:
                            is_distinct_list.append(is_distinct_column)
                    return is_distinct_list
                return_rows = self.copy_database.select(output_columns, table_name, order_by_columns, where_case, join)
            if min_max_lists:
                return_rows = list(return_rows)
                return_list = []
                for min_max_list in min_max_lists:
                    value = None
                    if "max" in min_max_list:
                        for return_row in return_rows:
                            if not value:
                                value = return_row[min_max_lists.index(min_max_list)]
                            elif return_row[min_max_lists.index(min_max_list)] > value:
                                value = return_row[min_max_lists.index(min_max_list)]
                    if "min" in min_max_list:
                        for return_row in return_rows:
                            if not value:
                                value = return_row[min_max_lists.index(min_max_list)]
                            elif return_row[min_max_lists.index(min_max_list)] < value:
                                value = return_row[min_max_lists.index(min_max_list)]
                    return_list.append(value)
                return [tuple(return_list)]
            return return_rows

        def select_view(input_select_view, view_name):
            drop_str = tokenize("DROP TABLE" + " IF EXISTS " + view_name)
            drop(drop_str)
            view_lists = copy.copy(self.database.view_select[view_name])
            table_names = []
            join_list = []
            if "FROM" in view_lists:
                table_names.append(view_lists[view_lists.index("FROM") + 1])
            if "JOIN" in view_lists:
                table_names.append(view_lists[view_lists.index("JOIN") + 1])
                join_list = [view_lists[view_lists.index("ON") + 3], view_lists[view_lists.index("ON") + 7]]
            view_lists = list(select(view_lists))
            create_str = "CREATE TABLE " + view_name + " ("
            for table_name in table_names:
                table_zip = zip(self.database.tables[table_name].column_names,
                                self.database.tables[table_name].column_types)
                for zip_i in table_zip:
                    if zip_i[0].column_name in join_list and len(join_list) == 1:
                        continue
                    if zip_i[0].column_name in join_list and len(join_list) == 2:
                        join_list.remove(zip_i[0].column_name)
                    for zip_j in zip_i:
                        if isinstance(zip_j, Qualified):
                            create_str += zip_j.column_name + " "
                        else:
                            create_str += zip_j + ", "
            create_str = create_str[:-2] + ");"
            create_str = tokenize(create_str)
            create_str.pop()
            create_table(create_str)
            for view_list in view_lists:
                insert_str = "INSERT INTO " + view_name + " VALUES " + str(view_list) + ";"
                insert_str = tokenize(insert_str)
                insert_str.pop()
                insert(insert_str)
            return select(input_select_view)

        def delete(input_delete):
            """
            Determines the table name, where_case.
            """
            pop_and_check(input_delete, "DELETE")
            pop_and_check(input_delete, "FROM")
            table_name = input_delete.pop(0)
            where_case = []
            if input_delete and input_delete[0] == "WHERE":
                pop_and_check(input_delete, "WHERE")
                col = input_delete.pop(0)
                if input_delete[0] == ".":
                    input_delete.pop(0)
                    column = input_delete.pop(0)
                    table = col
                    col = Qualified(column, table)
                else:
                    col = Qualified(col, table_name)
                where_case.append(col)
                operate = input_delete.pop(0)
                if input_delete[0] == "NOT":
                    input_delete.pop(0)
                    operate += " NOT"
                where_case.append(operate)
                value = input_delete.pop(0)
                where_case.append(value)
            if reserved_lock(table_name):
                self.database.delete(table_name, where_case)
            else:
                self.copy_database.delete(table_name, where_case)

        def update(input_update):
            """
            Determines the table name, update_case, where_case.
            """
            pop_and_check(input_update, "UPDATE")
            table_name = input_update.pop(0)
            pop_and_check(input_update, "SET")
            update_case = []
            while True:
                col = input_update.pop(0)
                col = Qualified(col, table_name)
                pop_and_check(input_update, "=")
                value = input_update.pop(0)
                update_list = [col, value]
                update_case.append(update_list)
                if input_update:
                    comma_or_where = input_update[0]
                    if comma_or_where == "WHERE":
                        break
                    input_update.pop(0)
                else:
                    break
            where_case = []
            if input_update and input_update[0] == "WHERE":
                pop_and_check(input_update, "WHERE")
                col = input_update.pop(0)
                col = Qualified(col, table_name)
                where_case.append(col)
                operate = input_update.pop(0)
                if input_update[0] == "NOT":
                    input_update.pop(0)
                    operate += " NOT"
                where_case.append(operate)
                value = input_update.pop(0)
                where_case.append(value)
            if reserved_lock(table_name):
                self.database.update(table_name, update_case, where_case)
            else:
                self.copy_database.update(table_name, update_case, where_case)

        def drop(input_drop):
            """
            Determines the name and column information from tokens drop table.
            """
            pop_and_check(input_drop, "DROP")
            pop_and_check(input_drop, "TABLE")

            if input_drop[0] == "IF":
                pop_and_check(input_drop, "IF")
                pop_and_check(input_drop, "EXISTS")
                table_name = input_drop.pop(0)
                if table_name not in self.database.tables:
                    return []
            else:
                table_name = input_drop.pop(0)
                assert table_name in self.database.tables
            self.database.tables.pop(table_name)

        def begin(input_begin):
            """
            Determines the table name to begin.
            """
            pop_and_check(input_begin, "BEGIN")
            assert not self.transaction
            self.transaction = True

            mode = input_begin[0] if input_begin[0] != "TRANSACTION" else "DEFERRED"

            if mode == "IMMEDIATE":
                for modes in transaction_modes.values():
                    self.transaction = modes not in ["IMMEDIATE"]
                    assert self.transaction
            elif mode == "EXCLUSIVE":
                for modes in transaction_modes.values():
                    self.transaction = modes not in ["IMMEDIATE", "EXCLUSIVE"]
                    assert self.transaction

            for table in self.database.tables:
                self.table_mode[table] = mode
            transaction_modes[self.transaction_index] = mode

            self.copy_database.tables = copy.deepcopy(self.database.tables)

        def commit(input_commit):
            """
            Determines the table name to commit.
            """
            pop_and_check(input_commit, "COMMIT")
            assert self.transaction
            self.transaction = False

            for transaction, table in transaction_table.items():
                if transaction != self.transaction_index:
                    for mode in table.values():
                        assert mode != "COMMIT"

            self.database.tables = copy.deepcopy(self.copy_database.tables)
            for table in transaction_table[self.transaction_index].keys():
                transaction_table[self.transaction_index][table] = "DEFERRED"
            transaction_modes[self.transaction_index] = "DEFERRED"

        def rollback(input_rollback):
            """
            Determines the table name to rollback.
            """
            pop_and_check(input_rollback, "ROLLBACK")
            assert self.transaction
            self.transaction = False

            self.copy_database.tables = copy.deepcopy(self.database.tables)
            for table in transaction_table[self.transaction_index].keys():
                transaction_table[self.transaction_index][table] = "DEFERRED"
            transaction_modes[self.transaction_index] = "DEFERRED"

        def reserved_lock(input_table_name):
            """
            Determines the table name to reserved lock.
            """
            for transaction, table in transaction_table.items():
                if transaction != self.transaction_index:
                    if input_table_name in table.keys():
                        assert table[input_table_name] not in ["IMMEDIATE", "EXCLUSIVE"]
            if self.transaction and transaction_modes[self.transaction_index] not in ["EXCLUSIVE"]:
                transaction_modes[self.transaction_index] = "IMMEDIATE"
                transaction_table[self.transaction_index][input_table_name] = "IMMEDIATE"
            return not self.transaction

        def shared_lock(input_table_name):
            """
            Determines the table name to shared lock.
            """
            for transaction, table in transaction_table.items():
                if transaction != self.transaction_index:
                    if input_table_name in table.keys():
                        assert table[input_table_name] not in ["EXCLUSIVE"]
            if self.transaction and transaction_modes[self.transaction_index] not in ["IMMEDIATE", "EXCLUSIVE"]:
                transaction_modes[self.transaction_index] = "COMMIT"
                transaction_table[self.transaction_index][input_table_name] = "COMMIT"
            return not self.transaction

        tokens = tokenize(statement)
        assert tokens[0] in {"CREATE", "INSERT", "SELECT", "DELETE", "UPDATE", "DROP", "BEGIN", "COMMIT", "ROLLBACK"}
        last_semicolon = tokens.pop()
        assert last_semicolon == ";"

        if tokens[0] == "CREATE":
            if tokens[1] == "TABLE":
                create_table(tokens)
            elif tokens[1] == "VIEW":
                create_view(tokens)
            return []
        elif tokens[0] == "INSERT":
            insert(tokens)
            return []
        elif tokens[0] == "DELETE":
            delete(tokens)
            return []
        elif tokens[0] == "UPDATE":
            update(tokens)
            return []
        elif tokens[0] == "DROP":
            drop(tokens)
            return []
        elif tokens[0] == "BEGIN":
            begin(tokens)
            return []
        elif tokens[0] == "COMMIT":
            commit(tokens)
            return []
        elif tokens[0] == "ROLLBACK":
            rollback(tokens)
            return []
        elif tokens[0] == "SELECT":
            name = tokens[tokens.index("FROM") + 1]
            if name in self.database.view_select.keys():
                return select_view(tokens, name)
            return select(tokens)
        else:
            raise AssertionError("Unexpected first word in statements: " + tokens[0])

    @staticmethod
    def create_collation(name, function):
        collation_table[name] = function

    def close(self):
        """
        Empty method that will be used in future projects
        """
        pass


def connect(filename, timeout=0, isolation_level=None):
    """
    Creates a Connection object with the given filename
    """
    return Connection(filename, timeout, isolation_level)


class Database:
    def __init__(self, filename):
        self.filename = filename
        self.view_select = {}
        self.tables = {}

    def create_new_table(self, table_name, column_name_type_pairs):
        assert table_name not in self.tables
        self.tables[table_name] = Table(table_name, column_name_type_pairs)
        return []

    def create_view(self, view_name, select):
        self.view_select[view_name] = select
        return []

    def insert_into(self, table_name, row_contents, column_name):
        assert table_name in self.tables
        table = self.tables[table_name]
        table.insert_new_row(row_contents, column_name)
        return []

    def select(self, output_columns, table_name, order_by_columns, where_case, join):
        assert join.left_table in self.tables
        if join.right_table:
            left_table = self.tables[join.left_table]
            left_col = join.col_left
            right_table = self.tables[join.right_table]
            right_col = join.col_right
            column = chain(zip(left_table.column_names, left_table.column_types),
                           zip(right_table.column_names, right_table.column_types))
            table = Table("JOIN_TABLE", column)
            join_rows = []
            for left_row in left_table.rows:
                match_status = False
                left_value = left_row[left_col]
                for right_row in right_table.rows:
                    right_value = right_row[right_col]
                    if left_value is None:
                        break
                    if right_row is None:
                        continue
                    if left_value == right_value:
                        row = dict(left_row)
                        row.update(right_row)
                        join_rows.append(row)
                        match_status = True
                if match_status is False or left_value is None:
                    row = dict(left_row)
                    row.update(zip(right_table.rows[0].keys(), repeat(None)))
                    join_rows.append(row)
            table.rows = join_rows
        else:
            table = self.tables[table_name]
        return table.select_rows(output_columns, order_by_columns, where_case)

    def delete(self, table_name, where_case):
        table = self.tables[table_name]
        table.delete(where_case)

    def update(self, table_name, update_case, where_case):
        table = self.tables[table_name]
        table.update(update_case, where_case)


class Table:
    def __init__(self, name, column_name_type_pairs):
        self.name = name
        self.column_names, self.column_types = zip(*column_name_type_pairs)
        self.rows = []

    def insert_new_row(self, row_contents, column_name):
        if not column_name:
            column_name = self.column_names
        if row_contents == "DEFAULT":
            row_contents = []
            column_name = []
        assert len(column_name) == len(row_contents)
        row = dict(zip(column_name, row_contents))
        for name in set(self.column_names):
            if name not in set(column_name):
                row[name] = name.default_case
        self.rows.append(row)

    def select_rows(self, output_columns, order_by_columns, where_case):
        def expand_star_column(output_columns_expand_star_column):
            new_output_columns = []
            for col in output_columns_expand_star_column:
                if col.column_name == "*":
                    new_output_columns.extend(self.column_names)
                else:
                    new_output_columns.append(col)
            return new_output_columns

        def check_columns_exist(columns):
            assert all(col in self.column_names for col in columns)

        def sort_rows(order_by_columns_sort_rows):
            for order in reversed(order_by_columns_sort_rows):
                if order.collation_case:
                    collation_case_list = []
                    for case_row in self.rows:
                        collation_case_list.append(case_row[order])
                    collation_case_list = sorted(collation_case_list,
                                                 key=functools.cmp_to_key(collation_table[order.collation_case]),
                                                 reverse=order.collation_desc_case)
                    return_list = []
                    for item in collation_case_list:
                        for case_row in self.rows:
                            if case_row[order] == item:
                                return_list.append(case_row)
                    self.rows = return_list
                else:
                    self.rows.sort(key=lambda rows: rows[order], reverse=order.desc_case)
            return self.rows

        def sort_rows_where(where, order_by_columns_sort_rows):
            for order in reversed(order_by_columns_sort_rows):
                if order.collation_case:
                    collation_case_list = []
                    for case_row in where:
                        collation_case_list.append(case_row[order])
                    collation_case_list = sorted(collation_case_list,
                                                 key=functools.cmp_to_key(collation_table[order.collation_case]),
                                                 reverse=order.collation_desc_case)
                    return_list = []
                    for item in collation_case_list:
                        for case_row in where:
                            if case_row[order] == item:
                                return_list.append(case_row)
                    where = return_list
                else:
                    where.sort(key=lambda rows: rows[order], reverse=order.desc_case)
            return where

        def generate_tuples(rows, output_columns_generate_tuples):
            for tuple_row in rows:
                yield tuple(tuple_row[col] for col in output_columns_generate_tuples)

        expanded_output_columns = expand_star_column(output_columns)
        check_columns_exist(expanded_output_columns)
        for column in expanded_output_columns:
            if column.table_name is None:
                column.table_name = self.name
        check_columns_exist(order_by_columns)
        for column in order_by_columns:
            if column.table_name is None:
                column.table_name = self.name
        where_list = []
        if len(where_case) > 0:
            for row in self.rows:
                row_value = row[where_case[0]]
                operator = where_case[1]
                judge = False
                if operator == "=":
                    if row_value is not None:
                        judge = row_value == where_case[2]
                elif operator == "!=":
                    if row_value is not None:
                        judge = row_value != where_case[2]
                elif operator == "<":
                    if row_value is not None:
                        judge = row_value < where_case[2]
                elif operator == ">":
                    if row_value is not None:
                        judge = row_value > where_case[2]
                elif operator == "IS":
                    judge = row_value is None
                elif operator == "IS NOT":
                    judge = row_value is not None
                if judge:
                    where_list.append(row)
            sorted_rows = sort_rows_where(where_list, order_by_columns)
        else:
            sorted_rows = sort_rows(order_by_columns)
        return generate_tuples(sorted_rows, expanded_output_columns)

    def delete(self, where_case):
        where_list = []
        if len(where_case) > 0:
            for row in self.rows:
                row_value = row[where_case[0]]
                operator = where_case[1]
                judge = False
                if operator == "=":
                    if row_value is not None:
                        judge = row_value == where_case[2]
                elif operator == "!=":
                    if row_value is not None:
                        judge = row_value != where_case[2]
                elif operator == "<":
                    if row_value is not None:
                        judge = row_value < where_case[2]
                elif operator == ">":
                    if row_value is not None:
                        judge = row_value > where_case[2]
                elif operator == "IS":
                    judge = row_value is None
                elif operator == "IS NOT":
                    judge = row_value is not None
                if judge:
                    where_list.append(row)
            for row in where_list:
                self.rows.remove(row)
        else:
            self.rows = where_list

    def update(self, update_case, where_case):
        if len(where_case) > 0:
            for row in self.rows:
                row_value = row[where_case[0]]
                operator = where_case[1]
                judge = False
                if operator == "=":
                    if row_value is not None:
                        judge = row_value == where_case[2]
                elif operator == "!=":
                    if row_value is not None:
                        judge = row_value != where_case[2]
                elif operator == "<":
                    if row_value is not None:
                        judge = row_value < where_case[2]
                elif operator == ">":
                    if row_value is not None:
                        judge = row_value > where_case[2]
                elif operator == "IS":
                    judge = row_value is None
                elif operator == "IS NOT":
                    judge = row_value is not None
                if judge:
                    for case in update_case:
                        row[case[0]] = case[1]
        else:
            for row in self.rows:
                for case in update_case:
                    row[case[0]] = case[1]


class Qualified:
    """
    Qualified class: implements columns prefixed with Table
    """
    def __init__(self, column_name, table_name=None, desc_case=False, default_case=None,
                 collation_case=None, collation_desc_case=False):
        self.column_name = column_name
        self.table_name = table_name
        self.desc_case = desc_case
        self.default_case = default_case
        self.collation_case = collation_case
        self.collation_desc_case = collation_desc_case

    def __hash__(self):
        return hash((self.column_name, self.table_name))

    def __eq__(self, other):
        if self.column_name == other.column_name or self.table_name == other.table_name:
            return True
        return False


class Join:
    """
    Join class: implements the case when a SELECT contains a Join.
    """
    def __init__(self, join_or_input):
        self.left_table = join_or_input.pop(0)
        if join_or_input[0] == "LEFT":
            pop_and_check(join_or_input, "LEFT")
            pop_and_check(join_or_input, "OUTER")
            pop_and_check(join_or_input, "JOIN")
            self.right_table = join_or_input.pop(0)
            pop_and_check(join_or_input, "ON")
            self.col_left = join_or_input.pop(0)
            if join_or_input and join_or_input[0] == ".":
                join_or_input.pop(0)
                column_name = join_or_input.pop(0)
                table_name = self.col_left
                self.col_left = Qualified(column_name, table_name)
            else:
                self.col_left = Qualified(self.col_left)
            join_or_input.pop(0)
            self.col_right = join_or_input.pop(0)
            if join_or_input and join_or_input[0] == ".":
                join_or_input.pop(0)
                column_name = join_or_input.pop(0)
                table_name = self.col_right
                self.col_right = Qualified(column_name, table_name)
            else:
                self.col_right = Qualified(self.col_left)
        else:
            self.right_table = None
            self.col_left = None
            self.col_right = None


def pop_and_check(tokens, same_as):
    item = tokens.pop(0)
    assert item == same_as, "{} != {}".format(item, same_as)


def collect_characters(query, allowed_characters):
    letters = []
    for letter in query:
        if letter not in allowed_characters:
            break
        letters.append(letter)
    return "".join(letters)


def remove_leading_whitespace(query):
    whitespace = collect_characters(query, string.whitespace)
    return query[len(whitespace):]


def remove_word(query, tokens):
    word = collect_characters(query, string.ascii_letters + "_" + string.digits)
    if word == "NULL" or word == "None":
        tokens.append(None)
    else:
        tokens.append(word)
    return query[len(word):]


def remove_text(query, tokens):
    # Category discusses double and single quotes.
    quotes = ""
    if query[0] == "'":
        quotes = "'"
    if query[0] == '"':
        quotes = '"'
    # assert query[0] == "'"
    query = query[1:]
    end_quote_index = query.find(quotes)
    # A single quoted string may contain single quotes.
    while query[end_quote_index + 1] == quotes:
        query = list(query)
        query[end_quote_index] = ""
        query = "".join(query)
        end_quote_index = query.find(quotes, end_quote_index + 1)
    text = query[:end_quote_index]
    tokens.append(text)
    query = query[end_quote_index + 1:]
    return query


def remove_integer(query, tokens):
    int_str = collect_characters(query, string.digits)
    tokens.append(int_str)
    return query[len(int_str):]


def remove_number(query, tokens):
    query = remove_integer(query, tokens)
    if query[0] == ".":
        whole_str = tokens.pop()
        query = query[1:]
        query = remove_integer(query, tokens)
        frac_str = tokens.pop()
        float_str = whole_str + "." + frac_str
        tokens.append(float(float_str))
    else:
        int_str = tokens.pop()
        tokens.append(int(int_str))
    return query


def tokenize(query):
    tokens = []
    while query:
        # print("Query:{}".format(query))
        # print("Tokens: ", tokens)
        old_query = query

        if query[0] in string.whitespace:
            query = remove_leading_whitespace(query)
            continue

        if query[0] in (string.ascii_letters + "_"):
            query = remove_word(query, tokens)
            continue

        if query[0] in "(),;*.<>=?":
            tokens.append(query[0])
            query = query[1:]
            continue

        if query[:2] == "!=":
            tokens.append(query[:2])
            query = query[2:]
            continue

        if query[0] == "'" or query[0] == '"':
            query = remove_text(query, tokens)
            continue

        if query[0] in string.digits:
            query = remove_number(query, tokens)
            continue

        if len(query) == len(old_query):
            raise AssertionError("Query didn't get shorter.")

    return tokens


# The code below isn't needed, but may be helpful in testing code.
if __name__ == "__main__":
    conn = connect("test.db")
    conn.execute("CREATE TABLE students (col1 INTEGER, col2 TEXT, col3 REAL);")
    conn.execute("INSERT INTO students VALUES (3, 'hi there', 4.5);")
    conn.execute("INSERT INTO students VALUES (2, 'bye', 4.7);")
    conn.execute("INSERT INTO students VALUES (7842, 'string with spaces', 3.0);")
    conn.execute("INSERT INTO students VALUES (7, 'look a null', NULL);")
    result = conn.execute("SELECT col1, col2, col3 FROM students ORDER BY col1;")
    result_list = list(result)
    print(result_list)

    conn.execute("CREATE TABLE table_1 (col_1 INTEGER, _col2 TEXT, col_3_ REAL);")
    conn.execute("INSERT INTO table_1 VALUES (33, 'hi', 4.5);")
    conn.execute("INSERT INTO table_1 VALUES (36, 'don''t', 7);")
    conn.execute("INSERT INTO table_1 VALUES (36, 'hi ''josh''', 7);")
    result = conn.execute("SELECT * FROM table_1 ORDER BY _col2, col_1;")
    result_list = list(result)
    print(result_list)

    conn.execute("DROP TABLE table_1;")
    conn.execute("CREATE TABLE table_1 (col_1 INTEGER, _col2 TEXT, col_3_ REAL);")
    conn.execute("INSERT INTO table_1 VALUES (33, 'hi', 4.5);")
    conn.execute("INSERT INTO table_1 VALUES (36, 'don''t', 7);")
    conn.execute("INSERT INTO table_1 VALUES (36, 'hi ''josh''', 7);")
    result = conn.execute(
        "SELECT col_1, *, col_3_, table_1._col2, * FROM table_1 ORDER BY table_1._col2, _col2, col_1;")
    result_list = list(result)
    print(result_list)

    conn.execute("CREATE TABLE table (one REAL, two INTEGER, three TEXT);")
    conn.execute("INSERT INTO table VALUES (3.4, 43, 'happiness'), (5345.6, 42, 'sadness'), (43.24, 25, 'life');")
    conn.execute("INSERT INTO table VALUES (323.4, 433, 'warmth'), (5.6, 42, 'thirst'), (4.4, 235, 'Scrim');")
    result = conn.execute("SELECT * FROM table ORDER BY three, two, one;")
    result_list = list(result)
    print(result_list)

    conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
    conn_1.execute("BEGIN TRANSACTION;")
    conn_1.execute("COMMIT TRANSACTION;")
    conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
    conn_2.execute("BEGIN TRANSACTION;")
    conn_1.execute("BEGIN TRANSACTION;")
    conn_1.execute("COMMIT TRANSACTION;")
    conn_2.execute("COMMIT TRANSACTION;")

    conn_1 = connect("test1.db", timeout=0.1, isolation_level=None)
    conn_2 = connect("test1.db", timeout=0.1, isolation_level=None)
    conn_1.execute("CREATE TABLE students (name TEXT);")
    conn_1.execute("INSERT INTO students VALUES ('Josh');")
    result = conn_1.execute("SELECT * FROM students ORDER BY name;")
    result_list = list(result)
    print(result_list)
    conn_1.execute("BEGIN TRANSACTION;")
    result = conn_2.execute("SELECT * FROM students ORDER BY name;")
    result_list = list(result)
    print(result_list)
    conn_1.execute("INSERT INTO students VALUES ('Cam');")
    result = conn_1.execute("SELECT * FROM students ORDER BY name;")
    result_list = list(result)
    print(result_list)
    result = conn_2.execute("SELECT * FROM students ORDER BY name;")
    result_list = list(result)
    print(result_list)
    conn_1.execute("COMMIT TRANSACTION;")
    result = conn_1.execute("SELECT * FROM students ORDER BY name;")
    result_list = list(result)
    print(result_list)
    result = conn_2.execute("SELECT * FROM students ORDER BY name;")
    result_list = list(result)
    print(result_list)

    conn_1 = connect("test.db", timeout=0.1, isolation_level=None)
    conn_2 = connect("test.db", timeout=0.1, isolation_level=None)
    conn_3 = connect("test.db", timeout=0.1, isolation_level=None)
    conn_4 = connect("test.db", timeout=0.1, isolation_level=None)
    conn_5 = connect("test.db", timeout=0.1, isolation_level=None)
    conn_1.execute("BEGIN TRANSACTION;")
    conn_1.execute("ROLLBACK TRANSACTION;")

    conn_1.execute("BEGIN TRANSACTION;")
    conn_1.execute("COMMIT TRANSACTION;")

    conn = connect("test2.db")
    conn.execute("CREATE TABLE students (name TEXT, grade REAL, course INTEGER);")
    conn.execute("CREATE TABLE profs (name TEXT, course INTEGER);")
    conn.execute("""INSERT INTO students VALUES ('Zizhen', 4.0, 450),
    ('Cam', 3.5, 480),
    ('Cam', 3.0, 450),
    ('Jie', 0.0, 231),
    ('Jie', 2.0, 331),
    ('Dennis', 2.0, 331),
    ('Dennis', 2.0, 231),
    ('Anne', 3.0, 231),
    ('Josh', 1.0, 231),
    ('Josh', 0.0, 480),
    ('Josh', 0.0, 331);""")
    conn.execute("""INSERT INTO profs VALUES ('Josh', 480),
    ('Josh', 450),
    ('Rich', 231),
    ('Sebnem', 331);""")
    result = conn.execute("SELECT profs.name, students.grade, students.name "
                            "FROM students LEFT OUTER JOIN profs ON students.course = profs.course "
                            "WHERE students.grade > 0.0 ORDER BY students.grade, students.name DESC, profs.name DESC;")
    result_list = list(result)
    print(result_list)

    conn = connect("test4.db")
    conn.execute(
        "CREATE TABLE students (name TEXT DEFAULT '', health INTEGER DEFAULT 100, "
        "grade REAL DEFAULT 0.0, id TEXT DEFAULT 'NONE PROVIDED');")
    conn.execute("INSERT INTO students VALUES ('Zizhen', 45, 4.0, 'Hi');")
    conn.execute("INSERT INTO students DEFAULT VALUES;")
    conn.execute("INSERT INTO students (name, id) VALUES ('Cam', 'Hello');")
    conn.execute("INSERT INTO students (id, name) VALUES ('Instructor', 'Josh');")
    conn.execute("INSERT INTO students DEFAULT VALUES;")
    conn.execute("INSERT INTO students (id, name, grade) VALUES ('TA', 'Dennis', 3.0);")
    conn.execute("INSERT INTO students (id, name) VALUES ('regular', 'Emily'), ('regular', 'Alex');")
    result = conn.execute("SELECT name, id, grade, health  FROM students ORDER BY students.name;")
    result_list = list(result)
    print(result_list)

    conn = connect("test5.db")
    conn.execute("CREATE TABLE students (name TEXT, grade REAL);")
    conn.execute("CREATE VIEW stu_view AS SELECT * FROM students WHERE grade > 3.0 ORDER BY name;")
    conn.execute("""INSERT INTO students VALUES 
    ('Josh', 3.5),
    ('Dennis', 2.5),
    ('Cam', 1.5),
    ('Zizhen', 4.0)
    ;""")
    conn.execute("""INSERT INTO students VALUES 
    ('Emily', 3.7),
    ('Alex', 2.5),
    ('Jake', 3.2)
    ;""")
    result = conn.execute("SELECT grade, name FROM stu_view WHERE name < 'W' ORDER BY grade DESC;")
    result_list = list(result)
    print(result_list)

    conn = connect("test6.db")
    conn.execute("CREATE TABLE students (name TEXT, grade REAL, class INTEGER DEFAULT 231);")
    conn.executemany("INSERT INTO students VALUES (?, ?, 480);", [('Josh', 3.5), ('Tyler', 2.5), ('Grant', 3.0)])
    conn.executemany("INSERT INTO students VALUES (?, 0.0, ?);", [('Jim', 231), ('Tim', 331), ('Gary', 450)])
    conn.executemany("INSERT INTO students (grade, name) VALUES (?, ?);", [(4.1, 'Tess'), (1.1, 'Jane')])
    result = conn.execute("SELECT name, class, grade FROM students ORDER BY grade, name;")
    result_list = list(result)
    print(result_list)

    conn = connect("test7.db")
    conn.execute("CREATE TABLE students (name TEXT, grade REAL, class INTEGER);")
    conn.executemany("INSERT INTO students VALUES (?, ?, ?);",
                     [('Josh', 3.5, 480),
                      ('Tyler', 2.5, 480),
                      ('Alice', 2.2, 231),
                      ('Tosh', 4.5, 450),
                      ('Losh', 3.2, 450),
                      ('Grant', 3.3, 480),
                      ('Emily', 2.25, 450),
                      ('James', 2.25, 450)])
    result = conn.execute("SELECT * FROM students ORDER BY class, name;")
    result_list = list(result)
    print(result_list)

    def collate_ignore_first_two_letter(string1, string2):
        string1 = string1[2:]
        string2 = string2[2:]
        if string1 == string2:
            return 0
        if string1 < string2:
            return -1
        else:
            return 1

    conn.create_collation("skip2", collate_ignore_first_two_letter)
    result = conn.execute("SELECT * FROM students ORDER BY name COLLATE skip2 DESC, grade;")
    result_list = list(result)
    print(result_list)

    conn = connect("test8.db")
    conn.execute("CREATE TABLE students (name TEXT, grade REAL, class INTEGER);")
    conn.executemany("INSERT INTO students VALUES (?, ?, ?);",
                     [('Josh', 3.5, 480),
                      ('Tyler', 2.5, 480),
                      ('Tosh', 4.5, 450),
                      ('Losh', 3.2, 450),
                      ('Grant', 3.3, 480),
                      ('Emily', 2.25, 450),
                      ('James', 2.25, 450)])
    result = conn.execute("SELECT max(grade) FROM students WHERE class = 480 ORDER BY grade;")
    result_list = list(result)
    print(result_list)
    result = conn.execute("SELECT min(grade), min(name) FROM students WHERE name > 'T' ORDER BY grade, name;")
    result_list = list(result)
    print(result_list)
