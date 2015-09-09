#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2012-2013, Anders Lowinger, Abundo AB
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of the <organization> nor the
#      names of its contributors may be used to endorse or promote products
#      derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""
Basium database driver that handles MySQL

All database operations are tried twice if any error occurs, clearing the
connection if an error occurs. This makes all operations to reconnect if the
connection to the database has been lost.

To use this driver, install the mysql.connector
    In ubuntu
        sudo apt-get install python3-mysql.connector

"""

import datetime
import decimal

import basium_common as bc
import basium_driver

err = None
try:
    import mysql.connector
except ImportError:
    err = "Can't find the mysql.connector python module"
if err:
    raise bc.Error(1, err)


class BooleanCol(basium_driver.Column):
    """
    Stores boolean as number: 0 or 1
    """

    def typeToSql(self):
        sql = "tinyint(1)"
        if self.nullable:
            sql += " null"
        else:
            sql += " not null"
        if self.default is not None:
            if self.default:
                sql += " default 1"
            else:
                sql += " default 0"
        return sql

    def toPython(self, value):
        return value == 1

    def toSql(self, value):
        if value is None:
            return "NULL"
        if value:
            return 1
        return 0


class DateCol(basium_driver.Column):
    """
    Stores a date
    """

    def typeToSql(self):
        sql = "date"
        if self.nullable:
            sql += " null"
        else:
            sql += " not null"
        if self.default is not None:
            sql += " default %s" % self.default
        return sql

    def toPython(self, value):
        if isinstance(value, datetime.datetime):
            value = value.date()
        if isinstance(value, str):
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S').date()
        return value

    def toSql(self, value):
        if value is None:
            return "NULL"
        return value


class DateTimeCol(basium_driver.Column):
    """
    Stores date+time
    ignores microseconds
    if default is 'NOW' the current date+time is stored
    """

    def typeToSql(self):
        sql = 'datetime'
        if self.nullable:
            sql += " null"
        else:
            sql += " not null"
        if self.default is not None and self.default != 'NOW':
            sql += " default %s" % self.default
        return sql

    def toPython(self, value):
        if isinstance(value, str):
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        return value


class DecimalCol(basium_driver.Column):
    """
    stores a fixed precision number
    we cheat and represent this as a float in python
    """

    def typeToSql(self):
        sql = 'decimal(%d,%d)' % (self.maxdigits, self.decimal)
        if self.nullable:
            sql += " null"
        else:
            sql += " not null"
        if self.default is not None:
            sql += " default '%s'" % str(self.default)
        return sql

    def toPython(self, value):
        if value is None:
            return None
        if isinstance(value, decimal.Decimal):
            return value
        return decimal.Decimal(value)

    def toSql(self, value):
        if value is None:
            return "NULL"
        return value


class FloatCol(basium_driver.Column):
    """
    Stores a floating point number
    """

    def typeToSql(self):
        sql = "float"
        if self.nullable:
            sql += " null"
        else:
            sql += " not null"
        if self.default is not None:
            sql += " default %s" % str(self.default)
        return sql

    def toPython(self, value):
        if isinstance(value, str):
            value = float(value)
        return value

    def toSql(self, value):
        if value is None:
            return "NULL"
        return str(value)


class IntegerCol(basium_driver.Column):
    """
    Stores an integer
    """

    def typeToSql(self):
        if self.primary_key:
            return "serial"
        sql = 'int(%s)' % self.length
        if self.nullable:
            sql += " null"
        else:
            sql += " not null"
        if self.default is not None:
            if self.default:
                sql += " default %i" % self.default
        return sql

    def toPython(self, value):
        if isinstance(value, str):
            value = int(value)
        return value

    def toSql(self, value):
        if value is None:
            return "NULL"
        return value


class VarcharCol(basium_driver.Column):
    """
    Stores a string
    """

    def typeToSql(self):
        sql = 'varchar(%d)' % self.length
        if self.nullable:
            sql += " null"
        else:
            sql += " not null"
        if self.default is not None:
            if self.default != '':
                sql += " default '%s'" % self.default
        return sql


class Action:
    def __init__(self, msg=None, unattended=None, sqlcmd=None):
        self.msg = msg
        self.unattended = unattended
        self.sqlcmd = sqlcmd


class MySQLCursorDict(mysql.connector.cursor.MySQLCursor):
    """A cursor class that returns rows as dictionary"""
    def _row_to_python(self, rowdata, desc=None):
        row = super(MySQLCursorDict, self)._row_to_python(rowdata, desc)
        if row:
            return dict(zip(self.column_names, row))
        return None


class BasiumDriver(basium_driver.BaseDriver):
    def __init__(self, log=None, dbconf=None):
        self.log = log
        self.dbconf = dbconf
        self.dbconf.database = self.dbconf.database

        self.dbconnection = None
        self.connectionStatus = None
        self.tables = None

    def connect(self):
        try:
            if not self.dbconf.port:
                self.dbconf.port = 3306
            self.dbconnection = mysql.connector.connect(
                                    host=self.dbconf.host,
                                    port=int(self.dbconf.port),
                                    user=self.dbconf.username,
                                    passwd=self.dbconf.password,
                                    db=self.dbconf.database)
            self.cursor = self.dbconnection.cursor(cursor_class=MySQLCursorDict)
            sql = "set autocommit=1;"
            if self.debug & bc.DEBUG_SQL:
                self.log.debug('SQL=%s' % sql)
            self.cursor.execute(sql)
            if self.dbconnection:
                self.dbconnection.commit()
        except mysql.connector.Error as err:
            raise bc.Error(err.errno, str(err))

    def disconnect(self):
        self.dbconnection = None
        self.tables = None

    def execute(self, sql, values=None, commit=False):
        """
        Execute a query,
        if error try to reconnect and redo the query to handle timeouts
        """
        for i in range(0, 2):
            if self.dbconnection is None:
                self.connect()
                if self.debug & bc.DEBUG_SQL:
                    self.log.debug('SQL=%s, values=%s' % (sql, values))
            try:
                if values is not None:
                    self.cursor.execute(sql, values)
                else:
                    self.cursor.execute(sql)
                if commit:
                    self.dbconnection.commit()
                return
            except mysql.connector.Error as err:
                if self.dbconnection is not None:
                    try:
                        self.dbconnection.commit()
                    except mysql.connector.Error as err:
                        pass
                if i == 1:
                    raise bc.Error(err.errno, str(err))
                self.disconnect()

    def isDatabase(self, dbName):
        """
        Returns True if the database exist
        """
        sql = "SELECT IF(EXISTS (SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s'), 'Yes','No')" % dbName
        exist = False
        self.execute(sql)
        try:
            row = self.cursor.fetchone()
            key = list(row.keys())[0]
            exist = row[key] == 'Yes'
        except mysql.connector.Error as err:
            raise bc.Error(err.errno, str(err))
        return exist

    def isTable(self, tableName):
        """
        Returns True if the table exist
        """
        if not self.tables:
            # Read all tables and cache locally
            self.tables = {}
            sql = "show tables like %s"
            self.execute(sql, (tableName,))
            try:
                for row in self.cursor.fetchall():
                    value= list(row.values())[0]
                    self.tables[value] = 1
            except mysql.connector.Error as err:
                raise bc.Error(err.errno, str(err))
        return tableName in self.tables

    def createTable(self, obj):
        """
        Create a tablet
        """
        sql = 'CREATE TABLE %s (\n   ' % obj._table
        columnlist = []
        for colname, column in obj._iterNameColumn():
            columnlist.append('%s %s' % (colname, column.typeToSql()))
        sql += "\n  ,".join(columnlist)
        sql += '\n)'
        self.execute(sql, commit=True)

    def verifyTable(self, obj):
        """
        Verify that a table has the correct definition
        Returns None if table does not exist
        Returns list of Action, zero length if nothing needs to be done
        """
        sql = 'DESCRIBE %s' % obj._table
        self.execute(sql)
        tabletypes = {}
        rows = self.cursor.fetchall()
        for row in rows:
            tabletypes[row["Field"]] = row
        actions = []
        for colname, column in obj._iterNameColumn():
            if colname in tabletypes:
                tabletype = tabletypes[colname]
                if column.typeToSql() != column.tableTypeToSql(tabletype):
                    msg = "Error: Column '%s' has incorrect type in SQL Table. Action: Change column type in SQL Table" % (colname)
                    if self.debug & bc.DEBUG_TABLE_MGMT:
                        self.log.debug(msg)
                        self.log.debug("  type in Object   : '%s'" % (column.typeToSql()) )
                        self.log.debug("  type in SQL table: '%s'" % (column.tableTypeToSql(tabletype)))
                    actions.append(Action(
                            msg=msg,
                            unattended=True,
                            sqlcmd='ALTER TABLE %s CHANGE %s %s %s' % (obj._table, colname, colname, column.typeToSql())
                            ))
            else:
                msg = "Error: Column '%s' does not exist in the SQL Table. Action: Add column to SQL Table" % (colname)
                if self.debug & bc.DEBUG_TABLE_MGMT:
                    self.log.debug(" " + msg)
                actions.append(Action(
                        msg=msg,
                        unattended=True,
                        sqlcmd='ALTER TABLE %s ADD %s %s' % (obj._table, colname, column.typeToSql())
                        ))

        for colname, tabletype in tabletypes.items():
            if colname not in obj._columns:
                actions.append(Action(
                        msg="Error: Column '%s' in SQL Table NOT used, should be removed" % colname,
                        unattended=False,
                        sqlcmd='ALTER TABLE %s DROP %s' % (obj._table, colname)
                        ))
        if len(actions) < 1:
            self.log.debug("SQL Table '%s' matches the object" % obj._table)
        else:
            self.log.debug("SQL Table '%s' DOES NOT match the object, need changes" % obj._table)
        return actions

    def modifyTable(self, obj, actions):
        """
        Update table to latest definition of class
        actions is the result from verifyTable()
        """
        if self.debug & bc.DEBUG_TABLE_MGMT:
            self.log.debug("Updating table %s" % obj._table)
        if len(actions) == 0:
            if self.debug & bc.DEBUG_TABLE_MGMT:
                self.log.debug("  Nothing to do")
            return False

        if self.debug & bc.DEBUG_TABLE_MGMT:
            self.log.debug("Actions that needs to be done:")
        askForConfirmation = False
        for action in actions:
            if self.debug & bc.DEBUG_TABLE_MGMT:
                self.log.debug("  " + action.msg)
                self.log.debug("   SQL: " + action.sqlcmd)
            if not action.unattended:
                askForConfirmation = True

        if askForConfirmation:
            if self.debug & bc.DEBUG_TABLE_MGMT:
                self.log.debug("WARNING: removal of columns can lead to data loss.")
            a = input('Are you sure (yes/No)? ')
            if a != 'yes':
                if self.debug & bc.DEBUG_TABLE_MGMT:
                    self.log.debug("Aborted!")
                    return True

        # we first remove columns, so we dont get into conflicts
        # with the new columns, for example changing primary key
        # (there can only be one primary key)
        for action in actions:
            if 'DROP' in action.sqlcmd:
                if self.debug & bc.DEBUG_TABLE_MGMT:
                    self.log.debug("Fixing " + action.msg)
                    self.log.debug("  Cmd: " + action.sqlcmd)
                self.execute(action.sqlcmd, commit=True)
        for action in actions:
            if 'DROP' not in action.sqlcmd:
                if self.debug & bc.DEBUG_TABLE_MGMT:
                    self.log.debug("Fixing " + action.msg)
                    self.log.debug("  Cmd: " + action.sqlcmd)
                self.execute(action.sqlcmd, commit=True)
        self.dbconnection.commit()
        return False

    def count(self, query):
        sql = "select count(*) from %s" % (query.table())
        sql2, values = query.toSql()
        sql += sql2
        self.execute(sql, values)
        try:
            row = self.cursor.fetchone()
            if row is None:
                raise bc.Error(1, 'Cannot query for count(*) in %s' % (query.table()))
            rows = int(row['count(*)'])
        except mysql.connector.Error as err:
            raise bc.Error(err.errno, str(err))
        return rows

    def select(self, query):
        """
        Fetch one or multiple rows from a database
        Returns an object that can be iterated over, returning rows
        If there is any errors, an DriverError exception is raised
        """
        sql = "SELECT * FROM %s" % query.table()
        sql2, values = query.toSql()
        sql += sql2
        self.execute(sql, values)
        return self.cursor

    def insert(self, table, values):
        """
        Insert a row in the table
        value is a dictionary with columns, primary key '_id' is ignored
        """
        parms = []
        holder = []
        vals = []
        for key, val in values.items():
            if key != '_id':
                parms.append(key)
                holder.append("%s")
                vals.append(val)
        sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (table, ",".join(parms), ",".join(holder))
        self.execute(sql, vals, commit=True)
        return self.cursor.lastrowid

    def update(self, table, values):
        """
        Update a row in the table
        """
        parms = []
        vals = []
        for key, val in values.items():
            if key != '_id':
                parms.append("%s=%%s" % key)
                vals.append(val)
            else:
                primary_key_val = val
        sql = "UPDATE %s SET %s WHERE %s=%%s" % (table, ",".join(parms), '_id')
        vals.append(primary_key_val)
        self.execute(sql, vals, commit=True)

    def delete(self, query):
        """
        delete a row from a table
        "DELETE FROM EMPLOYEE WHERE AGE > '%d'" % (20)
        refuses to delete all rows in a table (empty query)
        returns number of rows deleted
        """
        sql = "DELETE FROM %s" % query.table()
        sql2, values = query.toSql()
        if sql2 == '':
            raise bc.Error(1, 'delete() with empty query not accepted')
        sql += sql2
        self.execute(sql, values, commit=True)
        return self.cursor.rowcount
