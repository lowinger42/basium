#! /usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2013, Anders Lowinger, Abundo AB
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
Basium database driver that handles PostgreSQL

All database operations are tried twice if any error occurs, clearing the
connection if an error occurs. This makes all operations to reconnect if the
connection to the database has been lost.

To use this driver, install the psycopg2 connector
    In ubuntu
        sudo apt-get install python3-psycopg2

"""

import sys
import datetime
import decimal

import basium_common as bc
import basium_driver

err = None
try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    err = "Can't find the psycopg2 python module"
if err:
    raise bc.Error(1, err)

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)


#
# These are shadow classes from the basium_model
# handles the database specific functions such
# as convering to/from SQL types
#
class Column:

    def toPython(self, value):
        return value

    def toSql(self, value):
        return value

    # Convert a 'describe table' to sqltype
    #
    #    from "describe <table"
    #    {   'Default': 'CURRENT_TIMESTAMP',
    #        'Extra': 'on update CURRENT_TIMESTAMP',
    #        'Field': 'start',
    #        'Key': '',
    #        'Null': 'NO',
    #        'Type': 'timestamp'},
    #
    def tableTypeToSql(self, tabletype):
        if tabletype['Key'] == 'PRI':
            tmp = 'serial'
        else:
            tmp = tabletype['Type']
            if tabletype['Null'] == 'NO':
                tmp += " not null"
            else:
                tmp += " null"
        return tmp


class BooleanCol(basium_driver.Column):
    """
    Stores boolean as number: 0 or 1
    """

    def typeToSql(self):
        sql = "boolean"
        if self.nullable:
            sql += " null"
        else:
            sql += " not null"
        if self.default is not None:
            if self.default:
                sql += " default TRUE"
            else:
                sql += " default FALSE"
        return sql

    def toPython(self, value):
        return value == 1

    def toSql(self, value):
        if value is None:
            return "NULL"
        if value:
            return 'TRUE'
        return 'FALSE'


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
    """

    def typeToSql(self):
        sql = 'timestamp without time zone'
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
            return "SERIAL PRIMARY KEY"
        sql = 'integer'    # todo how to handle length? '(%s)' % self.length
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


class BasiumDriver(basium_driver.BaseDriver):
    def __init__(self, log=None, dbconf=None):
        self.log = log
        self.dbconf = dbconf
        self.dbconnection = None
        self.connectionStatus = None
        self.tables = None

    def connect(self):
        try:
            if not self.dbconf.port:
                self.dbconf.port = 5432
            self.dbconnection = psycopg2.connect(
                host=self.dbconf.host, port=self.dbconf.port, user=self.dbconf.username, password=self.dbconf.password, dbname=self.dbconf.database)
            self.cursor = self.dbconnection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except psycopg2.DatabaseError as e:
            raise bc.Error(1, str(e))

    def disconnect(self):
        self.dbconnection = None
        self.tables = None

    def execute(self, sql, values=None, commit=False):
        """
        Execute a query
        If error try to reconnect and redo the query to handle timeouts
        """
        for i in range(0, 2):
            if self.dbconnection is None:
                self.connect()
            try:
                if self.debug & bc.DEBUG_SQL:
                    self.log.debug(self.cursor.mogrify(sql, values))
                if values is not None:
                    self.cursor.execute(sql, values)
                else:
                    self.cursor.execute(sql)
                if commit:
                    self.dbconnection.commit()
                return

            except psycopg2.DatabaseError as e:
                if i == 1:
                    raise bc.Error(1, str(e))
                self.disconnect()
#                    try:
#                        self.dbconnection.rollback()    # make sure to clear any previous hanging transactions
#                    except psycopg2.DatabaseError, e:
#                        pass

    def isDatabase(self, dbName):
        """
        Returns True if the database exist
        """
        sql = "select * from pg_database where datname=%s"  # % dbName
        values = (dbName,)
        self.execute(sql, values)
        try:
            row = self.cursor.fetchone()
            if row and len(row) > 0:
                exist = row[0] == dbName
        except psycopg2.DatabaseError as e:
            raise bc.Error(1, str(e))
        return exist

    def isTable(self, tableName):
        """
        Returns True if the table exist
        """
        if not self.tables:
            self.tables = {}
            sql = "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
            values = (tableName,)
            self.execute(sql, values)
            try:
                for row in self.cursor.fetchall():
                    self.tables[row[0]] = 1
            except psycopg2.DatabaseError as e:
                raise bc.Error(1, str(e))
        return tableName in self.tables

    def createTable(self, obj):
        """
        Create a table
        """
        sql = 'CREATE TABLE %s (' % obj._table
        columnlist = []
        for colname, column in obj._iterNameColumn():
            columnlist.append('"%s" %s' % (colname, column.typeToSql()))
        sql += ",".join(columnlist)
        sql += ')'
        self.execute(sql, commit=True)

    def verifyTable(self, obj):
        """
        Verify that a table has the correct definition
        Returns None if table does not exist
        Returns list of Action, zero length if nothing needs to be done
        """
        actions = []
#        sql = 'DESCRIBE %s' % obj._table
#        for i in range(0,2):
#            if not self.connect():
#                return self.connectionStatus
#            try:
#                self.cursor.execute(sql)
#                rows = self.cursor.fetchall()
#                break
#            except psycopg2.DatabaseError, e:
#                response.setError( 1, str(e) )
#        if response.isError():
#            return response
#        tabletypes = {}
#        for row in rows:
#            tabletypes[row['Field']] = row
#        actions = []
#        for colname, column in obj._iterNameColumn():
#            if colname in tabletypes:
#                tabletype = tabletypes[colname]
#                if column.typeToSql() != column.tableTypeToSql(tabletype):
#                    msg = "Error: Column '%s' has incorrect type in SQL Table. Action: Change column type in SQL Table" % (colname)
#                    self.log.debug(msg)
#                    self.log.debug("  type in Object   : '%s'" % (column.typeToSql()) )
#                    self.log.debug("  type in SQL table: '%s'" % (column.tableTypeToSql(tabletype)))
#                    actions.append(Action(
#                            msg=msg,
#                            unattended=True,
#                            sqlcmd='ALTER TABLE %s CHANGE %s %s %s' % (obj._table, colname, colname, column.typeToSql())
#                            ))
#            else:
#                msg = "Error: Column '%s' does not exist in the SQL Table. Action: Add column to SQL Table" % (colname)
#                print(" " + msg)
#                actions.append(Action(
#                        msg=msg,
#                        unattended=True,
#                        sqlcmd='ALTER TABLE %s ADD %s %s' % (obj._table, colname, column.typeToSql())
#                        ))
#
#        for colname, tabletype in tabletypes.items():
#            if not colname in obj._columns:
#                actions.append(Action(
#                        msg="Error: Column '%s' in SQL Table NOT used, should be removed" % colname,
#                        unattended=False,
#                        sqlcmd='ALTER TABLE %s DROP %s' % (obj._table, colname)
#                        ))
#        if len(actions) < 1:
#            self.log.debug("SQL Table '%s' matches the object" % obj._table)
#        else:
#            self.log.debug("SQL Table '%s' DOES NOT match the object, need changes" % obj._table)
        return actions

    def modifyTable(self, obj, actions):
        """
        Update table to latest definition of class
        actions is the result from verifyTable
        Returns True if everything is ok
        """
#        response = Response()
#        self.log.debug("Updating table %s" % obj._table)
#        if len(actions) == 0:
#            self.log.debug("  Nothing to do")
#            return True
#
#        print("Actions that needs to be done:")
#        askForConfirmation = False
#        for action in actions:
#            print("  " + action.msg)
#            print("   SQL: " + action.sqlcmd)
#            if not action.unattended:
#                askForConfirmation = True
#
#        if askForConfirmation:
#            print("WARNING: removal of columns can lead to data loss.")
#            a = raw_input('Are you sure (yes/No)? ')
#            if a != 'yes':
#                print("Aborted!")
#                return False
#
#        # we first remove columns, so we dont get into conflicts
#        # with the new columns, for example changing primary key (there can only be one primary key)
#        for action in actions:
#            if 'DROP' in action.sqlcmd:
#                print("Fixing " + action.msg)
#                print("  Cmd: " + action.sqlcmd)
#                self.cursor.execute(action.sqlcmd)
#        for action in actions:
#            if not 'DROP' in action.sqlcmd:
#                print("Fixing " + action.msg)
#                print("  Cmd: " + action.sqlcmd)
#                self.cursor.execute(action.sqlcmd)
#        self.dbconnection.commit()
        return True

    def count(self, query):
        sql = "select count(*) from %s" % (query.table())
        sql2, values = query.toSql()
        sql += sql2

        self.execute(sql, values)
        try:
            row = self.cursor.fetchone()
            if row is None:
                raise bc.Error(1, 'Cannot query for count(*) in %s' % (query.table()))
            data = int(row[0])
        except psycopg2.DatabaseError as e:
            raise bc.Error(1, str(e))
        return data

    def select(self, query):
        """
        Fetch one or multiple rows from a database
        Returns an object that can be iterated over, returning rows
        If there is any errors, an exception is raised
        """
        sql = "SELECT * FROM %s" % query.table()
        sql2, values = query.toSql()
        sql += sql2
        self.execute(sql, values)
        return self.cursor

    def insert(self, table, values):
        """
        Insert a row in the table
        value is a dictionary with columns, excluding primary key
        """
        parms = []
        holder = []
        vals = []
        for key, val in values.items():
            if key != '_id':
                parms.append('"' + key + '"')
                holder.append("%s")
                vals.append(val)
        sql = "INSERT INTO %s ( %s ) VALUES ( %s ) RETURNING _id" % (table, ",".join(parms), ",".join(holder))
        self.execute(sql, vals, commit=True)
        try:
            data = self.cursor.fetchone()[0]
        except psycopg2.DatabaseError as e:
            raise bc.Error(1, str(e))
        return data

    def update(self, table, values):
        """
        Update a row in the table
        """
        parms = []
        vals = []
        
        for key, val in values.items():
            if key != '_id':
                parms.append('"%s"=%%s' % key)
                vals.append(val)
            else:
                primary_key_val = val
        sql = "UPDATE %s SET %s WHERE %s=%%s" % (table, ",".join(parms), '_id')
        vals.append(primary_key_val)
        self.execute(sql, vals, commit=True)

    def delete(self, query):
        """
        delete a row from a table
        "DELETE FROM EMPLOYEE WHERE AGE > '%s'", (20, )
        returns number of rows deleted
        """
        sql = "DELETE FROM %s" % query.table()
        sql2, values = query.toSql()
        if sql2 == '':
            raise bc.Error(1, 'Missing query on delete(), empty query is not accepted')
        sql += sql2
        self.execute(sql, values, commit=True)
        return self.cursor.rowcount
