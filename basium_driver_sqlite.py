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
Basium database driver that handles SQLite

All database operations are tried twice if any error occurs, clearing the
connection if an error occurs. This makes all operations to reconnect if the
connection to the database has been lost. 

SQLite only handles direct files, but the file can be located on a remote 
media that needs to be reconnected
"""

import datetime
import decimal

import basium_common as bc
import basium_driver

err = None
try:
    import sqlite3
except ImportError:
    err = "Can't find the sqlite3 python module"
if err:
    raise bc.Error(1, err)

class ColumnInfo:
    
    def __init__(self, arg):
        self.cid = arg["cid"]
        self.name = arg["name"]
        self.typ = arg["type"]
        self.notnull = arg["notnull"]
        self.default = arg["dflt_value"]
        self.pk = arg["pk"]
        

class BooleanCol(basium_driver.Column):
    """"Stores boolean as number: 0 or 1"""

    def typeToSql(self):
        sql = "tinyint(1)"
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            if self.default:
                sql += " default 1"
            else:
                sql += " default 0"
        return sql

    def toPython(self, value):
        return value == 1

    def toSql(self, value):
        if value == None:
            return "NULL"
        if value:
            return 1
        return 0
    

class DateCol(basium_driver.Column):
    """Stores a date"""

    def typeToSql(self):
        sql = "date"
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            sql += " default %s" % self.default
        return sql

    def toPython(self, value):
        if isinstance(value, datetime.datetime):
            value = value.date()
        elif isinstance(value, str):
            value = datetime.datetime.strptime(value, '%Y-%m-%d').date()
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return value


class DateTimeCol(basium_driver.Column):
    """
    Stores date+time
    ignores microseconds
    """
    
    def typeToSql(self):
        sql = 'datetime'
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None and self.default != 'NOW':
            sql += " default %s" % self.default
        return sql

    def toPython(self, value):
        if isinstance(value, str):
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        return value


class DecimalCol(basium_driver.Column):
    """
    Stores a fixed precision number
    we cheat and represent this as a float in python
    sqlite does not handle decimal, so we use varchar instead
    """

    def typeToSql(self):
        sql = 'varchar'
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            sql += " default '%s'" % str(self.default)
        return sql

    def toPython(self, value):
        if value == None:
            return None
        if isinstance(value, decimal.Decimal):
            return value
        return decimal.Decimal(value)
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return float(value)


class FloatCol(basium_driver.Column):
    """Stores a floating point number"""

    def typeToSql(self):
        sql = "float"
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            sql += " default %s" % str(self.default)
        return sql

    def toPython(self, value):
        if isinstance(value, str):
            value = float(value)
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return str(value)

    
class IntegerCol(basium_driver.Column):
    """Stores an integer"""

    def typeToSql(self):
        if self.primary_key:
            return "INTEGER PRIMARY KEY";
        sql = 'INTEGER'  # no length in sqlite % self.length
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            if self.default: 
                sql += " default %i" % self.default
        return sql

    def toPython(self, value):
        if isinstance(value, str):
            value = int(value)
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return value


class VarcharCol(basium_driver.Column):
    """
    Stores a string
    sqlite ignores the length so it is not used
    """
    def typeToSql(self):
        sql = 'varchar'
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            if self.default != '':
                sql += " default '%s'" % self.default
        return sql


class Action:
    
    def __init__(self, msg=None, unattended=None, sqlcmd=None):
        self.msg = msg
        self.unattended = unattended
        self.sqlcmd = sqlcmd

class Driver:
    def __init__(self, log=None, dbconf=None):
        self.log = log
        self.dbconf = dbconf
        
        self.dbconnection = None
        self.tables = None
        self.connectionStatus = None

    def connect(self):
        try:
            self.dbconnection = sqlite3.connect(self.dbconf.database,  check_same_thread=False)
            self.dbconnection.row_factory = sqlite3.Row   # return querys as dictionaries
            self.cursor = self.dbconnection.cursor()
        except sqlite3.Error as e:
            raise bc.Error(1, e.args[0])
    
    def disconnect(self):
        self.dbconnection = None
        self.tables = None

    def execute(self, sql, values=None, commit=True):
        """
        Execute a query, if error try to reconnect and redo the query
        to handle timeouts
        """
        for i in range(0, 2):
            if self.dbconnection == None:
                self.connect()
            try:
                if self.debug & bc.DEBUG_SQL:
                    self.log.debug('SQL=%s' % sql)
                    if values:
                        self.log.debug('   =%s' % values)
                if values != None:
                    self.cursor.execute(sql, values)
                else:
                    self.cursor.execute(sql)
                if commit:
                    self.dbconnection.commit()
                return

            except sqlite3.Error as e:
                if i == 1:
                    raise bc.Error( 1, e.args[0] )
    
    def isDatabase(self, dbName):
        """
        Returns True if the database exist
        We always return true, sqlite automatically creates the database when opened
        """
        return True

    def isTable(self, tableName):
        """Returns True if the table exist"""
        if not self.tables:
            self.tables = {}
            sql = "SELECT name FROM sqlite_master WHERE type='table'"
            self.execute(sql)
            try:
                for row in self.cursor.fetchall():
                    self.tables[row[0]] = 1
            except sqlite3.Error as e:
                raise bc.Error( 1, e.args[0] )
        return tableName in self.tables

    def createTable(self, obj):
        """Create a table"""
        sql = 'CREATE TABLE %s (' % obj._table
        columnlist = []
        for colname, column in obj._iterNameColumn():
            columnlist.append('%s %s' % (colname, column.typeToSql()))
        sql += "  ,".join(columnlist)
        sql += ')'
        self.execute(sql)
        return True

    def tableTypeToSql(self, tabletype):
        """
        Map from sql query to table types
         0 cid
         1 name
         2 type
         3 not null
         4 default value
         5 pk - primary key
        """
        if  tabletype[5] > 0:
            tmp = 'INTEGER PRIMARY KEY'
        else:
            tmp = tabletype[2]
            if tabletype[3] != 0:
                tmp += " not null"
            else:
                tmp += " null"
        return tmp

    def verifyTable(self, obj):
        """
        Verify that a table has the correct definition
        Returns None if table does not exist
        Returns list of Action, zero length if nothing needs to be done
        """
        
        sql = "PRAGMA table_info([%s])" % obj._table
        self.execute(sql)

        # cid, name, type, notnull, dflt_value, pk
        tabletypes = {}
        rows = self.cursor.fetchall()
        for row in rows:
            tabletypes[row[1]] = row
        actions = []
        for colname, column in obj._iterNameColumn():
            if colname in tabletypes:
                tabletype = tabletypes[colname]
                columntype_str = column.typeToSql()
                tabletype_str = self.tableTypeToSql(tabletype)
                if columntype_str != tabletype_str:
                    msg = "Error: Column '%s' has incorrect type in SQL Table. Action: Change column type in SQL Table" % (colname)
                    if self.debug & bc.DEBUG_TABLE_MGMT:
                        self.log.debug(msg)
                        self.log.debug("  type in Object   : '%s'" % (columntype_str) )
                        self.log.debug("  type in SQL table: '%s'" % (tabletype_str))
                    actions.append(Action(
                            msg=msg,
                            unattended=True,
                            sqlcmd='ALTER TABLE %s CHANGE %s %s %s' % (obj._table, colname, colname, columntype_str)
                            ))
            else:
                msg = "Error: Column '%s' does not exist in the SQL Table. Action: Add column to SQL Table" % (colname)
                if self.debug & bc.DEBUG_TABLE_MGMT:
                    self.log.debug(" %s" % msg)
                actions.append(Action(
                        msg=msg,
                        unattended=True,
                        sqlcmd='ALTER TABLE %s ADD COLUMN %s %s' % (obj._table, colname, column.typeToSql())
                        ))

        for colname, tabletype in tabletypes.items():
            if not colname in obj._columns:
                actions.append(Action(
                        msg="Error: Column '%s' in SQL Table NOT used, should be removed" % colname,
                        unattended=False,
                        sqlcmd='ALTER TABLE %s DROP %s' % (obj._table, colname)
                        ))
        return actions

    def modifyTable(self, obj, actions):
        """
        Update table to latest definition of class
        actions is the result from verifytable
        todo: sqlite only support a subset of functionality in "ALTER TABLE...", so we work around this
        by copying the table to a new one
        """
        if len(actions) == 0:
            if self.debug & bc.DEBUG_TABLE_MGMT:
                self.log.debug("  Nothing to do")
            return

        self.log.debug("Actions that needs to be done:")
        askForConfirmation = False
        for action in actions:
            if self.debug & bc.DEBUG_TABLE_MGMT:
                self.log.debug("  %s" % action.msg)
                self.log.debug("   SQL: %s" % action.sqlcmd)
            if not action.unattended:
                askForConfirmation = True

        if askForConfirmation:
            self.log.debug("WARNING: removal of columns can lead to data loss.")
            a = input('Are you sure (yes/No)? ')
            if a != 'yes':
                raise bc.Error(1, "Aborted!")

        # we first remove columns, so we dont get into conflicts
        # with the new columns, for example changing primary key (there can only be one primary key)
        for action in actions:
            if 'DROP' in action.sqlcmd:
                if self.debug & bc.DEBUG_TABLE_MGMT:
                    self.log.debug("Fixing %s" % action.msg)
                    self.log.debug("  Cmd: %s" % action.sqlcmd)
                self.cursor.execute(action.sqlcmd)
        for action in actions:
            if not 'DROP' in action.sqlcmd:
                self.log.debug("Fixing %s" % action.msg)
                self.log.debug("  Cmd: %s" % action.sqlcmd)
                self.cursor.execute(action.sqlcmd)
        self.dbconnection.commit()

    def count(self, query):
        sql = "select count(*) from %s" % (query.table())
        sql2, values = query.toSql()
        sql += sql2
        self.execute(sql, values)
        try:
            row = self.cursor.fetchone()
            if row != None:
                key = 'count(*)'
                rows = int(row[key])
            else:
                raise bc.Error(1, 'Cannot query for count(*) in %s' % (query.table()))
        except sqlite3.Error as e:
            raise bc.Error( 1, e.args[0] )
        return rows

    def select(self, query):
        """
        Fetch one or multiple rows from a database
        Returns an object that can be iterated over, returning rows
        If there is any errors, an exception is raised
        """
        sql = "SELECT * FROM %s" % query.table() 
        sql2, values = query.toSql()
        sql += sql2.replace("%s", "?")
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
                holder.append("?")
                vals.append(val)
        sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (table, ",".join(parms), ",".join(holder))
        self.execute(sql, vals, commit=True)
        return self.cursor.lastrowid

    def update(self, table, values):
        """Update a row in the table"""
        parms = []
        vals = []
        for key, val in values.items():
            if key != '_id':
                parms.append("%s=?" % key)
                vals.append(val)
            else:
                primary_key_val = val
        sql = "UPDATE %s SET %s WHERE _id=?" % (table, ",".join(parms))
        vals.append(primary_key_val)
        self.execute(sql, vals)

    def delete(self, query):
        """
        delete a row from a table
         "DELETE FROM EMPLOYEE WHERE AGE > '%d'" % (20)
        returns number of rows deleted
        """
        sql = "DELETE FROM %s" % query.table() 
        sql2, values = query.toSql()
        if sql2 == '':
            raise bc.Error(1, 'Missing query on delete(), empty query is not accepted')
        sql += sql2.replace("%s", "?")
        self.execute(sql, values)
        try:
            data = self.cursor.rowcount
        except sqlite3.Error as e:
            raise bc.Error( 1, e.args[0] )
        return data
