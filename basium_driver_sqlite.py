#!/usr/bin/env python

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

from __future__ import print_function
from __future__ import unicode_literals
__metaclass__ = type

import datetime
import decimal
import sqlite3

import basium
import basium_driver

Response=basium.Response
log = basium.log

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
        elif basium.isstring(value):
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
    if default is 'NOW' the current date+time is stored
    """
    
    def getDefault(self):
        if self.default == 'NOW':
            return datetime.datetime.now().replace(microsecond=0)
        return self.default

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
        if basium.isstring(value):
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
        if basium.isstring(value):
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
        if basium.isstring(value):
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
    def __init__(self, dbconf=None):
        self.dbconf = dbconf
        
        self.dbconnection = None
        self.connectionStatus = None

    def connect(self):
        response = Response()
        try:
            self.dbconnection = sqlite3.connect(self.dbconf.database)
            self.dbconnection.row_factory = sqlite3.Row   # return querys as dictionaries
            self.cursor = self.dbconnection.cursor()
        except sqlite3.Error as e:
            response.setError( 1, e.args[0] )

        return response

    def execute(self, sql, values=None, commit=True):
        """
        Execute a query, if error try to reconnect and redo the query
        to handle timeouts
        """
        response = Response()
        for i in range(0, 2):
            if self.dbconnection == None:
                response = self.connect()
                if response.isError():
                    return response
            try:
                if self.dbconf.debugSQL:
                    log.debug('SQL=%s' % sql)
                if values != None:
                    self.cursor.execute(sql, values)
                else:
                    self.cursor.execute(sql)
                if commit:
                    self.dbconnection.commit()
                return response
                    
            except sqlite3.Error as e:
                if i == 1:
                    response.setError( 1, e.args[0] )
            
        return response
    
    def isDatabase(self, dbName):
        """
        Returns True if the database exist
        We always return true, sqlite automatically creates the database when opened
        """
        response = Response()
        response.data = True
        return response

    def isTable(self, tableName):
        """Returns True if the table exist"""
        sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        values = (tableName,)
        exist = False
        response = self.execute(sql, values)
        if not response.isError():
            try:
                row = self.cursor.fetchone()
                if row != None:
                    exist = row[0] == tableName
            except sqlite3.Error as e:
                response.setError( 1, e.args[0] )
        response.data = exist
        return response

    def createTable(self, obj):
        """Create a table"""
        sql = 'CREATE TABLE %s (' % obj._table
        columnlist = []
        for (colname, column) in obj._columns.items():
            columnlist.append('%s %s' % (colname, column.typeToSql()))
        sql += "  ,".join(columnlist)
        sql += ')'
        response = self.execute(sql)
        return response

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
        response = self.execute(sql)
        if response.isError():
            return response

        # cid, name, type, notnull, dflt_value, pk
        tabletypes = {}
        rows = self.cursor.fetchall()
        for row in rows:
            tabletypes[row[1]] = row
        actions = []
        for (colname, column) in obj._columns.items():
            if colname in tabletypes:
                tabletype = tabletypes[colname]
                columntype_str = column.typeToSql()
                tabletype_str = self.tableTypeToSql(tabletype)
                if columntype_str != tabletype_str:
                    msg = "Error: Column '%s' has incorrect type in SQL Table. Action: Change column type in SQL Table" % (colname)
                    log.debug(msg)
                    log.debug("  type in Object   : '%s'" % (columntype_str) )
                    log.debug("  type in SQL table: '%s'" % (tabletype_str))
                    actions.append(Action(
                            msg=msg,
                            unattended=True,
                            sqlcmd='ALTER TABLE %s CHANGE %s %s %s' % (obj._table, colname, colname, columntype_str)
                            ))
            else:
                msg = "Error: Column '%s' does not exist in the SQL Table. Action: Add column to SQL Table" % (colname)
                print(" %s" % msg)
                actions.append(Action(
                        msg=msg,
                        unattended=True,
                        sqlcmd='ALTER TABLE %s ADD COLUMN %s %s' % (obj._table, colname, column.typeToSql())
                        ))

        for (colname, tabletype) in tabletypes.items():
            if not colname in obj._columns:
                actions.append(Action(
                        msg="Error: Column '%s' in SQL Table NOT used, should be removed" % colname,
                        unattended=False,
                        sqlcmd='ALTER TABLE %s DROP %s' % (obj._table, colname)
                        ))
        if len(actions) < 1:
            log.debug("SQL Table '%s' matches the object" % obj._table)
        else:
            log.debug("SQL Table '%s' DOES NOT match the object, need changes" % obj._table)
        response.data = actions
        return response

    def modifyTable(self, obj, actions):
        """
        Update table to latest definition of class
        actions is the result from verifytable
        todo: sqlite only support a subset of functionality in "ALTER TABLE...", so we work around this
        by copying the table to a new one
        """
        response = Response()
        log.debug("Updating table %s" % obj._table)
        if len(actions) == 0:
            log.debug("  Nothing to do")
            return False

        print("Actions that needs to be done:")
        askForConfirmation = False
        for action in actions:
            print("  %s" % action.msg)
            print("   SQL: %s" % action.sqlcmd)
            if not action.unattended:
                askForConfirmation = True

        if askForConfirmation:
            print("WARNING: removal of columns can lead to data loss.")
            a = basium.rawinput('Are you sure (yes/No)? ')
            if a != 'yes':
                print("Aborted!")
                return True

        # we first remove columns, so we dont get into conflicts
        # with the new columns, for example changing primary key (there can only be one primary key)
        for action in actions:
            if 'DROP' in action.sqlcmd:
                print("Fixing %s" % action.msg)
                print("  Cmd: %s" % action.sqlcmd)
                self.cursor.execute(action.sqlcmd)
        for action in actions:
            if not 'DROP' in action.sqlcmd:
                print("Fixing %s" % action.msg)
                print("  Cmd: %s" % action.sqlcmd)
                self.cursor.execute(action.sqlcmd)
        self.dbconnection.commit()
        return False

    def count(self, query):
        sql = "select count(*) from %s" % (query._model._table)
        sql2, values = query.toSql()
        sql += sql2
        rows = 0
        response = self.execute(sql, values)
        if not response.isError():
            try:
                row = self.cursor.fetchone()
                if row != None:
                    key = basium.b('count(*)')
                    rows = int(row[key])
                else:
                    response.setError(1, 'Cannot query for count(*) in %s' % (query._model._table))
            except sqlite3.Error as e:
                response.setError( 1, e.args[0] )
        response.data = rows
        return response

    def select(self, query):
        """
        Fetch one or multiple rows from a database
        Return data as list, each with a dictionary
        """
        rows = []
        sql = "SELECT * FROM %s" % query._model._table 
        sql2, values = query.toSql()
        sql += sql2.replace("%s", "?")
        response = self.execute(sql, values)
        if not response.isError():
            try:
                for row in self.cursor:
                    resp = {}
                    for colname in row.keys():
                        resp[colname] = row[colname]
                    rows.append(resp)
                response.data = rows
            except sqlite3.Error as e:
                response.setError( 1, e.args[0] )
                self.dbconnection = False
        return response

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
        response = self.execute(sql, vals, commit=True)
        if not response.isError():
            response.data = self.cursor.lastrowid
        return response

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
        response = self.execute(sql, vals)
        return response

    def delete(self, query):
        """
        delete a row from a table
         "DELETE FROM EMPLOYEE WHERE AGE > '%d'" % (20)
        """
        sql = "DELETE FROM %s" % query._model._table 
        sql2, values = query.toSql()
        if sql2 == '':
            return Response(1, 'Missing query on delete(), empty query is not accepted')
        sql += sql2.replace("%s", "?")
        response = self.execute(sql, values)
        if not response.isError():
            try:
                response.data = self.cursor.rowcount
            except sqlite3.Error as e:
                response.setError( 1, e.args[0] )
        return response
