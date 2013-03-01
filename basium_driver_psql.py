#! /usr/bin/env python

# ----------------------------------------------------------------------------
#
# Basium database driver that handles PostgreSQL
#
# All database operations are tried twice if any error occurs, clearing the
# connection if an error occurs. This makes all operations to reconnect if the
# connection to the database has been lost.
#
# ----------------------------------------------------------------------------

#
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

__metaclass__ = type

import sys
import datetime
import psycopg2
import psycopg2.extras
import decimal

import basium_common
import basium_driver

Response=basium_common.Response
log = basium_common.log

#
# These are shadow classes from the basium_model
# handles the database specific functions such
# as convering to/from SQL types
#
class Column(object):

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
    # Todo: This should be part of the db driver
    #
    def tableTypeToSql(self, tabletype):
        if  tabletype['Key'] == 'PRI':
            tmp = 'serial'
        else:
            tmp = tabletype['Type']
            if tabletype['Null'] == 'NO':
                tmp += " not null"
            else:
                tmp += " null"
        return tmp


# stores boolean as number: 0 or 1
class BooleanCol(basium_driver.Column):

    def typeToSql(self):
        sql = "boolean"
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            if self.default:
                sql += " default TRUE"
            else:
                sql += " default FALSE"
        return sql

    def toPython(self, value):
        return value == 1

    def toSql(self, value):
        if value == None:
            return "NULL"
        if value:
            return 'TRUE'
        return 'FALSE'
    

# stores a date
class DateCol(basium_driver.Column):
    
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
        elif isinstance(value, basestring):
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S').date()
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return value


# stores date+time
# ignores microseconds
# if default is 'NOW' the current date+time is stored
class DateTimeCol(basium_driver.Column):
    
    def getDefault(self):
        if self.default == 'NOW':
            return datetime.datetime.now().replace(microsecond=0)
        return self.default

    def typeToSql(self):
        sql = 'timestamp without time zone'
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None and self.default != 'NOW':
            sql += " default %s" % self.default
        return sql

    def toPython(self, value):
        if isinstance(value, basestring):
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        return value


# stores a fixed precision number
# we cheat and represent this as a float in python
class DecimalCol(basium_driver.Column):
    
    def typeToSql(self):
        sql = 'decimal(%d,%d)' % (self.maxdigits, self.decimal)
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
        return value


# stores a floating point number
class FloatCol(basium_driver.Column):
    
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
        if isinstance(value, basestring):
            value = float(value)
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return str(value)

    
# stores an integer
class IntegerCol(basium_driver.Column):
    
    def typeToSql(self):
        if self.primary_key:
            return "SERIAL PRIMARY KEY"
        sql = 'integer'    # todo how to handle length? '(%s)' % self.length
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            if self.default: 
                sql += " default %i" % self.default
        return sql

    def toPython(self, value):
        if isinstance(value, basestring):
            value = int(value)
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return value


# stores a string
class VarcharCol(basium_driver.Column):

    def typeToSql(self):
        sql = 'varchar(%d)' % self.length
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            if self.default != '':
                sql += " default '%s'" % self.default
        return sql

    def toPython(self, value):
        if isinstance(value, unicode):
            value = str(value)
        return value


class Action:
    
    def __init__(self, msg=None, unattended=None, sqlcmd=None):
        self.msg = msg
        self.unattended = unattended
        self.sqlcmd = sqlcmd

class Driver:
    def __init__(self, host=None, port=None, username=None, password=None, name=None, debugSql=False):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.name = name
        self.debugSql = debugSql
        
        self.conn = None
        self.connectionStatus = None

    def connect(self):
        response = Response()
        try:
            conn_string = "host='%s' dbname='%s' user='%s' password='%s'" % \
                (self.host, self.name, self.username, self.password)
            self.conn = psycopg2.connect(conn_string)
            self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        except psycopg2.DatabaseError, e:
            response.setError( 1, str(e) )
        return response

    #
    # Execute a query, if error try to reconnect and redo the query
    # to handle timeouts
    #    
    def execute(self, sql, values = None, commit=False):
        response = Response()
        for i in range(0, 2):
            if self.conn == None:
                response = self.connect()
                if response.isError():
                    return response
            try:
                if self.debugSql or True:
                    log.debug(self.cursor.mogrify(sql, values))
                if values != None:
                    self.cursor.execute(sql, values)
                else:
                    self.cursor.execute(sql)
                if commit:
                    self.conn.commit()
                return response
                    
            except psycopg2.DatabaseError, e:
                if i == 0:
                    self.conn = None
                if i == 1:
                    response.setError( 1, str(e) )
                else:
                    pass
#                    try:
#                        self.conn.rollback()    # make sure to clear any previous hanging transactions
#                    except psycopg2.DatabaseError, e:
#                        pass
             
        return response
    
    #
    # Returns True if the database exist
    #
    def isDatabase(self, dbName):
        response = Response()
        sql = "select * from pg_database where datname=%s" # % dbName
        values = (dbName,)
        exist = False
        try:
            resp = self.execute(sql, values)
            if resp.isError():
                return resp
            row = self.cursor.fetchone()
            exist = row[0] == dbName
        except psycopg2.DatabaseError, e:
            response.setError( 1, str(e) )

        response.set('data', exist)
        return response

    #
    # Check if a table exist in the database
    #
    def isTable(self, tableName):
        response = Response()
        sql = "SELECT EXISTS(SELECT relname FROM pg_class WHERE relname=%s and relkind='r')"
        values = (tableName,)
        exist = False
        try:
            resp = self.execute(sql, values)
            if resp.isError():
                return resp
            row = self.cursor.fetchone()
            if row != None:
                exist = row[0]
        except psycopg2.DatabaseError, e:
            response.setError( 1, str(e) )

        response.set('data', exist)
        return response

    #
    # Create a table
    #
    def createTable(self, obj):
        response = Response()
        sql = 'CREATE TABLE %s (' % obj._table
        columnlist = []
        for (colname, column) in obj._columns.items():
            columnlist.append('"%s" %s' % (colname, column.typeToSql()))
        sql += ",".join(columnlist)
        sql += ')'
        res = self.execute(sql, commit=True)
        if res.isError():
            return res
        return response


    #
    # Verify that a table has the correct definition
    # Returns None if table does not exist
    # Returns list of Action, zero length if nothing needs to be done
    #
    def verifyTable(self, obj):
        response = Response()
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
#        for (colname, column) in obj._columns.items():
#            if colname in tabletypes:
#                tabletype = tabletypes[colname]
#                if column.typeToSql() != column.tableTypeToSql(tabletype):
#                    msg = "Error: Column '%s' has incorrect type in SQL Table. Action: Change column type in SQL Table" % (colname)
#                    log.debug(msg)
#                    log.debug("  type in Object   : '%s'" % (column.typeToSql()) )
#                    log.debug("  type in SQL table: '%s'" % (column.tableTypeToSql(tabletype)))
#                    actions.append(Action(
#                            msg=msg,
#                            unattended=True,
#                            sqlcmd='ALTER TABLE %s CHANGE %s %s %s' % (obj._table, colname, colname, column.typeToSql())
#                            ))
#            else:
#                msg = "Error: Column '%s' does not exist in the SQL Table. Action: Add column to SQL Table" % (colname)
#                print " ", msg
#                actions.append(Action(
#                        msg=msg,
#                        unattended=True,
#                        sqlcmd='ALTER TABLE %s ADD %s %s' % (obj._table, colname, column.typeToSql())
#                        ))
#
#        for (colname, tabletype) in tabletypes.items():
#            if not colname in obj._columns:
#                actions.append(Action(
#                        msg="Error: Column '%s' in SQL Table NOT used, should be removed" % colname,
#                        unattended=False,
#                        sqlcmd='ALTER TABLE %s DROP %s' % (obj._table, colname)
#                        ))
#        if len(actions) < 1:
#            log.debug("SQL Table '%s' matches the object" % obj._table)
#        else:
#            log.debug("SQL Table '%s' DOES NOT match the object, need changes" % obj._table)
        response.set('actions', actions)
        return response


    #
    # Update table to latest definition of class
    # actions is the result from verifyTable
    # Returns True if everything is ok
    #
    def modifyTable(self, obj, actions):
#        response = Response()
#        log.debug("Updating table %s" % obj._table)
#        if len(actions) == 0:
#            log.debug("  Nothing to do")
#            return True
#
#        print "Actions that needs to be done:"
#        askForConfirmation = False
#        for action in actions:
#            print "  ", action.msg
#            print "   SQL:", action.sqlcmd
#            if not action.unattended:
#                askForConfirmation = True
#
#        if askForConfirmation:
#            print "WARNING: removal of columns can lead to data loss."
#            a = raw_input('Are you sure (yes/No)? ')
#            if a != 'yes':
#                print "Aborted!"
#                return False
#
#        # we first remove columns, so we dont get into conflicts
#        # with the new columns, for example changing primary key (there can only be one primary key)
#        for action in actions:
#            if 'DROP' in action.sqlcmd:
#                print "Fixing", action.msg
#                print "  Cmd:", action.sqlcmd
#                self.cursor.execute(action.sqlcmd)
#        for action in actions:
#            if not 'DROP' in action.sqlcmd:
#                print "Fixing", action.msg
#                print "  Cmd:", action.sqlcmd
#                self.cursor.execute(action.sqlcmd)
#        self.conn.commit()
        return True

    #
    #
    #           
    def count(self, query):
        table = query._model._table
        sql = "select count(*) from %s" % (table)
        sql2, values = query.toSql()
        sql += sql2

        response = self.execute(sql, values)
        if not response.isError():
            row = self.cursor.fetchone()
            if row != None:
                response.set('data', int(row[0]) )
            else:
                response.setError(1, 'Cannot query for count(*) in %s' % (table))
        return response

    #
    # Fetch one or multiple rows from a database
    # Return data as list, each with a dictionary
    #
    def select(self, query):
        rows = []
        sql = "SELECT * FROM %s" % query._model._table 
        sql2, values = query.toSql()
        sql += sql2
        response = self.execute(sql, values)
        if not response.isError():
            for row in self.cursor:
                resp = {}
                for colname in row.keys():
                    resp[colname] = row[colname]
                rows.append(resp)
        response.set('data', rows)
        return response


    #
    # Insert a row in the table
    # value is a dictionary with columns, excluding primary key
    #
    def insert(self, table, values):
        parms = []
        holder = []
        vals = []
        for key, val in values.items():
            if key != 'id':
                parms.append('"' + key + '"')
                holder.append("%s")
                vals.append(val)
        sql = "INSERT INTO %s ( %s ) VALUES ( %s ) RETURNING id" % (table, ",".join(parms), ",".join(holder))
        response = self.execute(sql, vals, commit=True)
        if not response.isError():
            response.set('data', self.cursor.fetchone()[0])
#        if not response.isError():
#            response = self.execute("select lastval();")
#            if not response.isError():
#                response.set('data', self.cursor.fetchone()[0])
        return response

    #
    # update a row in the table
    #
    def update(self, table, values):
        parms = []
        vals = []
        for key, val in values.items():
            if key != 'id':
                parms.append('"%s"=%%s' % key)
                vals.append(val)
            else:
                primary_key_val = val
        sql = "UPDATE %s SET %s WHERE %s=%%s" % (table, ",".join(parms), 'id')
        vals.append(primary_key_val)
        response = self.execute(sql, vals, commit=True)
        response.set('data', None)
        return response

    #
    # delete a row from a table
    # "DELETE FROM EMPLOYEE WHERE AGE > '%s'", (20, )
    #
    def delete(self, query):
        sql = "DELETE FROM %s" % query.getTable()
        sql2, values = query.toSql()
        if sql2 == '':
            return Response(1, 'Missing query on delete(), empty query is not accepted')
        sql += sql2
        response = self.execute(sql, values, commit=True)
        if not response.isError():
            response.set('data', self.cursor.rowcount)
        return response
