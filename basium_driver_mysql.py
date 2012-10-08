#! /usr/bin/env python

#
# Object persistence for Python and MySQL
#

#
# Copyright (c) 2012, Anders Lowinger, Abundo AB
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

import sys
import inspect
import datetime
import MySQLdb
import urlparse
import types
import decimal

import basium_common # as util
from basium_common import Response

log = basium_common.log

#
class Action:
    
    def __init__(self, msg=None, unattended=None, sqlcmd=None):
        self.msg = msg
        self.unattended = unattended
        self.sqlcmd = sqlcmd


# ----------------------------------------------------------------------------
#
# Database driver that handles MySQL
#
# All database operations are tried twice if any error occurs, clearing the
# connection if an error occurs. This makes all operations to reconnect if the
# connection to mysql has been lost.
#
# ----------------------------------------------------------------------------
class Driver:
    def __init__(self, host=None, port=None, username=None, password=None, name=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.name = name
        
        self.conn = None
        self.connectionStatus = None

    def connect(self):
        response = Response()
        try:
            if self.port != None and self.port != '':
                self.conn = MySQLdb.connect (
                                        host=self.host,
                                        port=int(self.port),
                                        user=self.username,
                                        passwd=self.password,
                                        db=self.name)
            else:
                self.conn = MySQLdb.connect (
                                        host=self.host,
                                        user=self.username,
                                        passwd=self.password,
                                        db=self.name)
                
            self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor)
        except MySQLdb.Error, e:
            response.setError( e.args[0], e.args[1] )
            
        return response

    #
    # Execute a query, if error try to reconnect and redo the query
    # to handle timeouts
    #    
    def execute(self, sql, values = None):
        response = Response()
        for i in range(0, 2):
            if self.conn == None:
                response = self.connect()
                if response.isError():
                    return response
            try:
                if values != None:
                    self.cursor.execute(sql, values)
                else:
                    self.cursor.execute(sql)
                return response
                    
            except MySQLdb.Error, e:
                if i == 1:
                    response.setError( e.args[0], e.args[1] )
            
        return response
    
    #
    # Returns True if the database exist
    #
    def isDatabase(self, dbName):
        response = Response()
        sql = "SELECT IF(EXISTS (SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s'), 'Yes','No')" % dbName
        exist = False
        try:
            resp = self.execute(sql)
            if resp.isError():
                return resp
            row = self.cursor.fetchone()
            exist = row.values()[0] == 'Yes'
        except MySQLdb.Error, e:
            response.set(e.args[0], e.args[1])

        response.set('data', exist)
        return response

    #
    # Check if a table exist in the database
    #
    def isTable(self, tableName):
        response = Response()
        sql = "show tables like %s"
        exist = False
        try:
            resp = self.execute(sql, tableName)
            if resp.isError():
                return resp
            row = self.cursor.fetchone()
            if row != None:
                exist = row.values()[0] == tableName
        except MySQLdb.Error, e:
            response.set(e.args[0], e.args[1])

        response.set('data', exist)
        return response

    #
    # Create a tableName
    #
    def createTable(self, obj):
        response = Response()
        sql = 'CREATE TABLE %s (\n   ' % obj._table
        columnlist = []
        for (colname, column) in obj._columns.items():
            columnlist.append('%s %s' % (colname, column.typeToSql()))
        sql += "\n  ,".join(columnlist)
        sql += '\n)'
        res = self.execute(sql)
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
        sql = 'DESCRIBE %s' % obj._table
        for i in range(0,2):
            if not self.connect():
                return self.connectionStatus
            try:
                self.cursor.execute(sql)
                rows = self.cursor.fetchall()
                break
            except MySQLdb.Error, e:
                response.setError(e.args[0], e.args[1])
        if response.isError():
            return response
        tabletypes = {}
        for row in rows:
            tabletypes[row['Field']] = row
        actions = []
        for (colname, column) in obj._columns.items():
            if colname in tabletypes:
                tabletype = tabletypes[colname]
                if column.typeToSql() != column.tableTypeToSql(tabletype):
                    msg = "Error: Column '%s' has incorrect type in SQL Table. Action: Change column type in SQL Table" % (colname)
                    log.debug(msg)
                    log.debug("  type in Object   : '%s'" % (column.typeToSql()) )
                    log.debug("  type in SQL table: '%s'" % (column.tableTypeToSql(tabletype)))
                    actions.append(Action(
                            msg=msg,
                            unattended=True,
                            sqlcmd='ALTER TABLE %s CHANGE %s %s %s' % (obj._table, colname, colname, column.typeToSql())
                            ))
            else:
                msg = "Error: Column '%s' does not exist in the SQL Table. Action: Add column to SQL Table" % (colname)
                print " ", msg
                actions.append(Action(
                        msg=msg,
                        unattended=True,
                        sqlcmd='ALTER TABLE %s ADD %s %s' % (obj._table, colname, column.typeToSql())
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
        response.set('actions', actions)
        return response


    #
    # Update table to latest definiton of class
    # actions is the result from verifytable
    #
    def modifyTable(self, obj, actions):
        response = Response()
        log.debug("Updating table %s" % obj._table)
        if len(actions) == 0:
            log.debug("  Nothing to do")
            return False

        print "Actions that needs to be done:"
        askForConfirmation = False
        for action in actions:
            print "  ", action.msg
            print "   SQL:", action.sqlcmd
            if not action.unattended:
                askForConfirmation = True

        if askForConfirmation:
            print "WARNING: removal of columns can lead to data loss."
            a = raw_input('Are you sure (yes/No)? ')
            if a != 'yes':
                print "Aborted!"
                return True

        # we first remove columns, so we dont get into conflicts
        # with the new columns, for example changing primary key (there can only be one primary key)
        for action in actions:
            if 'DROP' in action.sqlcmd:
                print "Fixing", action.msg
                print "  Cmd:", action.sqlcmd
                self.cursor.execute(action.sqlcmd)
        for action in actions:
            if not 'DROP' in action.sqlcmd:
                print "Fixing", action.msg
                print "  Cmd:", action.sqlcmd
                self.cursor.execute(action.sqlcmd)
        self.conn.commit()
        return False

    #
    #
    #           
    def count(self, query):
        response = Response()
        sql = "select count(*) from %s" % (query._cls._table)
        values = []
        sql2, values = query.toSql()
        sql += sql2
        log.debug('count(*), sql=%s, values=%s' % (sql, values))
        rows = 0
        for i in range(0,2):
            if not self.connect():
                return self.connectionStatus
            try:
                self.cursor.execute(sql, values)
                row = self.cursor.fetchone()
                if row != None:
                    rows = int(row['count(*)'])
                else:
                    response.setError(1, 'Cannot query for count(*) in %s' % (query._cls._table))
                break
            except MySQLdb.Error, e:
                response.set(e.args[0], e.args[1])
                self.conn = False
        
        response.set('data', rows)
        return response

    #
    # Fetch one or multiple rows from a database
    # Return data as list, each with a dictionary
    #
    def select(self, query):
        response = Response()
        rows = []
        sql = "SELECT * FROM %s" % query._cls._table 
        sql2, values = query.toSql()
        sql += sql2
        log.debug('sql=%s, values=%s)' %( sql, values))
        for i in range(0,2):
            if not self.connect():
                return self.connectionStatus
            try:
                self.cursor.execute(sql, tuple(values))
                rowcount = int(self.cursor.rowcount)
                for i in range(rowcount):
                    row = self.cursor.fetchone()
                    resp = {}
                    for (colname, column) in row.items():
                        resp[colname] = row[colname]
                    rows.append(resp)
                response.set('data', rows)
                break
            except MySQLdb.Error, e:
                response.set(e.args[0], e.args[1])
                self.conn = False
        return response


    #
    # Insert a row in the table
    # value is a dictionary with columns, excluding primary key
    #
    def insert(self, table, values):
        response = Response()
        parms = "%s, " * len(values) #[:-2]   #
        sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (table, ", ".join(values.keys()), parms[:-2])
        log.debug('sql=%s, values=%s)' %( sql, values))
        for i in range(0,2):
            if not self.connect():
                return self.connectionStatus
            try:
                self.cursor.execute(sql, values.values())
                self.conn.commit()
                response.set('data', self.cursor.lastrowid)
                break
            except MySQLdb.Error, e:
                response.set(e.args[0], e.args[1])
                self.conn = False
        return response

    #
    # update a row in the table
    #
    def update(self, table, values, primary_key):
        response = Response()
        columns = "=%s, " * len(values)[:-2]
        values.append(primary_key['id'])
        sql = "UPDATE %s SET %s WHERE %s=%%s" % (table, columns, 'id')
        log.debug('sql=%s, values=%s)' %( sql, values))
        for i in range(0,2):
            if not self.connect():
                return self.connectionStatus
            try:
                self.cursor.execute(sql, tuple(values))
                self.conn.commit()
                break
            except MySQLdb.Error, e:
                response.set(e.args[0], e.args[1])
                self.conn = False
        return response

    #
    # delete a row from a table
    #  "DELETE FROM EMPLOYEE WHERE AGE > '%d'" % (20)
    #
    def delete(self, table, query = None):
        response = Response()
        sql = "DELETE FROM %s" % table
        sql2, values = query.toSql()
        if sql2 == '':
            response.setError(1, 'Missing query on delete(), empty query is not accepted')
            return response
        sql += sql2
        log.debug('sql=%s, values=%s)' %( sql, values))
        for i in range(0,2):
            if not self.connect():
                return self.connectionStatus
            try:
                self.cursor.execute(sql, values)
                row = self.cursor.fetchone()
                if row != None:
                    response.set('data', None)
                break
            except MySQLdb.Error, e:
                response.set(e.args[0], e.args[1])
                self.conn = False
        return response


#
# Main
#
if __name__ == "__main__":
    pass
