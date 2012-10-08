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
#

import sys
import inspect
import datetime
import MySQLdb
import pprint
import urllib
import urllib2
import urlparse
import json
import types
import decimal

import basium_common

from basium_model import *

log = basium_common.log


# to make less errors in queries
LT = '<'
LE = '<='
EQ = '='
GT = '>'
GE = '>='
NE = '!='


# ----------------------------------------------------------------------------
#
#  Main database class, implement load, store, count, delete of objects
#  Uses a database driver, MySQL or JSON to do the actual work
#
# ----------------------------------------------------------------------------

class BasiumDatabase:

    #
    def __init__(self, driver = None):
        self.driver = driver
#        self.driver.connect()

    
    #
    # Returns
    #    True if the database exist
    #    False if the database does not exist
    #    None  if there was an error
    #
    def isDatabase(self, dbName):
        result = self.driver.isDatabase(dbName)
        if result.isError():
            log.error("Cannot check if database exist. %s" % (result.getError()) )
            return None

        exist = result.get('data') 
        if exist:
            log.debug("SQL Database '%s' exist" % dbName)
        else:
            log.error("SQL Database does NOT exist, it needs to be created")
        return exit


    #
    # Check if a table exist in the database
    #
    def isTable(self, obj):
        result = self.driver.isTable(obj._table)
        if result == None:
            log.error("Cannot check if SQL Table '%s' exist. %s" % (obj._table, result.getError()))
            return False
        exist = result.get('data')
        if exist:
            log.debug("SQL Table '%s' does exist" % obj._table)
        else:
            log.debug("SQL Table '%s' does NOT exist" % obj._table)
        return exist

    

    #
    # Create a obj
    #
    def createTable(self, obj):
        result = self.driver.createTable(obj)
        if result.isError():
            log.error("Creation of SQL Table '%s' failed. %s" % (result.getError()))
            return False
        return True


    #
    # Verify that a table has the correct definition
    # Returns None if table does not exist
    # Returns list of Action, zero length if nothing needs to be done
    #
    def verifyTable(self, obj):
        result = self.driver.verifyTable(obj)
        actions = result.get('actions')
        if len(actions) < 1:
            log.debug("SQL Table '%s' matches the object" % obj._table)
        else:
            log.debug("SQL Table '%s' DOES NOT match the object, need changes" % obj._table)
        return actions


    #
    # Update table to latest definiton of class
    # actions is the result from verifytable
    #
    def modifyTable(self, obj, actions):
        result = self.driver.modifyTable(obj, actions)
        if result == None:
            log.error('Fatal: Cannot update table structure for %s. %s' % (obj._table, result.getError()))
            return False
        return True

    #
    #
    #           
    def count(self, query_):
        if isinstance(query_, Model):
            query = Query(self, query_.__class__)
        elif isinstance(query_, Query):
            query = query_
        else:
            log.error("Fatal: incorrect object type in count")
            return None
        result = self.driver.count(query)
        if result.isError():
            log.error('Cannot do count(*) on %s' % (query._table))
            return None
        return int(result.get('data'))


    #
    # Fetch one or multiple rows from table, each stored in a object
    # If no query is specified, the default is to fetch one object with the query.id
    # Query can be either
    #   An instance of Model
    #   Query()
    # Returns
    #   list of objects, one or more if ok
    #   None if error
    #
    #
    def load(self, query_):
        response = basium_common.Response()
        if isinstance(query_, Model):
            query = Query(self, query_.__class__).filter('id', EQ, query_.id)
        elif isinstance(query_, Query):
            query = query_
        else:
            response.setError(1, "Fatal: incorrect object type in load()")
            return response
        response = self.driver.select(query)
        if response.isError():
            return response
        rows = []
        for row in response.get('data'):
            newobj = query._cls()
            for (colname, column) in newobj._columns.items():
                newobj._values[colname] = column.toPython( row[colname] )
            rows.append(newobj)
        response.set('data', rows)
        return response

    
    #
    # Store the query in the database
    # If the objects ID is set, we update the current db row,
    # otherwise we create a new row
    #
    def store(self, obj):
        columns = {}
        for (colname, column) in obj._columns.items():
            if not obj.isPrimaryKey(colname):
                columns[colname] = column.toSql(obj._values[colname])

        if obj.id >= 0:
            # update
            primary_key = {'id': obj.id}
            response = self.driver.update(obj._table, columns, primary_key)
            if response.isError():
                return False
        else:
            # insert
            response = self.driver.insert(obj._table, columns)
            if response.isError():
                return False
#            print response
            obj.id = response.get('data')
        return True
    
    #
    #  "DELETE FROM BasiumTest WHERE AGE > '%d'" % (20)
    #
    def delete(self, query_):
        response = basium_common.Response()
        if isinstance(query_, Model):
            query = Query(self, query_.__class__).filter('id', EQ, query_.id)
        elif isinstance(query_, Query):
            query = query_
        else:
            response.setError(1, "Fatal: incorrect object type in delete()")
            return response
        response = self.driver.delete(query) 
        return response

    #
    # Create and return a query object
    def query(self, Cls):
        q = Query(self, Cls)
        return q


# ----------------------------------------------------------------------------
#
# Class that build queries
#
# ----------------------------------------------------------------------------

class Query():

    #
    class Where:
        def __init__(self, column = None, operand = None, value = None):
            self.column = column
            self.operand = operand
            self.value = value

        def toSql(self, column):
            sql = '%s %s %%s' % (self.column, self.operand )
            value = column.toSql( self.value )
            return (sql, value)
        
        def encode(self):
            return "w=" + urllib.quote("%s,%s,%s" % (self.column, self.operand, self.value), ',:=' )

        def decode(self, value):
            self.column, self.operand, self.value = value.split(',')

    #
    class Group:
        def __init__(self):
            pass

    #        
    class Order:
        def __init__(self):
            pass

    #
    class Limit:
        def __init__(self, offset = None, rowcount = None):
            self.offset = offset
            self.rowcount = rowcount

        def toSql(self):
            offset = 0
            if self.offset != None:
                offset = self.offset
            return ' LIMIT %i, %i' % ( offset, self._maxcount )

        def encode(self):
            pass
        
        def decode(self, value):
            pass

    #
    def reset(self):
        self._where = []
        self._group = []
        self._order = []
        self._limit = None
    
    #
    def __init__(self, db, cls):
        if not isinstance(cls, (type, types.ClassType)):
            log.debug('Fatal: Query constructor called with an instance of an object')
            sys.exit(1)
        self._db = db
        self._cls = cls
        self.reset()

    #
    def isId(self):
        if len(self._where) != 1:
            return False
        w = self._where[0]
        return w.column == 'id' and w.operand == '='

    #
    # Add a filter. Returns self so it can be chained
    #
    def filter(self, column, operand, value):
        self._where.append(self.Where(column=column, operand=operand, value=value))
        return self

    #
    # Add a group. Returns self so it can be chained
    #
    def group(self):
        return self


    #
    # Add a sort order. Returns self so it can be chained
    #
    def order(self, order, desc = False):
        self._order = order 
        return self

    #
    # Offset and maximum number of rows that should be returned
    # Offset is optional
    # Maximum number of rows is mandatory
    # Can be called once, if multiple calls last one wins
    # 
    #
    def limit(self, offset = None, rowcount = None):
        self._rowcount = rowcount
        return self
        
    #
    # Return the query 
    # Handles
    # - WHERE
    # - GROUP BY
    # - ORDER BY
    # - LIMIT
    #
    def toSql(self):
        if len(self._where) < 1:
            return ('', [])
        sql = ' where ('
        value = []
        add = False
        if self._where != None:
            for where in self._where:
                if add:
                    sql += ' and '
                else:
                    add = True
                # get the instance variable for the column
                column = self._cls._columns[where.column]
                sql2, value2 = where.toSql(column)
                sql += sql2
                value.append(value2)
            sql += ')'
        
        if self._limit != None:
            sql += self._limit.encode()
        return (sql, value)

    #
    # Return the query as a string that can be appended to an URI
    #
    def encode(self):
        url = []
        
        for where in self._where:
            url.append(where.encode() )

        # group
        
        # order
        
        # limit
        return "&".join(url)

    #
    # Decode an URL query and update this query object
    #
    def decode(self, url):
        u = urlparse.parse_qsl(url)
        print u
        self.reset()
        for (key, val) in u:
            if key == 'w':
                w = self.Where()
                w.decode(val)
                self._where.append(w)
            elif key == 'g':
                g = self.Group()
                g.decode(val)
                self._group.append(g)
            elif key == 'o':
                o = self.Order()
                o.decode(val)
                self._order.append(o)
                pass
            elif key == 'l':
                l = self.Limit()
                l.decode(val)
                self._limit = l
            else:
                log.error("Incorrect key=%s, url='%s' in URL" % (key, url))


#
# Future tests
#
if __name__ == "__main__":
    pass
