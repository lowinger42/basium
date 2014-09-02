#!/usr/bin/env python
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
#

"""
Object persistence for Python

This class handles all mapping between objects and dictionaries,
before calling database driver, or returning objects
"""

from __future__ import absolute_import, division, print_function, unicode_literals
__metaclass__ = type

import inspect

from basium_common import *

import basium_compatibilty as c
import basium_model
import basium_driver

# to make less errors in queries
LT = '<'
LE = '<='
EQ = '='
GT = '>'
GE = '>='
NE = '!='


class BasiumOrm:

    def startOrm(self, driver = None, drivermodule = None):
        """
        mixin, let the various model classes also inherit from the
        corresponding driver classes. We change the class so all
        future instances will have correct base classes
        The driver class is inserted first, so it overrides any generic basium_model class
        
        todo: is there a better way to do this?
              it would be nice to have the model instance decoupled from the driver
        """
        self.driver = driver
        self.drivermodule = drivermodule

        drvclasses = {}
        for tmp in inspect.getmembers(self.drivermodule, inspect.isclass):
            drvclasses[tmp[0]] = tmp[1]
        for modelclsname, modelcls in inspect.getmembers(basium_model, inspect.isclass):
            if issubclass(modelcls, basium_model.Column) and modelclsname != 'Column':
                # ok, found one, get the drivers corresponding class
                if modelclsname in drvclasses:
                    if not drvclasses[modelclsname] in modelcls.__bases__:
                        modelcls.__bases__ = (drvclasses[modelclsname],) + modelcls.__bases__
                else:
                    self.log.error('Driver %s is missing Class %s' % (self.drivermodule.__name__, modelclsname))
                    return False
        return True
    
    def isDatabase(self, dbName):
        """
        Returns
           True if the database exist
           False if the database does not exist
           None  if there was an error
        """
        exist = self.driver.isDatabase(dbName) 
        if exist:
            self.log.debug("SQL Database '%s' exist" % dbName)
        else:
            self.log.debug("SQL Database '%s' does NOT exist, it needs to be created" % dbName)
        return exist

    def isTable(self, obj):
        """Check if a table exist in the database"""
        exist = self.driver.isTable(obj._table)
        if exist:
            self.log.debug("SQL Table '%s' does exist" % obj._table)
        else:
            self.log.debug("SQL Table '%s' does NOT exist" % obj._table)
        return exist

    def createTable(self, obj):
        """Create a table that can store objects"""
        self.driver.createTable(obj)
        return True

    def verifyTable(self, obj):
        """
        Verify that a table has the correct definition
        Returns None if table does not exist
        Returns list of Action, zero length if nothing needs to be done
        """
        actions = self.driver.verifyTable(obj)
        if len(actions) < 1:
            self.log.debug("SQL Table '%s' matches the object" % obj._table)
        else:
            self.log.debug("SQL Table '%s' DOES NOT match the object, need changes" % obj._table)
        return actions

    def modifyTable(self, obj, actions):
        """
        Update table to latest definition of class
        actions is the result from verifyTable
        """
        self.driver.modifyTable(obj, actions)
        return True

    def count(self, query_):
        if isinstance(query_, basium_model.Model):
            query = Query(query_)
        elif isinstance(query_, Query):
            query = query_
        else:
            raise c.Error(1, "Fatal: incorrect object type in count")
        return self.driver.count(query)

    def load(self, query_):
        """
        Fetch one or multiple rows from table, each stored in a object
        If no query is specified, the default is to fetch one object identified with the object._id
        Query can be either
            An instance of Model()
            An instance of Query()
        Driver returns an object that can be iterated over one row at a time
        or throws DriverError
        
        Note: when loading a single object, an error is returned if not found. 
        Workaround is to use a query instead
        """
        one = False
        if isinstance(query_, basium_model.Model):
            query = Query().filter(query_.q._id, EQ, query_._id)
            one = True
        elif isinstance(query_, Query):
            query = query_
        else:
            raise c.Error(1, "Fatal: incorrect object type")

        data = []
        for row in self.driver.select(query):
            newobj = query._model.__class__()
            for colname,column in newobj._iterNameColumn():
                try:
                    newobj._values[colname] = column.toPython( row[colname] )
                except (KeyError, ValueError):
                    pass
            data.append(newobj)
        if one and len(data) < 1:
            raise c.Error(1, "Unknown ID %s in table %s" % (query_._id, query_._table))

        return data
    
    def store(self, obj):
        """
        Store the query in the database
        If the objects _id is set, we update the current row in the table,
        otherwise we create a new row
        """
        columns = {}
        for colname, column in obj._iterNameColumn():
            columns[colname] = column.toSql(obj._values[colname])

        if obj._id >= 0:
            # update
            data = self.driver.update(obj._table, columns)
        else:
            # insert
            obj._id = self.driver.insert(obj._table, columns)
        return obj._id
    
    def delete(self, query_):
        """
        Delete objects in the table.
        query_ can be either
            An instance of Model()
            An instance of Query()
        
        If instance of model, that instance will be deleted
        If query, the objects matching the query is deleted
        """ 
        one = False
        if isinstance(query_, basium_model.Model):
            query = Query().filter(query_.q._id, EQ, query_._id)
            one = True
        elif isinstance(query_, Query):
            query = query_
        else:
            raise c.Error(1, "Fatal: incorrect object type passed")
        rowcount = self.driver.delete(query)
        if one:
            query_._id = -1
        return rowcount

    def query(self, obj = None):
        """
        Create and return a query object
        Convenience method, makes it unnecessary to import the basium_orm module
        just for doing queries
        """
        q = Query(obj)
        return q


class Query():
    """
    Class that build queries
    """

    def __init__(self, model = None, log=None):
        self._model = model
        self.log = log

        self._table = None
        if model:
            if not isinstance(model, basium_model.Model):
                self.log.error('Fatal: Query() called with a non-Model object')
                return
            self._table = model._table
        self._reset()

    def _reset(self):
        self._where = []
        self._group = []
        self._order = []
        self._limit = None

    def isId(self):
        if len(self._where) != 1:
            return False
        w = self._where[0]
        return w.column.name == '_id' and w.operand == '='

    def table(self):
        return self._table

    class _Where:
        def __init__(self, column = None, operand = None, value = None):
            self.column = column
            self.operand = operand
            self.value = value

        def toSql(self):
            sql = '%s %s %%s' % (self.column.name, self.operand )
            value = self.column.toSql( self.value )
            return (sql, value)
        
        def encode(self):
            return "w=" + c.urllib_quote("%s,%s,%s" % (self.column.name, self.operand, self.value), ',:=' )

        def decode(self, obj, value):
            column, self.operand, self.value = value.split(',')
            self.column = obj._columns[column]

    class _Group:
        def __init__(self):
            pass
        
        def toSql(self):
            return None
        
        def encode(self):
            return None

        def decode(self, obj, value):
            return None

    class _Order:
        def __init__(self, column=None, desc=False):
            self.column = column
            self.desc = desc
        
        def toSql(self):
            sql = '%s' % (self.column.name)
            if self.desc:
                sql += ' DESC'
            return sql
        
        def encode(self):
            return "o=" + c.urllib_quote("%s,%s" % (self.column.name, self.desc ))
        
        def decode(self, obj, value):
            tmp = value.split(',')
            if len(tmp) < 1 or len(tmp) > 2:
                return
            column = tmp[0]
            self.column = obj._columns[column]
            if len(tmp) == 2:
                self.desc = tmp[1] == 'True'

    class _Limit:
        def __init__(self, offset=None, rowcount=None):
            self.offset = offset
            self.rowcount = rowcount

        def toSql(self):
            offset = 0
            if self.offset != None:
                offset = self.offset
            return ' LIMIT %i OFFSET %i' % ( self.rowcount, offset)
        
        def encode(self):
            return "l=" + c.urllib_quote("%s,%s" % (self.offset, self.rowcount))
        
        def decode(self, value):
            tmp = value.split(',')
            if len(tmp) != 2:
                return
            if tmp[0] != "None":
                self.offset = int(tmp[0])
            else:
                self.offset = None
            if tmp[1] != "None":
                self.rowcount = int(tmp[1])
            else:
                self.rowcount = None

    def filter(self, column, operand, value):
        """Add a filter. Returns self so it can be chained"""
        if not isinstance(column, basium_model.Column):
            self.log.error('Query.filter() called with a non-Column %s' % column)
            return None
        if self._model == None:
            self._model = column._model
            self._table = column._model._table
        elif self._table != column._model._table:
            self.log.error('Filter from multiple tables not implemented')
            return None
        self._where.append( self._Where(column=column, operand=operand, value=value) )
        return self
    
    def group(self):
        """Add a group. Returns self so it can be chained"""
        return self

    def order(self, column, desc = False):
        """Add a sort order. Returns self so it can be chained"""
        if not isinstance(column, basium_model.Column):
            self.log.error('Query.order() called with a non-Column %s' % column)
            return None
        if self._model == None:
            self._model = column._model
            self._table = column._model._table
        elif self._table != column._model._table:
            self.log.error('Order from multiple tables not implemented')
            return None
        self._order.append( self._Order(column=column, desc=desc) )
        return self

    def limit(self, offset=None, rowcount=None):
        """
        Offset and maximum number of rows that should be returned
        Offset is optional, maximum number of rows is mandatory
        Can be called once, if multiple calls last one is the one being used
        """
        self._limit = self._Limit(offset, rowcount)
        return self
        
    def toSql(self):
        """
        Return the query as SQL
        Handles
        - WHERE
        - GROUP BY (todo)
        - ORDER BY
        - LIMIT
        """
        value = []
        sql = ''
        if len(self._where) > 0:
            sql += ' where ('
            addComma = False
            if self._where != None:
                for where in self._where:
                    if addComma:
                        sql += ' and '
                    else:
                        addComma = True
                    sql2, value2 = where.toSql()
                    sql += sql2
                    value.append(value2)
                sql += ')'

        if len(self._order) > 0:
            sql += " ORDER BY "
            addComma = False
            for order in self._order:
                if addComma:
                    sql += ','
                else:
                    addComma = True
                sql += order.toSql()

        if self._limit != None:
            sql += self._limit.toSql()

        return (sql, value)

    def encode(self):
        """Return the query as a string that can be appended to an URI"""
        url = []
        
        # where
        for where in self._where:
            url.append(where.encode() )

        # group
        
        # order
        for order in self._order:
            url.append(order.encode() )
        
        # limit
        if self._limit:
            url.append(self._limit.encode())
        
        return "&".join(url)

    def decode(self, url):
        """Decode an URL query and update this query object"""
        u = c.urllib_parse_qsl(url)
        self._reset()
        for (key, val) in u:
            if key == 'w':
                w = self._Where()
                w.decode(self._model, val)
                self._where.append(w)
            elif key == 'g':
                g = self._Group()
                g.decode(val)
                self._group.append(self._model, g)
            elif key == 'o':
                o = self._Order()
                o.decode(self._model, val)
                self._order.append(o)
            elif key == 'l':
                l = self._Limit()
                l.decode(val)
                self._limit = l
            else:
                self.log.error("Incorrect key=%s, url='%s' in URL" % (key, url))
