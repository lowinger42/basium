#!/usr/bin/env python

# -----------------------------------------------------------------------------
#
# Object persistence for Python
#
# This class handles all mapping between objects and dictionaries,
# before calling database driver, or returning objects
#
# -----------------------------------------------------------------------------

#
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

__metaclass__ = type

import sys
import inspect
import urlparse
import urllib

import basium_common
import basium_model
import basium_driver

log = basium_common.log

# to make less errors in queries
LT = '<'
LE = '<='
EQ = '='
GT = '>'
GE = '>='
NE = '!='


class BasiumOrm:

    def __init__(self, driver = None, drivermodule = None):
        self.driver = driver
        self.drivermodule = drivermodule

        # mixin, let the various model classes also inherit from the
        # corresponding driver classes. We change the class so all
        # future instances will have correct base classes
        #
        # todo: is there a better way to do this?
        #       it would be nice to have the model instance decoupled from the driver
        #
        drvclasses = {}
        for tmp in inspect.getmembers(self.drivermodule, inspect.isclass):
            drvclasses[tmp[0]] = tmp[1]
        for modelclsname, modelcls in inspect.getmembers(basium_model, inspect.isclass):
            if issubclass(modelcls, basium_model.Column) and modelclsname != 'Column':
                # ok, found one, get the drivers corresponding class
                if modelclsname in drvclasses:
#                    print "found %s!" % modelclsname
#                    print "  ", modelcls.__bases__
                    modelcls.__bases__ = modelcls.__bases__  + (drvclasses[modelclsname],)
#                    print "  ", modelcls.__bases__
                else:
                    log.error('Driver %s is missing Class %s' % (self.drivermodule.__name__, modelclsname))

    #
    # Returns
    #    True if the database exist
    #    False if the database does not exist
    #    None  if there was an error
    #
    def isDatabase(self, dbName):
        result = self.driver.isDatabase(dbName)
        if result.isError():
            log.error("Check if Database '%s' exist: %s" % (dbName, result.getError()) )
            return None
        exist = result.get('data')
        if exist:
            log.debug("SQL Database '%s' exist" % dbName)
        else:
            log.error("SQL Database '%s' does NOT exist, it needs to be created" % dbName)
        return exist

    #
    # Check if a table exist in the database
    #
    def isTable(self, obj):
        result = self.driver.isTable(obj._table)
        if result == None:
            log.error("Check if Table '%s' exist: %s" % (obj._table, result.getError()))
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
            log.error("Create SQL Table '%s' failed. %s" % (obj._table, result.getError()))
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
    # actions is the result from verifyTtable
    #
    def modifyTable(self, obj, actions):
        result = self.driver.modifyTable(obj, actions)
        if result.isError():
            log.error('Fatal: Cannot update table structure for %s. %s' % (obj._table, result.getError()))
            return False
        return True

    #
    #
    #           
    def count(self, query_):
        if isinstance(query_, basium_model.Model):
            query = Query(self, query_)
        elif isinstance(query_, Query):
            query = query_
        else:
            log.error("Fatal: incorrect object type in count")
            return None
        result = self.driver.count(query)
        if result.isError():
            log.error('Cannot do count(*) on %s' % (query._model._table))
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
    # Note: querying for a single object with id returns error if not found
    #
    def load(self, query_):
        response = basium_common.Response()
        one = False
        if isinstance(query_, basium_model.Model):
            query = Query(self).filter(query_.q.id, EQ, query_.id)
            one = True
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
            newobj = query._model.__class__()
            for (colname, column) in newobj._columns.items():
                newobj._values[colname] = column.toPython( row[colname] )
            rows.append(newobj)
        response.set('data', rows)
        if one and len(rows) < 1:
            response.setError(1, "Unknown UD %s in table %s" % (query_.id, query_._table))
        return response

    
    #
    # Store the query in the database
    # If the objects ID is set, we update the current row in the table,
    # otherwise we create a new row
    #
    def store(self, obj):
        columns = {}
        for (colname, column) in obj._columns.items():
            columns[colname] = column.toSql(obj._values[colname])

        if obj.id >= 0:
            # update
            response = self.driver.update(obj._table, columns)
        else:
            # insert
            response = self.driver.insert(obj._table, columns)
            if not response.isError():
                obj.id = response.get('data')
        return response
    
    #
    # Delete objects in the table.
    #   query_ can be either
    #     An instance of Model
    #     Query()
    #
    #   If instance of model, that instance will be deleted
    #   If query, the objects matching the query is deleted 
    #
    def delete(self, query_):
        response = basium_common.Response()
        clearID = False
        if isinstance(query_, basium_model.Model):
            query = Query(self).filter(query_.q.id, EQ, query_.id)
            clearID = True
        elif isinstance(query_, Query):
            query = query_
        else:
            response.setError(1, "Fatal: incorrect object type in delete()")
            return response
        response = self.driver.delete(query)
        if not response.isError() and clearID:
            query_.id = -1
        return response

    #
    # Create and return a query object
    # Convenience method, makes it unnecessary to import the basium_orm module
    # just for doing queries
    #
    def query(self, obj = None):
        q = Query(self, obj)
        return q


# ----------------------------------------------------------------------------
#
# Class that build queries
#
# ----------------------------------------------------------------------------

class Query():

    #
    def __init__(self, db, model = None):
        self._db = db
        self._model = model
        self.reset()

    #
    def reset(self):
        self._where = []
        self._group = []
        self._order = []
        self._limit = None

    def isId(self):
        if len(self._where) != 1:
            return False
        w = self._where[0]
        return w.column.name == 'id' and w.operand == '='

    def getTable(self):
        if self._model != None:
            return self._model._table
        log.error('Fatal: No table name')
        sys.exit(1)

    #
    class Where:
        def __init__(self, column = None, operand = None, value = None):
            self.column = column
            self.operand = operand
            self.value = value

        def toSql(self):
            sql = '%s %s %%s' % (self.column.name, self.operand )
            value = self.column.toSql( self.value )
            return (sql, value)
        
        def encode(self):
            return "w=" + urllib.quote("%s,%s,%s" % (self.column.name, self.operand, self.value), ',:=' )

        def decode(self, obj, value):
            column, self.operand, self.value = value.split(',')
            self.column = obj._columns[column]

    #
    class Group:
        def __init__(self):
            pass
        
        def toSql(self):
            return None
        
        def encode(self):
            return None

        def decode(self, obj, value):
            return None

    #        
    class Order:
        def __init__(self, column=None, desc=False):
            self.column = column
            self.desc = desc
        
        def toSql(self):
            sql = '%s' % (self.column.name)
            if self.desc:
                sql += ' DESC'
            return sql
        
        def encode(self):
            return "o=" + urllib.quote("%s,%s" % (self.column.name, self.desc ))
        
        def decode(self, obj, value):
            tmp = value.split(',')
            if len(tmp) < 1 or len(tmp) > 2:
                return
            column = tmp[0]
            self.column = obj._columns[column]
            if len(tmp) == 2:
                self.desc = tmp[1] == 'True'


    #
    class Limit:
        def __init__(self, offset=None, rowcount=None):
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
    # Add a filter. Returns self so it can be chained
    #
    def filter(self, column, operand, value):
        if not isinstance(column, basium_model.Column):
            log.error('Fatal: filter() called with a non-Column %s' % column)
            return None
        if self._model == None:
            self._model = column._model
        elif self._model != column._model:
            log.error('Fatal: filter from multiple tables not implemented')
            sys.exit(1)
        self._where.append( self.Where(column=column, operand=operand, value=value) )
        return self

    #
    # Add a group. Returns self so it can be chained
    #
    def group(self):
        return self


    #
    # Add a sort order. Returns self so it can be chained
    #
    def order(self, column, desc = False):
        if not isinstance(column, basium_model.Column):
            log.error('Fatal: order() called with a non-Column %s' % column)
            return None
        if self._model == None:
            self._model = column._model
        elif self._model != column._model:
            log.error('Fatal: filter from multiple tables not implemented')
            return None
        self._order.append( self.Order(column=column, desc=desc) )
        return self

    #
    # Offset and maximum number of rows that should be returned
    # Offset is optional
    # Maximum number of rows is mandatory
    # Can be called once, if multiple calls last one wins
    #
    def limit(self, offset=None, rowcount=None):
        self._limit = self.Limit(offset, rowcount)
        return self
        
    #
    # Return the query as SQL
    # Handles
    # - WHERE
    # - GROUP BY
    # - ORDER BY
    # - LIMIT
    #
    def toSql(self):
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
        for order in self._order:
            url.append(order.encode() )
        
        # limit
        
        return "&".join(url)

    #
    # Decode an URL query and update this query object
    #
    def decode(self, url):
        u = urlparse.parse_qsl(url)
        self.reset()
        for (key, val) in u:
            if key == 'w':
                w = self.Where()
                w.decode(self._model, val)
                self._where.append(w)
            elif key == 'g':
                g = self.Group()
                g.decode(val)
                self._group.append(g)
            elif key == 'o':
                o = self.Order()
                o.decode(val)
                self._order.append(o)
            elif key == 'l':
                l = self.Limit()
                l.decode(val)
                self._limit = l
            else:
                log.error("Incorrect key=%s, url='%s' in URL" % (key, url))
