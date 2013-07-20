#! /usr/bin/env python

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

"""
Basium database driver that handles remote JSON server

the toSql/toPython methods are different compared to a standard SQL driver
toSQL, converts data so it can be sent from client->server
toPython, convert in the server from wire format->python format

api.py is the code running on the server, that handles all requests from
this driver
"""

from __future__ import print_function
from __future__ import unicode_literals
__metaclass__ = type

import datetime
import decimal

import basium
import basium_driver

log = basium.log

#
# These are shadow classes from the basium_model
# handles the database specific functions such
# as converting to/from SQL types
#

class BooleanCol(basium_driver.BooleanCol):

    @classmethod
    def toPython(self, value):
        if basium.isstring(value):
            return value.lower() == "true"
        return value

    def toSql(self, value):
        if value == None:
            return "NULL"
        if value:
            return 'True'
        return 'False'
 
# stores a date
class DateCol(basium_driver.DateCol):
    
    @classmethod
    def toPython(self, value):
        if isinstance(value, datetime.datetime):
            value = value.date()
        if basium.isstring(value):
            value = datetime.datetime.strptime(value[:10], '%Y-%m-%d').date()
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return value

# stores date+time
# ignores microseconds
# if default is 'NOW' the current date+time is stored
class DateTimeCol(basium_driver.DateTimeCol):
    
    def getDefault(self):
        if self.default == 'NOW':
            return datetime.datetime.now().replace(microsecond=0)
        return self.default

    @classmethod
    def toPython(self, value):
        if basium.isstring(value):
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        return value

# stores a fixed precision number
class DecimalCol(basium_driver.DecimalCol):
    
    def typeToSql(self):
        sql = 'decimal(%d,%d)' % (self.maxdigits, self.decimal)
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            sql += " default '%s'" % str(self.default)
        return sql

    @classmethod
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
class FloatCol(basium_driver.FloatCol):
    
    @classmethod
    def toPython(self, value):
        if basium.isstring(value):
            value = float(value)
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return str(value)

# stores an integer
class IntegerCol(basium_driver.IntegerCol):
    
    @classmethod
    def toPython(self, value):
        if basium.isstring(value):
            value = int(value)
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return value

# stores a string
class VarcharCol(basium_driver.VarcharCol):

    @classmethod
    def toPython(self, value):
        if basium.isstring(value):
            value = str(value)
        return value


class Driver(basium_driver.Driver):
    def __init__(self, dbconf=None,):
        self.dbconf = dbconf
        
        self.uri = '%s/api' % (self.dbconf.host)

    def connect(self):
        """
        dummy, json api is stateless, we don't need connect
        
        todo, could potentially check if server is reachable
        """
        pass

    def execute(self, method=None, url=None, data=None, decode=False):
        if self.dbconf.debugSQL:
            log.debug('Method=%s URL=%s Data=%s' % (method, url, data))
        response = basium.urllib_request_urlopen(url, method, 
                                                 username=self.dbconf.username,
                                                 password=self.dbconf.password,
                                                 data=data, decode=decode)
        return response

#     def isDatabase(self, dbName):
#         """
#         Check if a database exist
#         todo: add server API for this
#         """
#         response = basium.Response()
#         response.data = True
#         return response

#     def isTable(self, tableName):
#         """
#         Check if a table exist
#         Todo: add server API for this
#         """
#         response = basium.Response()
#         response.data = True
#         return response

#     def createTable(self, obj):
#         """This is not valid for JSON API due to security issues"""
#         response = basium.Response()
#         return response

#     def verifyTable(self, obj):
#         """
#         Verify that a table is equal the objects attributes and types
#         Todo: This should be possible to check over http
#            One idea is to calulate a checksum on all columns and their
#            attributes, and then compare client and server checksum
#         """
#         response = basium.Response()
#         response.data = []
#         return response
    
#     def modifyTable(self, obj, actions):
#         """
#         Modify a database table so it corresponds to a object attributes and types
#         Not valid for JSON, due to security issues
#         """
#         return True
    
    def count(self, query):
        """Count the number of objects, filtered by query"""
        log.debug("Count query from database, using HTTP API")
        if len(query._where) == 0:
            url = '%s/%s' %(self.uri, query._model._table )
        else:
            url = '%s/%s/filter?%s' %(self.uri, query._model._table, query.encode() )
        response = self.execute(method='HEAD', url=url)
        if not response.isError():
            rows = response.info().get('X-Result-Count')
            response.data = rows
        return response
    
    def select(self, query):
        """
        Get data from a table
        
        two different formats:
          simple: <url>/<table>/<id>
          query : <url>/<table>/filter?column=oper,value[&column=oper,value]
        """
        log.debug("Load query from database, using HTTP API")

        if query.isId():
            # simple
            url = '%s/%s/%i' % (self.uri, query._model._table, query._where[0].value)
        else:
            # real query 
            url = '%s/%s/filter?%s' %(self.uri, query._model._table, query.encode() )
        response = self.execute(method='GET', url=url, decode=True)
        return response

    def insert(self, table, values):
        log.debug("Store obj in database, using HTTP API")
        url = '%s/%s' % (self.uri, table)
        response = self.execute(method='POST', url=url, data=values, decode=True)
        return response

    def update(self, table, values):
        log.debug("Update obj in database, using HTTP API")
        url = '%s/%s/%i' % (self.uri, table, values['_id'])
        response = self.execute(method='PUT', url=url, data=values, decode=True)
        return response

    def delete(self, query):
        log.debug("Delete obj from database, using HTTP API")
        if query.isId():
            # simple
            url = '%s/%s/%i' % (self.uri, query._model._table, query._where[0].value)
        else:
            # real query 
            url = '%s/%s/filter?%s' %(self.uri, query._model._table, query.encode() )
        response = self.execute('DELETE', url, decode = True)
        return response
