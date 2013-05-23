#! /usr/bin/env python

# ----------------------------------------------------------------------------
#
# Basium database driver that handles remote JSON server
#
# the toSql/toPython methods are different compared to a standard SQL driver
#   toSQL, converts data so it can be sent from client->server
#   toPython, convert in the server from wire format->python format
# ----------------------------------------------------------------------------

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

import datetime
import urllib
import urllib2
import json
import decimal
import httplib
import base64

import basium_common
import basium_driver
from basium_model import *

log = basium_common.log

#
# These are shadow classes from the basium_model
# handles the database specific functions such
# as converting to/from SQL types
#

class BooleanCol(basium_driver.BooleanCol):

    @classmethod
    def toPython(self, value):
        return value == "True" or value == "1"

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
class DateTimeCol(basium_driver.DateTimeCol):
    
    def getDefault(self):
        if self.default == 'NOW':
            return datetime.datetime.now().replace(microsecond=0)
        return self.default

    @classmethod
    def toPython(self, value):
        if isinstance(value, basestring):
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
        if isinstance(value, basestring):
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
        if isinstance(value, basestring):
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
        if isinstance(value, unicode):
            value = str(value)
        return value

#
# Helper class, to implement all four HTTP methods
#   GET, POST, PUT, DELETE
#
class RequestWithMethod(urllib2.Request):
    def __init__(self, *args, **kwargs):
        self._method = kwargs.pop('method', None)
        urllib2.Request.__init__(self, *args, **kwargs)

    def get_method(self):
        return self._method if self._method else super(RequestWithMethod, self).get_method()


# ----------------------------------------------------------------------------
#
# Database driver that uses JSON to talk to the database through
# a remote server
#
# Due to mainly security issues, the JSON driver cannot create/change
# tables on the server
#
# ----------------------------------------------------------------------------
class Driver(basium_driver.Driver):
    def __init__(self, host=None, port=None, username=None, password=None, name=None, debugSql=False):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.name = name
        self.debugSql = debugSql
        
        self.uri = '%s/api' % (self.host)


    #
    # dummy, json api is stateless, we don't need connect
    # todo, could potentially check if server is reachable
    #
    def connect(self):
        pass


    def execute(self, method=None, url=None, data=None, decode=False):
        if self.debugSql:
            log.debug('Method=%s URL=%s Data=%s' % (method, url, data))
        response = basium_common.Response()
        req = RequestWithMethod(url, method=method)
        if self.username != None:
            base64string = base64.standard_b64encode('%s:%s' % (self.username, self.password))
            req.add_header("Authorization", "Basic %s" % base64string)
        try:
            if data:
                o = urllib2.urlopen(req, urllib.urlencode(data))
            else:
                o = urllib2.urlopen(req)
            response.set('info', o.info)
        except urllib2.HTTPError, e:
            response.setError(1, "HTTPerror %s" % e)
            return response
        except urllib2.URLError, e:
            response.setError(1, "URLerror %s" % e)
            return response
        except httplib.HTTPException, e:
            response.setError(1, 'HTTPException %s' % e)
            return response

        if decode:
            try:
                tmp = o.read()
                # print "From server: '%s'" % tmp
                res = json.loads(tmp)
                o.close()
            except ValueError:
                response.setError(1, "JSON ValueError for " + tmp)
                return response
            except TypeError:
                response.setError(1, "JSON TypeError for " + tmp)
                return response
            try:
                if res['errno'] == 0:
                    response.set('data', res['data'])
                else:
                    response.setError(res['errno'], res['errmsg'])
            except KeyError:
                response.setError(1, "Result keyerror")

        return response


    #
    # Check if a database exist
    # There is no API for this, and the server will not start/reply
    # if the database is not there so we cheat, always respond with: Yes, DB is there
    #
#    def isDatabase(self, dbName):
#        response = basium_common.Response()
#        response.set('data', True)
#        return response


    #
    # Check if a table exist
    # Todo: This should be possible to check over http
    #
#    def isTable(self, tableName):
#        response = basium_common.Response()
#        response.set('data', True)
#        return response


    #
    # Create table, this is not valid for JSON API
    # Todo: This should be possible to check over http
    #
#    def createTable(self, obj):
#        response = basium_common.Response()
#        return response


    #
    # Verify that a table is equal the objects attributes and types
    # Todo: This should be possible to check over http
    #    One idea is to calulate a checksum on all columns and their
    #    attributes, and then compare client and server checksum
    #    
#    def verifyTable(self, obj):
#        response = basium_common.Response()
#        response.set('actions', [])
#        return response
    
    #
    # Modify a database table so it corresponds to a object attributes and types
    # Not valid for JSON, due to security
    #    
#    def modifyTable(self, obj, actions):
#        return True

    
    #
    # Count the number of objects, filtered by query
    #    
    def count(self, query):
        log.debug("Count query from database, using HTTP API")
        if len(query._where) == 0:
            url = '%s/%s' %(self.uri, query._model._table )
        else:
            url = '%s/%s/filter?%s' %(self.uri, query._model._table, query.encode() )
        response = self.execute(method='HEAD', url=url)
        if not response.isError():
            info = response.get('info')
            rows = info().getheader('X-Result-Count')
            response.set('data', rows)
        return response
    
    #
    # two different formats:
    # simple: <url>/<table>/<id>
    # query : <url>/<table>/filter?column=oper,value[&column=oper,value]
    #
    def select(self, query):
        log.debug("Load query from database, using HTTP API")

        if query.isId():
            # simple
            url = '%s/%s/%i' % (self.uri, query._model._table, query._where[0].value)
        else:
            # real query 
            url = '%s/%s/filter?%s' %(self.uri, query._model._table, query.encode() )
        response = self.execute(method='GET', url=url, decode=True)
        return response
#        o = urllib2.urlopen(url)
#        data = json.load(o)
#        if not response.isError():
#            rows = []
#            for row in response.get('data'):
#                resp = {}
#                for colname in row:
#                    resp[colname] = row[colname]
#                rows.append(resp)
#            response.set('data', rows)
#        else:
#            response.setError(data['errno'], data['errmsg'])
#        return response

    #
    #
    #    
    def insert(self, table, values):
        log.debug("Store obj in database, using HTTP API")
        url = '%s/%s' % (self.uri, table)
        # print "basium_driver_json::insert() values =", values
        response = self.execute(method='POST', url=url, data=values, decode=True)
        return response

    #
    #
    #
    def update(self, table, values):
        log.debug("Update obj in database, using HTTP API")
        url = '%s/%s/%i' % (self.uri, table, values['id'])
        response = self.execute(method='PUT', url=url, data=values, decode=True)
        return response
    
#         req = RequestWithMethod(url, method='PUT')
#         o = urllib2.urlopen(req, urllib.urlencode(values))
#         res = json.load(o)
#         response.setError(res['errno'], res['errmsg'])

    #
    #
    #
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
#        if not result.isError():
#        if data['errno'] == 0:
#            rows = []
#            for row in data['data']:
#                resp = {}
#                for colname in row:
#                    resp[colname] = row[colname]
#                rows.append(resp)
#            response.set('data', rows)
#        else:
#            response.setError(data['errno'], data['errmsg'])
