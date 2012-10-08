#! /usr/bin/env python

#
# Object persistence for Python. The database or json interface is accessed
# through a driver, which makes it easy to implement new databases.
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
import datetime
import urllib2
import urlparse
import json
import types
import decimal

import basium_common
from basium_model import *

log = basium_common.log


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
class Driver:
    def __init__(self, host=None, port=None, username=None, password=None, name=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.name = name
        
        self.uri = 'http://%s:%s/api' % (self.host, self.port)

    #
    # dummy, json api is stateless, we don't need connect
    #
    def connect(self):
        pass


    #
    # Check if a database exist
    #
    def isDatabase(self, dbName):
        response = basium_common.Response()
        response.set('data', True)
        return response


    #
    # Check if a table exist
    #
    def isTable(self, tableName):
        response = basium_common.Response()
        response.set('data', True)
        return response


    #
    # Create table, this is not valid for JSON API
    #
    def createTable(self, obj):
        response = basium_common.Response()
        return response


    #
    # Verify that a table is equal the objects attributes and types
    #    
    def verifyTable(self, obj):
        response = basium_common.Response()
        response.set('actions', [])
        return response
    
    #
    # Modify a database table so it corresponds to a object attributes and types
    # Not valid for JSON, due to security
    #    
    def modifyTable(self, obj, actions):
        return True

    
    #
    #
    #    
    def count(self, query=None):
        log.debug("Count query from database, using HTTP API")
        response = basium_common.Response()
        tmp = query.encode()
        if tmp == '':
            url = '%s/%s' %(self.uri, query._cls._table )
        else:
            url = '%s/%s/filter?%s' %(self.uri, query._cls._table, query.encode() )
        
        req = RequestWithMethod(url, method='HEAD')
        
        o = urllib2.urlopen(req)
        rows = o.info().getheader('X-Result-Count')
    
        response.set('data', rows)
        return response
    
    #
    # two different formats:
    # simple: <url>/<table>/<id>
    # query : <url>/<table>/filter?column=oper,value[&column=oper,value]
    #
    def select(self, query):
        log.debug("Load query from database, using HTTP API")
        response = basium_common.Response()

        if query.isId():
            # simple
            url = '%s/%s/%i' % (self.uri, query._cls._table, query._where[0].value)
        else:
            # real query 
            url = '%s/%s/filter?%s' %(self.uri, query._cls._table, query.encode() )
        # print "url =", url
        o = urllib2.urlopen(url)
        data = json.load(o)
        if data['_errno'] == 0:
            rows = []
            for row in data['data']:
                resp = {}
                for colname in row:
                    resp[colname] = row[colname]
                rows.append(resp)
            response.set('data', rows)
        else:
            response.setError(data['_errno'], data['_errmsg'])
        return response


    #
    #
    #    
    def insert(self, table, values):
        log.debug("Store obj in database, using HTTP API")
        response = basium_common.Response()
        response.set('data', 112)

        url = '%s/%s' % (self.uri, table)                # insert
        jdata = json.dumps(values, cls=basium_common.JsonOrmEncoder)
        o = urllib2.urlopen(url, jdata)                 # POST
        res = json.load(o)
        if res['_errno'] == 0:
            response.set('data', res['data'])
        else:
            response.setError(res['_errno'], res['_errmsg'])
        return response


    #
    #
    #
    def update(self, table, values, id_):
        log.debug("Update obj in database, using HTTP API")
        response = basium_common.Response()
        return response

    #
    #
    #
    def delete(self, table, id_):
        log.debug("Delete obj from database, using HTTP API")
        response = basium_common.Response()
        return response


#
# Main
#
if __name__ == "__main__":
    pass
