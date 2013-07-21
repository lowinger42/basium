#!/usr/bin/env python

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

"""
A basic HTTP REST API for the Basium registered classes

If you want to use this, symlink to your web server documentroot
"""

from __future__ import print_function
from __future__ import unicode_literals
__metaclass__ = type

import json

import basium
import basium_compatibilty as c
import basium_model
import basium_driver
import basium_driver_json

log = basium.log

class API():
    def __init__(self, request, response, basium):
        self.request = request
        self.response = response
        self.basium = basium
        self.write = response.write # convenience

    def getData(self, obj):
        postdata = c.urllib_parse_qs(self.request.body)
        for key in postdata.keys():
            if key in obj._columns:
                column = obj._columns[key]
                data = postdata[key][0]
                if isinstance(column, basium_model.BooleanCol):
                    data = basium_driver_json.BooleanCol.toPython(data)
                elif isinstance(column, basium_model.DateCol):
                    data = basium_driver_json.DateCol.toPython(data)
                elif isinstance(column, basium_model.DateTimeCol):
                    data = basium_driver_json.DateTimeCol.toPython(data)
                elif isinstance(column, basium_model.DecimalCol):
                    data = basium_driver_json.DecimalCol.toPython(data)
                elif isinstance(column, basium_model.FloatCol):
                    data = basium_driver_json.FloatCol.toPython(data)
                elif isinstance(column, basium_model.IntegerCol):
                    data = basium_driver_json.IntegerCol.toPython(data)
                elif isinstance(column, basium_model.VarcharCol):
                    data = basium_driver_json.VarcharCol.toPython(data)
                postdata[key] = column.toSql(data)        # encode to database specific format
            else:
                log.warning("Warning, handlePost got unknown key/column %s" % key)
        return postdata

    def handleGet(self, classname, _id, attr):
        obj = classname()
        if _id == None:
            log.debug('Get all rows in table %s' % obj._table)
            # all rows (put some sane limit here maybe?)
            dbquery = self.basium.query(obj)
        elif _id == 'filter':
            # filter out specific rows
            dbquery = self.basium.query(obj)
            dbquery.decode(self.request.querystr)
            log.debug("Get all rows in table '%s' matching query %s" % (obj._table, dbquery.toSql()))
        else:
            # one row, identified by rowID
            dbquery = self.basium.query().filter(obj.q._id, '=', _id)
            log.debug("Get one row in table '%s' matching query %s" % (obj._table, dbquery.toSql()))
        
        try:
            response = basium.Response()
            response.data = []
            for row in self.basium.driver.select(dbquery):  # we call driver directly for efficiency reason
                tmp = {}
                for colname in obj._iterName():
                    tmp[colname] = row[colname]
                response.data.append(tmp)
        except basium_driver.DriverError as err:
            msg = "Could not load objects from table '%s'. %s %s" % (obj._table, err.errno, err.errmsg)
            log.debug(msg)
            self.write(msg)
            self.response.status = '404 ' + msg
            return
            
        if _id != None and _id != 'filter' and len(response.data) == 0:
            msg = "Unknown ID %s in table '%s'" % (_id, obj._table)
            log.debug(msg)
            response.setError(1, msg)
            self.response.status = '404 ' + msg
            return
        self.write( json.dumps(response.dict(), cls=basium.JsonOrmEncoder) )

    def handlePost(self, classname, _id, attr):
        if _id != None:
            self.response.status = "400 Bad Request, cannot specify ID when inserting a row"
            return
        obj = classname()
        log.debug("Insert one row in table '%s'" % (obj._table))
        postdata = self.getData(obj)
        response = self.basium.driver.insert(obj._table, postdata) # we call driver direct for efficiency reason
        print(json.dumps(response.dict(), cls=basium.JsonOrmEncoder))

    def handlePut(self, classname, _id, attr):
        if _id == None:
            self.response.status = "400 Bad Request, need ID to update a row"
            return
        # update row
        obj = classname()
        log.debug("Update one row in table '%s'" % (obj._table))
        putdata = self.getData(obj)
        putdata['_id'] = _id
        response = self.basium.driver.update(obj._table, putdata) # we call driver direct for efficiency reason
        print(json.dumps(response.dict(), cls=basium.JsonOrmEncoder))

    def handleDelete(self, classname, _id, attr):
        obj = classname()
        if _id == None:
            msg = "Refusing to delete all rows in table '%s" % obj._table
            log.debug(msg)
            self.response.status = "400 %s" % msg
            return
        elif _id == 'filter':
            # filter out specific rows
            dbquery = self.basium.query(obj)
            dbquery.decode(self.request.querystr)
            log.debug("Delete all rows in table '%s' matching query %s" % (obj._table, dbquery.toSql()))
        else:
            # one row, identified by rowID
            dbquery = self.basium.query().filter(obj.q._id, '=', _id)
            log.debug("Delete one row in table '%s' matching id %s" % (obj._table, _id))
        response = self.basium.driver.delete(dbquery)
        self.write( json.dumps(response.dict(), cls=basium.JsonOrmEncoder) )

    def handleHead(self, classname, _id, attr):
        """
        Count the number of rows matching a query
        Return data in a HTML header
        """
        obj = classname()
        if _id == None:
            log.debug('Count all rows in table %s' % obj._table)
            # all rows (put some sane limit here maybe?)
            dbquery = self.basium.query(obj)
        elif _id == 'filter':
            # filter out specific rows
            dbquery = self.basium.query()
            dbquery.decode(self.request.querystr)
            log.debug("Count all rows in table '%s' matching query %s" % (obj._table, dbquery.toSql()))

        response = self.basium.driver.count(dbquery)  # we call driver direct for efficiency reason
        if response.isError():
            msg = "Could not count objects in table '%s'. %s" % (obj._table, response.getError())
            log.debug(msg)
            self.status = '404 ' + msg
            return
        self.response.addHeader('X-Result-Count', str(response.data))

    
    def handleAPI(self):
        attr = self.request.attr
        ix = 0
        if not attr[ix] in self.basium.cls:
            self.response.status = "404 table '%s' not found" % (attr[ix])
            return
        classname = self.basium.cls[attr[ix]]
        ix += 1
        if len(attr) > ix:
            _id = attr[ix]
            ix += 1
        else:
            _id = None
        if self.request.method == 'GET':
            self.handleGet(classname, _id, attr[ix:])
        elif self.request.method == "POST":
            self.handlePost(classname, _id, attr[ix:])
        elif self.request.method == "PUT":
            self.handlePut(classname, _id, attr[ix:])
        elif self.request.method == "DELETE":
            self.handleDelete(classname, _id, attr[ix:])
        elif self.request.method == "HEAD":
            self.handleHead(classname, _id, attr[ix:])
        else:
            # not a request we understand
            self.response.status = "400 Unknown request %s" % self.request.method

def run(request, response, basium):
    api = API(request, response, basium)
    api.handleAPI()
