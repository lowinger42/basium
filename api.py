#!/usr/bin/env python

# ----------------------------------------------------------------------------
#
# Page for the Basium WSGI handler
# Implements a basic HTTP REST API for the Basium registered classes
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

import json
import urlparse

import basium_orm
import basium_common

log = basium_common.log

class API():
    def __init__(self, request, response, basium):
        self.request = request
        self.response = response
        self.basium = basium
        self.db = basium.db
        self.write = response.write # convenience
    
    def handleGet(self, classname, id_, attr):
        obj = classname()
        if id_ == None:
            log.debug('Get all rows in table %s' % obj._table)
            # all rows (put some sane limit here maybe?)
            dbquery = basium_orm.Query(self.db, obj)
        elif id_ == 'filter':
            # filter out specific rows
            dbquery = basium_orm.Query(self.db, obj)
            dbquery.decode(self.request.querystr)
            log.debug("Get all rows in table '%s' matching query %s" % (obj._table, dbquery.toSql()))
        else:
            # one row, identified by rowID
            dbquery = basium_orm.Query(self.db).filter(obj.q.id, '=', id_)
            log.debug("Get one row in table '%s' matching query %s" % (obj._table, dbquery.toSql()))

        response = self.db.driver.select(dbquery)  # we call driver directly for efficiency reason
        if response.isError():
            msg = "Could not load objects from table '%s'. %s" % (obj._table, response.getError())
            log.debug(msg)
            self.write(msg)
            self.response.status = '404 ' + msg
            return
        lst = response.get('data')
        if id_ != None and id_ != 'filter' and len(lst) == 0:
            msg = "Unknown ID %s in table '%s'" % (id_, obj._table)
            response.setError(1, msg)
            log.debug(msg)
            self.response.status = '404 ' + msg
        self.write( json.dumps(response, cls=basium_common.JsonOrmEncoder) )

    #
    #
    #    
    def handlePost(self, classname, id_, attr):
        obj = classname()
        if id_ != None:
            self.response.status = "400 Bad Request, no ID needed to insert a row"
            return
        postdata = urlparse.parse_qs(self.request.body)
        for key in postdata.keys():
            postdata[key] = postdata[key][0]
        # print "postdata =", postdata
        response = self.db.driver.insert(obj._table, postdata) # we call driver direct for efficiency reason
        print json.dumps(response.get(), cls=basium_common.JsonOrmEncoder)

    #
    #
    #    
    def handlePut(self, classname, id_, attr):
        if id_ == None:
            self.response.status = "400 Bad Request, need ID to update a row"
            return
        # update row
        obj = classname()
        putdata = urlparse.parse_qs(self.request.body)
        for key in putdata.keys():
            putdata[key] = putdata[key][0]
        putdata['id'] = id_
        response = self.db.driver.update(obj._table, putdata) # we call driver direct for efficiency reason
        print json.dumps(response.get(), cls=basium_common.JsonOrmEncoder)

    #
    #
    #    
    def handleDelete(self, classname, id_, attr):
        obj = classname()
        if id_ == None:
            msg = "Refusing to delete all rows in table '%s" % obj._table
            log.debug(msg)
            self.response.status = "400 %s" % msg
            return
        elif id_ == 'filter':
            # filter out specific rows
            dbquery = basium_orm.Query(self.db, obj)
            dbquery.decode(self.request.querystr)
            log.debug("Delete all rows in table '%s' matching query %s" % (obj._table, dbquery.toSql()))
        else:
            # one row, identified by rowID
            dbquery = basium_orm.Query(self.db).filter(obj.q.id, '=', id_)
            log.debug("Delete one row in table '%s' matching id %s" % (obj._table, id_))
        response = self.db.driver.delete(dbquery)
        self.write( json.dumps(response, cls=basium_common.JsonOrmEncoder) )

    #
    # Count the number of rows matching a query
    # Return data in a HTML header
    #
    def handleHead(self, classname, id_, attr):
        obj = classname()
        if id_ == None:
            log.debug('Count all rows in table %s' % obj._table)
            # all rows (put some sane limit here maybe?)
            dbquery = basium_orm.Query(self.db, obj)
        elif id_ == 'filter':
            # filter out specific rows
            dbquery = basium_orm.Query(self.db, obj)
            dbquery.decode(self.request.querystr)
            log.debug("Count all rows in table '%s' matching query %s" % (obj._table, dbquery.toSql()))

        response = self.db.driver.count(dbquery)  # we call driver direct for efficiency reason
        if response.isError():
            msg = "Could not count objects in table '%s'. %s" % (obj._table, response.getError())
            log.debug(msg)
            self.status = '404 ' + msg
            return
        self.response.addHeader('X-Result-Count', str(response.get('data') ))

    
    #
    #
    #    
    def handleAPI(self):
        attr = self.request.attr
        ix = 0
        if not attr[ix] in self.basium.cls:
            self.response.status = "404 table '%s' not found" % (attr[ix])
            return
        classname = self.basium.cls[attr[ix]]
        ix += 1
        if len(attr) > ix:
            id_ = attr[ix]
            ix += 1
        else:
            id_ = None
        if self.request.method == 'GET':
            self.handleGet(classname, id_, attr[ix:])
        elif self.request.method == "POST":
            self.handlePost(classname, id_, attr[ix:])
        elif self.request.method == "PUT":
            self.handlePut(classname, id_, attr[ix:])
        elif self.request.method == "DELETE":
            self.handleDelete(classname, id_, attr[ix:])
        elif self.request.method == "HEAD":
            self.handleHead(classname, id_, attr[ix:])
        else:
            # not a request we understand
            self.response.status = "400 Unknown request %s" % self.request.method

def run(request, response, basium):
    api = API(request, response, basium)
    api.handleAPI()
