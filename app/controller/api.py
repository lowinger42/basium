#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

If you want to use this, symlink or copy to your web server documentroot
If you use a symlink, make sure apache follows symlinks
"""

import json
import urllib

import basium_common as bc
import basium_model
# import basium_driver
import basium_driver_json

from wsgi.common import *


# todo, move to wsgi.util or the json driver?
def writejson(resp):
    try:
        tmp = json.dumps(resp.dict(), cls=db.JsonOrmEncoder)
        response.write(tmp)
    except ValueError:
        raise bc.Error(1, "JSON ValueError for " + resp.dict())
    except TypeError:
        raise bc.Error(1, "JSON TypeError for " + resp.dict())


def getclass(table):
    """Return a model object for the table"""
    if table not in db.cls:
        raise WsgiError(404, "table '%s' does not exist" % table)
    return db.cls[table]()


def getData(obj):
    decodeddata = {}
    postdata = request.form()
    for key in obj._columns:
        if key in postdata.keys():
            column = obj._columns[key]
            data = postdata[key]
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
            decodeddata[key] = column.toSql(data)        # encode to database specific format
        else:
            log.warning("Warning, missing key/column %s" % key)
    return decodeddata


@app.route("/_database/<dbname>")
def database(request, response, dbname=None):
    resp = bc.Response()
    try:
        resp.data = db.driver.isDatabase(dbname)
    except db.Error as e:
        resp.errno = e.errno
        resp.errmsg = e.errmsg
    writejson(resp)


@app.route("/_table/<table>")
def _table(request, response, table):
    resp = bc.Response()
    try:
        resp.data = db.driver.isTable(table)
    except db.Error as e:
        resp.errno = e.errno
        resp.errmsg = e.errmsg
    writejson(resp)


@app.route("/<table>/filter/")
def handleGetFilter(request, response, table):
    obj = getclass(table)
    dbquery = db.query(obj)
    dbquery.decode(request.query_string)
    log.debug("Get all rows in table '%s' matching query %s" % (obj._table, dbquery.toSql()))
    
    resp = bc.Response()
    try:
        resp.data = []
        for row in db.driver.select(dbquery):  # we call driver directly for efficiency reason
            tmp = {}
            for colname in obj._iterName():
                tmp[colname] = row[colname]
            resp.data.append(tmp)
    except db.Error as e:
        msg = "Could not load objects from table '%s'. %s" % (obj._table, e)
        log.debug(msg)
        response.setError(1, msg)
        response.status_code = '404 ' + msg
        return
        
    writejson(resp)


@app.route("/<table>/<_id:int:o>")
def handleGet(request, response, table, _id=None):
    obj = getclass(table)
    if _id is None:
        log.debug('Get all rows in table %s' % obj._table)
        # all rows (put some sane limit here maybe?)
        dbquery = db.query(obj)
    else:
        # one row, identified by rowID
        dbquery = db.query().filter(obj.q._id, '=', _id)
        log.debug("Get one row in table '%s' matching query %s" % (obj._table, dbquery.toSql()))
    
    resp = bc.Response()
    try:
        resp.data = []
        for row in db.driver.select(dbquery):  # we call driver directly for efficiency reason
            tmp = {}
            for colname in obj._iterName():
                tmp[colname] = row[colname]
            resp.data.append(tmp)
    except db.Error as e:
        msg = "Could not load objects from table '%s'. %s" % (obj._table, e)
        log.debug(msg)
        response.setError(1, msg)
        response.status_code = '404 ' + msg
        return
        
    if _id is not None and len(resp.data) == 0:
        msg = "Unknown ID %s in table '%s'" % (_id, obj._table)
        log.debug(msg)
        resp.setError(1, msg)
        resp.status_code = '404 ' + msg
        return
    writejson(resp)


@app.route("/<table>", methods=["POST"])
def handlePost(request, response, table):
    obj = getclass(table)
    log.debug("Insert one row in table '%s'" % (obj._table))
    postdata = getData(obj)
    resp = bc.Response()
    try:
        resp.data = db.driver.insert(obj._table, postdata) # we call driver direct for efficiency reason
    except db.Error as e:
        resp.errno = e.errno
        resp.errmsg = e.errmsg
    writejson(resp)


@app.route("/<table>/<_id:int>", methods=["PUT"])
def handlePut(request, response, table, _id):
    obj = getclass(table)
    log.debug("Update one row in table '%s'" % (obj._table))
    putdata = getData(obj)
    putdata['_id'] = _id
    resp = bc.Response()
    try:
        resp.data = db.driver.update(obj._table, putdata) # we call driver direct for efficiency reason
    except db.Error as e:
        resp.errno = e.errno
        resp.errmsg = e.errmsg
    writejson(resp)


@app.route("/<table>/<_id:int>", methods=["DELETE"])
def handleDelete(request, response, table, _id):
    obj = getclass(table)
    if _id == 'filter':
        # filter out specific rows
        dbquery = db.query(obj)
        dbquery.decode(request.querystr)
        log.debug("Delete all rows in table '%s' matching query %s" % (obj._table, dbquery.toSql()))
    else:
        # one row, identified by _id
        dbquery = db.query().filter(obj.q._id, '=', _id)
        log.debug("Delete one row in table '%s' matching id %s" % (obj._table, _id))
    resp = bc.Response()
    try:
        resp.data = db.driver.delete(dbquery)
    except db.Error as e:
        resp.errno = e.errno
        resp.errmsg = e.errmsg
    writejson(resp)


@app.route("/<table>/<_id:int:o>", methods=["HEAD"])
def handleHead(request, response, table, _id=None):
    """
    Count the number of rows matching a query
    Return data in a HTML header
    """
    obj = getclass(table)
    if _id is None:
        log.debug('Count all rows in table %s' % obj._table)
        # all rows (put some sane limit here maybe?)
        dbquery = db.query(obj)
    elif _id is 'filter':
        # filter out specific rows
        dbquery = db.query()
        dbquery.decode(request.querystr)
        log.debug("Count all rows in table '%s' matching query %s" % (obj._table, dbquery.toSql()))

    resp = bc.Response()
    try:
        resp.data = db.driver.count(dbquery)  # we call driver direct for efficiency reason
    except db.Error as e:
        msg = "Could not count objects in table '%s'. %s" % (obj._table, e)
        log.debug(msg)
        # self.status_code = '404 ' + msg
        return
    response.addHeader('X-Result-Count', str(resp.data))


