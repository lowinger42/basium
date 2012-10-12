#!/usr/bin/env python

# 
# Basium wsgi handler
# implements a web framework with object persistence
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

import os
import sys
import datetime
import pprint
import decimal

import json
import urllib2
from urlparse import parse_qs

import basium_orm
import basium_common
from basium_common import Response
from basium_model import *

from test_tables import *

log = basium_common.log

# ----- Stuff to configure ---------------------------------------------------

documentroot = '/var/www/candor'

mapExtToContenType = {
    '.html' : 'text/html',
    '.css': 'text/css',
    '.js' : 'text/javascript',
}

# ----- End of stuff to configure --------------------------------------------


# ----------------------------------------------------------------------------
#
# Server side Database Class
# Use JSON over HTTP to manipulate the local DB
#
# ----------------------------------------------------------------------------
#
#class JSONserver:
#    
#    def __init__(self, db):
#        self.db = db
#
#    # count the number of rows in a table
#    def count(self, obj, id):
#        response = Response()
#        co = self.db.count(obj)
#        if co != None:
#            response.set('count', co)
#        else:
#            response.setError(1, '')
#        return json.dumps(response.get())
#
#        
#    # fetch a row from the SQL database and return as json formatted data
#    def load(self, obj):
#        res = Response()
#        self.db.load(obj)
#        if obj.id >= 0:
#            res.set('data', obj.getValues() )
#        else:
#            res.setError(1, '')
#        return json.dumps(res.get())
#
#    # take the JSON formatted data, create an object and store in SQL database
#    # returns id, formatted as json so client can update the id if insert was done
#    def store(self, obj, jsontext):
#        res = Response()
#        jdata = json.loads(jsontext)
#        for (colname, column) in obj._columns.items():
#            obj._values[colname] = column.toPython( jdata[colname] )
#        if self.db.store(obj):
#            res.set('id', obj.id)
#        else:
#            res.setError(1, '')
#        return json.dumps(res.get())
#
#    # delete the object from the SQL database
#    def delete(self, obj):
#        res = Response()
#        res['_errno'] = 1
#        res['_errmsg'] = 'Not implemented'
#        return json.dumps(res.get())



def show_start_response(status, response_headers):
    print "status="
    pprint.pprint(status, indent=4)
    print "response_headers="
    pprint.pprint(response_headers, indent=4)
    

#
# Main WSGI handler
#
class AppServer(object):
    
    def __init__(self, basium):
        self.basium = basium
        self.db = basium.db

    def getSession(self):
        pass

    #
    #
    #
    def handleGet(self, classname, id_, uri):
        obj = classname()
        if id_ == None:
            log.debug('Get all rows in table %s' % obj._table)
            # all rows (put some sane limit here maybe?)
            dbquery = basium_orm.Query(db, classname)
        elif id_ == 'filter':
            # filter out specific rows
            dbquery = basium_orm.Query(db, classname)
            dbquery.decode(self.querystr)
            log.debug("Get all rows in table '%s' matching query %s" % (obj._table, dbquery.toSql()))
        else:
            # one row, identified by rowID
            dbquery = basium_orm.Query(db, classname).filter('id', '=', id_)
            log.debug("Get one row in table '%s' matching query %s" % (obj._table, dbquery.toSql()))

        response = db.driver.select(dbquery)  # we call driver direct for efficiency reason
        if response.isError():
            msg = "Could not load objects from table '%s'. %s" % (obj._table, response.getError())
            log.debug(msg)
            self.out += msg
            self.status = '404 ' + msg
            return
        lst = response.get('data')
        if id_ != None and id_ != 'filter' and len(lst) == 0:
            msg = "Unknown ID %s in table '%s'" % (id_, obj._table)
            response.setError(1, msg)
            log.debug(msg)
            self.status = '404 ' + msg
        self.out += json.dumps(response, cls=basium_common.JsonOrmEncoder)

    #
    #
    #    
    def handlePost(self, classname, id_, uri):
        obj = classname()
        if id_ != None:
            self.status = "400 Bad Request, no ID needed to insert a row"
            return

        postdata = json.loads(self.request_body)    # decode data that should be stored in database
        response = db.driver.insert(obj._table, postdata) # we call driver direct for efficiency reason
        self.out += json.dumps(response.get(), cls=basium_common.JsonOrmEncoder)
        self.status = '200 OK'

    #
    #
    #    
    def handlePut(self, classname, id_, uri):
        if id_ == None:
            self.status = "400 Bad Request, need ID to update a row"
            return
        # update row
        obj = classname()
        putdata = json.loads(self.request_body)    # decode data that should be stored in database
        response = db.driver.update(obj._table, putdata) # we call driver direct for efficiency reason
        self.out += json.dumps(response.get(), cls=basium_common.JsonOrmEncoder)
        self.status = '200 OK'

    #
    #
    #    
    def handleDelete(self, classname, id_, uri):
        if id_ == None:
            self.status = "400 Bad Request, need ID to delete a row"
            return
#        obj = classname()


    #
    # Count the number of rows matching a query
    # Return data in a HTML header
    #
    def handleHead(self, classname, id_, uri):
        obj = classname()
        if id_ == None:
            log.debug('Count all rows in table %s' % obj._table)
            # all rows (put some sane limit here maybe?)
            dbquery = basium_orm.Query(db, classname)
        elif id_ == 'filter':
            # filter out specific rows
            dbquery = basium_orm.Query(db, classname)
            dbquery.decode(self.querystr)
            log.debug("Count all rows in table '%s' matching query %s" % (obj._table, dbquery.toSql()))

        response = db.driver.count(dbquery)  # we call driver direct for efficiency reason
        if response.isError():
            msg = "Could not count objects in table '%s'. %s" % (obj._table, response.getError())
            log.debug(msg)
#            self.out += msg
            self.status = '404 ' + msg
            return
        self.headers.append( ('X-Result-Count', str(response.get('data')) ), )

    
    #
    #
    #    
    def handleAPI(self, uri):
        ix = 0
        if not uri[ix] in self.basium.cls:
            self.status = "404 table '%s' not found" % (uri[ix])
            return
        classname = self.basium.cls[uri[ix]]
        ix += 1
        if len(uri) > ix:
            id_ = uri[ix]
            ix += 1
        else:
            id_ = None
        if self.method == 'GET':
            self.handleGet(classname, id_, uri[ix:])
        elif self.method == "POST":
            self.handlePost(classname, id_, uri[ix:])
        elif self.method == "PUT":
            self.handlePut(classname, id_, uri[ix:])
        elif self.method == "DELETE":
            self.handleDelete(classname, id_, uri[ix:])
        elif self.method == "HEAD":
            self.handleHead(classname, id_, uri[ix:])
        else:
            # not a request we understand
            self.status = "400 Bad Request"

    #
    #
    #    
    def handleFile(self):
        # ok, must be a file in the file system
        path = self.path
        if path == "":
            path = "/index.html"
        path = documentroot + path
        if not os.path.exists(path):
            return False
        
        f = file(path, 'rb')
        self.out += f.read()
        f.close()
        
        # guess content type
        ext = os.path.splitext(path)
        if len(ext) > 1:
            ext = ext[1]
            if ext in mapExtToContenType:
                self.contentType = mapExtToContenType[ext]
        return True

    #
    #
    #    
    def handleError(self):
        # file does not exist

        self.out += "\n REQUEST_METHOD=" + self.method
        self.out += "\n PATH_INFO=" + self.path
        self.out += "\n QUERY_STRING=" + self.querystr
        
        # parse query variables
        queryp = parse_qs(self.environ['QUERY_STRING'])
        for key,val in queryp.items():
            self.out += "\nkey: %s, val: %s" % (key, val)
                                                 
        for key,val in self.environ.items():
            self.out += "\n%s=%s" % (key, val)

        self.status = '404 File or directory not found'
        

    #
    #
    #    
    def __call__(self, environ, start_response):
        self.out = ''
        self.status = "200 OK"
        self.headers = []
        self.contentType = 'text/plain'
        
        self.environ = environ
        self.path = environ['PATH_INFO']
        self.method = environ['REQUEST_METHOD']
        self.querystr = environ['QUERY_STRING']
        
        # query = parse_qs(querystr)
    
        #    age = d.get('age', [''])[0] # Returns the first age value.
        #    hobbies = d.get('hobbies', []) # Returns a list of hobbies.
        #    
        #    # Always escape user input to avoid script injection
        #    age = escape(age)
        #    hobbies = [escape(hobby) for hobby in hobbies]
        
        
        if self.method in ['POST', 'PUT']:
            # get the posted data
            # the environment variable CONTENT_LENGTH may be empty or missing
            try:
                self.request_body_size = int(self.environ.get('CONTENT_LENGTH', 0))
            except (ValueError):
                self.request_body_size = 0
    
            # When the method is POST/PUT the query string will be sent
            # in the HTTP request body which is passed by the WSGI server
            # in the file like wsgi.input environment variable.
            self.request_body = self.environ['wsgi.input'].read(self.request_body_size)

        uri = self.path.split('/')
        if len(uri) > 1 and uri[1] == 'api':
            self.handleAPI(uri[2:])
        else:
            if not self.handleFile():
                self.handleError()

        self.headers.append( ('content-type', self.contentType ), )
        self.headers.append( ('Content-Length', str(len(self.out)) ), )
        
        start_response(self.status, self.headers)
        return [self.out]
    
#
# Start a simple WSGI server
#
# Will respond to the basic request needed by the API, so functional
# test can be done
#
def server(basium):
    from wsgiref.simple_server import make_server
    
    global db
    global jsonserver
    
    print "-" * 79
    print "Starting WSGI server, press Ctrl-c to quit"

    # jsonserver = JSONserver(db)
    
    db = basium.start()
    if db == None:
        sys.exit(1)

    appServer = AppServer(basium)
    
    # Instantiate the WSGI server.
    # It will receive the request, pass it to the application
    # and send the application's response to the client
    try:
        httpd = make_server(
           '0.0.0.0', # The host name.
           8051, # A port number where to wait for the request.
           appServer # Our application object name
           )
    
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("Ctrl-C, server shutting down...")

# ----------------------------------------------------------------------------
#
#  Main program
#
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    
    conn={'host':'localhost', 
          'port':'8051', 
          'user':'basium_user', 
          'pass':'secret', 
          'name': 'basium_db'}
    basium = basium_common.Basium(driver='mysql', checkTables=True, conn=conn)
    basium.addClass(BasiumTest)
    
    server(basium)

    
#
# Some test code
#
# REQUEST_METHOD
# The HTTP request method, such as "GET" or "POST". This cannot ever be an empty string, 
# and so is always required.
#
# SCRIPT_NAME
# The initial portion of the request URL's "path" that corresponds to the application 
# object, so that the application knows its virtual "location". This may be an empty string, 
# if the application corresponds to the "root" of the server.
#
# PATH_INFO
# The remainder of the request URL's "path", designating the virtual "location" of the 
# request's target within the application. This may be an empty string, if the request 
# URL targets the application root and does not have a trailing slash.
#
# QUERY_STRING
# The portion of the request URL that follows the "?", if any. May be empty or absent.
#
# CONTENT_TYPE
# The contents of any Content-Type fields in the HTTP request. May be empty or absent.
#
# CONTENT_LENGTH
# The contents of any Content-Length fields in the HTTP request. May be empty or absent.
#
# SERVER_NAME, SERVER_PORT
# When combined with SCRIPT_NAME and PATH_INFO, these variables can be used to complete the
# URL. Note, however, that HTTP_HOST, if present, should be used in preference to SERVER_NAME
# for reconstructing the request URL. See the URL Reconstruction section below for more detail. 
# SERVER_NAME and SERVER_PORT can never be empty strings, and so are always required.
#
# SERVER_PROTOCOL
# The version of the protocol the client used to send the request. Typically this will
# be something like "HTTP/1.0" or "HTTP/1.1" and may be used by the application to determine 
# how to treat any HTTP request headers. (This variable should probably be called REQUEST_PROTOCOL, 
# since it denotes the protocol used in the request, and is not necessarily the protocol that will
# be used in the server's response. However, for compatibility with CGI we have to keep the existing name.)
#
# HTTP_ Variables
# Variables corresponding to the client-supplied HTTP request headers (i.e., variables whose names 
# begin with "HTTP_"). The presence or absence of these variables should correspond with the presence
# or absence of the appropriate HTTP header in the request. 
#

#    env = {}
#    env['PATH_INFO'] = '/protocoltest/33'
#    env['QUERY_STRING'] = ''
#    env['REQUEST_METHOD'] = 'GET' # The HTTP request method, such as "GET" or "POST". This cannot ever be an empty string, and so is always required.
#    print application(env, show_start_response)
