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
import threading
import traceback

import mimetypes
import json
from urlparse import parse_qs

import basium_orm
import basium_common
from basium_common import Response
from basium_model import *

from test_tables import *

log = basium_common.log

# ----- Stuff to configure ---------------------------------------------------

# documentroot = '/var/www/candor'

# ----- End of stuff to configure --------------------------------------------


def show_start_response(status, response_headers):
    print "status="
    pprint.pprint(status, indent=4)
    print "response_headers="
    pprint.pprint(response_headers, indent=4)



class Request():
    pass

class Response2():
    def __init__(self):
        self.status = "200 OK"
        self.contentType = 'text/plain'
        self.headers = []
        self.out = ''

    def write(self, msg):
        self.out += msg

    def addHeader(self, header, value):
        self.headers.append((header, value,))

#
# Main WSGI handler
#
class AppServer(object):
    
    def __init__(self, basium, documentroot=None):
        self.basium = basium
        self.db = basium.db
        self.documentroot = documentroot

    def getSession(self):
        pass


    #
    #
    #    
    def handleFile(self):
        # ok, must be a file in the file system
        path = self.request.path
        self.request.attr = []
        if path == "":
            path = "/index.html"
        abspath = self.documentroot + path
        if not os.path.isfile(abspath):

            # no direct match, search for filematch, part of uri
            attr = self.request.path.split("/")
            
            if len(attr) > 0 and attr[0] == '':
                attr.pop(0)
            uri = []
            while True:
                abspath = self.documentroot + "/" + "/".join(uri)
                if os.path.isfile(abspath):
                    break
                abspath += ".py"
                if os.path.isfile(abspath):
                    break
                if len(attr) == 0:
                    return False
                uri.append( attr.pop(0))
                
        self.request.attr = attr
        
        mimetype = mimetypes.guess_type(abspath)
        if mimetype[0] != None:
            self.response.contentType = mimetype[0]
        if mimetype[0] == 'text/x-python':
            # we import and execute code in the module
            self.response.contentType = 'text/html'
            module = abspath[len(self.documentroot):]
            if module[0] == '/': 
                module = module[1:]
            if module[-3:] == '.py':
                module = module[:-3]
            print "Importing module=%s  attr=%s" % (module, attr)
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = self.response  # catch all output as html code
            sys.stderr = self.response  # catch all output as html code
            try:
                extpage = __import__(module)
                extpage = reload(extpage)   # only do if file changed? compare timestamp on .py and .pyc
                extpage.run(self.request, self.response, self.basium)
#                print "response=", self.response.out
            except:
                # if debug, show stacktrace
                # make this a custom error page
                self.response.contentType = 'text/plain'
                traceback.print_exc()
                sys.stdout = old_stdout
                sys.stderr = old_stderr
                traceback.print_exc()
            sys.stdout = old_stdout
            sys.stderr = old_stderr
        else:
            f = file(abspath, 'rb')
            self.write( f.read() )
            f.close()
        return True

    #
    #
    #    
    def handleError(self):
        # file does not exist

        self.write( "\n REQUEST_METHOD=%s" % self.request.method )
        self.write( "\n PATH_INFO     =%s" % self.request.path )
        self.write( "\n QUERY_STRING  =%s" % self.request.querystr )
        
        # parse query variables
        queryp = parse_qs(self.request.environ['QUERY_STRING'])
        for key,val in queryp.items():
            self.write("\nkey: %s, val: %s" % (key, val) )
                                                 
        for key,val in self.request.environ.items():
            self.write("\n%s=%s" % (key, val) )

        self.status = '404 File or directory not found'
        

    #
    #
    #    
    def __call__(self, environ, start_response):
        self.request = Request()
        self.request.environ = environ
        self.request.path = environ['PATH_INFO']
        self.request.method = environ['REQUEST_METHOD']
        self.request.querystr = environ['QUERY_STRING']
        
        self.response = Response2()
        self.write = self.response.write

        
        # query = parse_qs(querystr)
    
        #    age = d.get('age', [''])[0] # Returns the first age value.
        #    hobbies = d.get('hobbies', []) # Returns a list of hobbies.
        #    
        #    # Always escape user input to avoid script injection
        #    age = escape(age)
        #    hobbies = [escape(hobby) for hobby in hobbies]
        
        if self.request.method in ['POST', 'PUT']:
            # get the posted data
            # the environment variable CONTENT_LENGTH may be empty or missing
            try:
                self.request.body_size = int(self.request.environ.get('CONTENT_LENGTH', 0))
            except (ValueError):
                self.request.body_size = 0
    
            # When the method is POST/PUT the query string will be sent
            # in the HTTP request body which is passed by the WSGI server
            # in the file like wsgi.input environment variable.
            self.request.body = self.request.environ['wsgi.input'].read(self.request.body_size)

#        uri = self.request.path.split('/')
#        if len(uri) > 1 and uri[1] == 'api':
#            self.handleAPI(uri[2:])
#        else:
        if not self.handleFile():
            self.handleError()

        self.response.addHeader( 'content-type', self.response.contentType )
        self.response.addHeader( 'Content-Length', str(len(self.response.out) ) )
        
        start_response(self.response.status, self.response.headers)
        return [self.response.out]
    
#
# Start a simple WSGI server
#
# Will respond to the basic request needed by the API, so functional
# test can be done
#
class Server(threading.Thread):

    def __init__(self, basium = None, documentroot = None):
        super(Server, self).__init__()
        self.basium = None
        self.running = True
        self.ready = False
        self.host = '0.0.0.0'
        self.port = 8051
        if documentroot != None:
            self.documentroot = documentroot
        else:
            self.documentroot = os.path.dirname( os.path.abspath(sys.argv[0]))
        print "wsgiserver using %s as documentroot" % self.documentroot

    def run(self):
#        from wsgiref.simple_server import make_server
        import wsgiref.simple_server
        
        print "-" * 79
        print "Starting WSGI server, press Ctrl-c to quit"
    
        if self.basium == None:
            conn={'host': '127.0.0.1', 
                  'port': 3306, 
                  'user':'basium_user', 
                  'pass':'secret', 
                  'name': 'basium_db'}
            basium = basium_common.Basium(driver='mysql', checkTables=True, conn=conn)
            basium.addClass(BasiumTest)
            
            self.db = basium.start()
            if self.db == None:
                sys.exit(1)

        appServer = AppServer(basium, documentroot=self.documentroot)
        
        # Instantiate the WSGI server.
        # It will receive the request, pass it to the application
        # and send the application's response to the client
        
        httpd = wsgiref.simple_server.make_server( self.host, self.port, appServer)
        self.ready = True
        while self.running:
            try:
                httpd.handle_request()
            except KeyboardInterrupt:
                print("Ctrl-C, server shutting down...")
                break
            except:
                self.running = False
        self.ready = False
        print "Thread stopping"
        

# ----------------------------------------------------------------------------
#
#  Main program
#
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    server = Server()
    server.run()

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
