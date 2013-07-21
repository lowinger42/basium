#!/usr/bin/env python

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

"""
Basium WSGI handler
Can handle static files and importing and running python modules
"""

from __future__ import print_function
from __future__ import unicode_literals
__metaclass__ = type

import os
import sys
import pprint
import threading
import traceback
import mimetypes
import wsgiref.simple_server

import basium

log = basium.log

def show_start_response(status, response_headers):
    print("status=")
    pprint.pprint(status, indent=4)
    print("response_headers=")
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
        self.headers.append( ( basium.b(header), basium.b(value)) )

class AppServer:
    """Main WSGI handler"""
    
    def __init__(self, basium, documentroot=None):
        self.basium = basium
        self.documentroot = documentroot

    def getSession(self):
        pass

    def handleFile(self):
        # ok, must be a file in the file system
        path = self.request.path
        self.request.attr = []
        if path == "":
            path = "/index.html"
        abspath = self.documentroot + path
        attr = []
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
            # print("Importing module=%s  attr=%s" % (module, attr))
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = self.response  # catch all output as html code
            sys.stderr = self.response  # catch all output as html code
            try:
                extpage = basium.importlib_import(module)
                extpage = basium.importlib_reload(extpage)   # only do if file changed? compare timestamp on .py and .pyc
                extpage.run(self.request, self.response, self.basium)
                # log.debug(self.response.out)
            except:
                # todo: make this a custom error page
                # if debug, show additional info, stacktrace
                self.response.contentType = 'text/plain'
                traceback.print_exc()
                sys.stdout = old_stdout
                sys.stderr = old_stderr
                traceback.print_exc()
            sys.stdout = old_stdout
            sys.stderr = old_stderr
        else:
            f = open(abspath, 'rb')
            self.write( f.read() )
            f.close()
        return True

    def handleError(self):
        """File does not exist"""

        self.write( "\n REQUEST_METHOD=%s" % self.request.method )
        self.write( "\n PATH_INFO     =%s" % self.request.path )
        self.write( "\n QUERY_STRING  =%s" % self.request.querystr )
        
        # parse query variables
        queryp = basium.urllib_parse_qs(self.request.environ['QUERY_STRING'])
        for key,val in queryp.items():
            self.write("\nkey: %s, val: %s" % (key, val) )
                                                 
        for key,val in self.request.environ.items():
            self.write("\n%s=%s" % (key, val) )

        self.status = '404 File or directory not found'
        
    def __call__(self, environ, start_response):
        """Main entrypoint for HTTP requests"""
        
        # we store these in self.request, so we can easily pass them over
        # to dynamic loaded modules (pages)
        self.request = Request()
        self.request.environ = environ
        self.request.path = environ['PATH_INFO']
        self.request.method = environ['REQUEST_METHOD']
        self.request.querystr = environ['QUERY_STRING']
        
        self.response = Response2()
        self.write = self.response.write    # make sure stdout works
        
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
            
            # convert to unicode, assuming utf-8 
            self.request.body = self.request.body.decode("utf-8")

        if not self.handleFile():
            self.handleError()

        self.response.contentType += "; charset=utf-8"
        self.response.addHeader( 'Content-type', self.response.contentType )
        self.response.addHeader( 'Content-Length', "%s" % len(self.response.out)  )
        
        start_response(basium.b(self.response.status), self.response.headers)
        return [self.response.out.encode("utf-8")]

    
class WSGIloghandler(wsgiref.simple_server.WSGIRequestHandler):
    """log to basium handler"""
    def log_message(self, *args):
        log.info("%s %s %s" % (args[1], args[2], args[3]))


class Server(threading.Thread):
    """
    Standalone WSGI server
    
    Will respond to the basic request needed by the API, mainly used 
    for development and functional tests
    Note: Does not implement authentification, not suitable for production
    """

    def __init__(self, basium = None, documentroot = None, host='0.0.0.0', port=8051):
        super(Server, self).__init__()
        self.basium = basium
        self.running = True
        self.ready = False
        self.host = host
        self.port = port
        if documentroot != None:
            self.documentroot = documentroot
        else:
            self.documentroot = os.path.dirname( os.path.abspath(sys.argv[0]))

    def run(self):
        
        log.info("-" * 79)
        log.info("Starting WSGI server, press Ctrl-c to quit")
        log.info("Using %s as documentroot" % self.documentroot)
    
        appServer = AppServer(self.basium, documentroot=self.documentroot)
        
        # Instantiate the WSGI server.
        # It will receive the request, pass it to the application
        # and send the application's response to the client
        
        httpd = wsgiref.simple_server.make_server( self.host, self.port, appServer, handler_class=WSGIloghandler)
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
        print("WSGI server stopping")
        

if __name__ == "__main__":
    """Main program, used for unit test"""
    
    import test_util
    
    driver = 'psql'
    dbconf, bas = test_util.getDbConf(driver, checkTables=True)
    server = Server(basium=bas)
    server.run()

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
