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
import basium_compatibilty as c

if __name__.startswith("_mod_wsgi_"):
    # Running under wsgi, apache writes the date&time info so we don't need to write it
    basium.log = basium.Logger(formatstr="%(levelname)s %(message)s ")

log = basium.log

def show_start_response(status, response_headers):
    print("status=")
    pprint.pprint(status, indent=4)
    print("response_headers=")
    pprint.pprint(response_headers, indent=4)


class Request():
    pass

class Response():
    def __init__(self):
        self.status = "200 OK"
        self.contentType = 'text/plain'
        self.headers = []
        self.out = ''

    def write(self, msg):
        self.out += msg

    def addHeader(self, header, value):
        self.headers.append( ( c.b(header), c.b(value)) )

class URLRouterResponse:
    pass

class URLRouter:
    """
    Parses an URL, and returns module & function to execute
      abspath   absolute path in filesystem to module
      absdir    absolute path in filesystem to module directory
      module    name of module to import, including any .py extension
      funcname  function to call in function
      attr      any extra text after funcname
    """
    
    def __init__(self, documentroot):
        self.documentroot = documentroot

    def route(self, url):
        r = URLRouterResponse()
        r.abspath = self.documentroot
        r.module = None
        r.funcname = "index"
        r.path = []
        u = url.split("/")
        if u[0] == "":
            u.pop(0)
        if u[-1] == "":
            u.pop()
        
        # check each path of url and walk down directories
        ix = 0
        while ix < len(u):
            if os.path.isdir(r.abspath + os.sep + u[ix]):
                r.abspath += os.sep + u[ix]
                ix += 1
                continue
            break
        r.absdir = r.abspath

        # check for module name
        if ix < len(u):
            tmp = r.abspath + os.sep + u[ix]
            if os.path.exists(tmp):
                r.module = u[ix]
                r.abspath = tmp
                ix += 1
            elif os.path.exists(tmp + ".py"):
                r.module = u[ix]
                r.abspath = tmp + ".py"
                ix += 1
            else:
                return r    # error, a path is specified but there is no file
        else:
            if os.path.exists(r.abspath + os.sep + "index.py"):
                r.module = "index.py" # no module specified, use default
                r.abspath += os.sep + r.module
            elif os.path.exists(r.abspath + os.sep + "index.html"):
                r.module = "index.html" # no module specified, use default
                r.abspath += os.sep + r.module
            else:
                return r

        # check for function name
        if ix < len(u):
            r.funcname = u[ix]
            ix += 1
        
        # Rest of URL is sent to the dynamic page
        if ix < len(u):
            r.path = u[ix:]

        if r.module[-3:] == ".py":
            r.module = r.module[:-3]
        return r


class AppServer:
    """Main WSGI handler"""
    
    def __init__(self, basium, documentroot=None):
        self.basium = basium
        self.documentroot = documentroot
        self.urlrouter = URLRouter(self.documentroot)

    def getSession(self):
        pass

    def handleFile(self):
        ur = self.urlrouter.route(self.request.path)
        if ur.module == None:
            return False

        mimetype = mimetypes.guess_type(ur.abspath)
        if mimetype[0] != None:
            self.response.contentType = mimetype[0]
        if mimetype[0] == 'text/x-python':
            # we import and execute code in the module
            self.response.contentType = 'text/html'

            old_path = sys.path
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = self.response  # catch all output as html code
            sys.stderr = self.response  # catch all output as html code
            try:
                # to make sure we find the module, we add the directory as first entry in python path
                sys.path.insert(0, ur.absdir)
                log.debug("Importing module=%s  path=%s" % (ur.abspath, ur.path))
                extpage = c.importlib_import(ur.module)
                extpage = c.importlib_reload(extpage)
            except ImportError as err:
                log.debug("Can't import module %s, error %s" % (ur.abspath, err))
                return False
            
            func = None
            if hasattr(extpage, ur.funcname):
                # the specified function exist, call it
                log.debug("Call '%s()' in module '%s'" % (ur.funcname, ur.module))
                func = getattr(extpage, ur.funcname)
            elif hasattr(extpage, "_default"):
                log.debug("Call '_default()' in module '%s'" % (ur.module))
                ur.path.insert(0, ur.funcname)
                func = getattr(extpage, "_default")
            else:
                log.debug("Cant find func %s or _default() in module %s" % (ur.funcname, ur.module))
                return False # no function to call found, return error

            # send data over to dynamic page as module global variables, for easy access
            extpage.log = log
            extpage.basium = self.basium
            extpage.request = self.request
            extpage.path = ur.path
            extpage.response = self.response
            try:
                func()
            except:     # yes, we catch all errors
                # todo: make this a custom error page
                # if debug, show additional info, stacktrace
                self.response.contentType = 'text/plain'
                traceback.print_exc()
                sys.stdout = old_stdout
                sys.stderr = old_stderr
                traceback.print_exc()
            finally:
                sys.path = old_path
                sys.stdout = old_stdout
                sys.stderr = old_stderr
            # log.debug(self.response.out)
        else:
            f = open(ur.abspath, 'rb')
            self.write( f.read() )
            f.close()
        return True

    def handleError(self):
        """File does not exist"""
        self.response.out = "404 Page not found"
        self.response.status = "404 Page not found"
        
    def __call__(self, environ, start_response):
        """Main entrypoint for HTTP requests"""
        
        # we store these in self.request, so we can easily pass them over
        # to dynamic loaded modules (pages)
        self.request = Request()
        self.request.environ = environ
        self.request.path = environ['PATH_INFO']
        self.request.method = environ['REQUEST_METHOD']
        self.request.querystr = environ['QUERY_STRING']
        
        self.response = Response()
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
        
        start_response(c.b(self.response.status), self.response.headers)
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
    """Main program, used for unit test. Start a WSGI server"""
    
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
