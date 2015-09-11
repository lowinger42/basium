#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

import os
import sys
import threading
import traceback
import mimetypes
import wsgiref.simple_server
import builtins

import basium_common as bc
import basium

import wsgi.common
import wsgi.view

if __name__.startswith("_mod_wsgi_"):
    # Running under wsgi, apache writes the date&time info so we don't
    # need to include it
    basium.log = bc.Logger(formatstr="%(levelname)s %(message)s ")

log = basium.log

wsgi.common.log = log
wsgi.view.log = log

class URLRouterResponse:
    pass


class URLRouter:
    """
    Parses an URL, and returns the controller file that will handle the request
      abspath   absolute path in filesystem to file
      absdir    absolute path in filesystem to file directory
      file      name of file to read
      path      rest of url after file
    """
    def __init__(self, documentroot):
        self.documentroot = documentroot

    def route(self, url):
        r = URLRouterResponse()
        r.abspath = self.documentroot
        r.file = None
        r.path = "/"

        u = list(filter(None, url.split("/")))

        # check each path of url and walk down directories
        ix = 0
        while ix < len(u):
            if not os.path.isdir(r.abspath + os.sep + u[ix]):
                break
            r.abspath += os.sep + u[ix]
            ix += 1
            continue
        r.absdir = r.abspath

        # check for file name
        if ix < len(u):
            tmp = r.abspath + os.sep + u[ix]
            if os.path.exists(tmp):
                r.file = u[ix]
                r.abspath = tmp
                ix += 1
            elif os.path.exists(tmp + ".py"):
                r.file = u[ix] + ".py"
                r.abspath = tmp + ".py"
                ix += 1
            else:
                return r    # error, a path is specified but there is no file
        else:
            if os.path.exists(r.abspath + os.sep + "index.py"):
                r.file = "index.py"  # no file specified, use default
                r.abspath += os.sep + r.file
            elif os.path.exists(r.abspath + os.sep + "index.html"):
                r.file = "index.html"  # no file specified, use default
                r.abspath += os.sep + r.file
            else:
                return r

        # Rest of URL is sent to the dynamic page
        if ix < len(u):
            r.path = "/%s" % "/".join(u[ix:])

        return r


class AppServer:
    """Main WSGI handler"""

    copy_headers = {
        "method":          "REQUEST_METHOD",
        "query_string":    "QUERY_STRING",
        "content_length":  "CONTENT_LENGTH",
        "server_name":     "SERVER_NAME",
        "server_port":     "SERVER_PORT",
        "server_protocol": "SERVER_PROTOCOL",
    }

    def __init__(self, app=None):
        builtins.app = app
        self.app = app
        self.urlrouter = URLRouter(self.app.controller_dir)
        self.request = None
        self.response = None
        sys.path.insert(0, self.app.controller_dir)
        self.app._reload = 1    # during development

    def handleRequest(self, environ):
        self.response = wsgi.common.Response()
        self.request = wsgi.common.Request()
        self.request.path = environ["PATH_INFO"]
        self.request.content_type = environ["CONTENT_TYPE"]

        ur = self.urlrouter.route(self.request.path)
        if ur.file is None:
            return False

        mimetype = mimetypes.guess_type(ur.abspath)
        if mimetype[0] != None:
            self.response.content_type = mimetype[0]
        if self.response.content_type == 'text/x-python':
            # import and execute code in the file

            # we store these in self.request for easy access
            for attr, key in self.copy_headers.items():
                setattr(self.request, attr, environ[key])
            self.request.environ = environ

            self.response.content_type = 'text/html'

            if self.app._reload:
                current_modules = sys.modules.copy()
            old_stdout = sys.stdout
            old_stderr = sys.stderr

            sys.stdout = self.response  # catch all output as html code
            sys.stderr = self.response  # catch all output as html code

            module_name = wsgi.common.pathToPythonModule(self.app.controller_dir, ur.abspath)

            try:
                extpage = wsgi.common.importFile(ur.abspath)
                self.app.freezePageRoutes(module_name)
            except ImportError as err:
                log.debug("Can't import file %s, error %s" % (ur.abspath, err))
                return False

            extpage.log = log
            extpage.app = self.app
            extpage.db = self.app.db
            extpage.request = self.request
            extpage.response = self.response

            func, kwargs = self.app.getMethodFunction(module_name, ur.path, self.request)
            if func is None:
                log.debug("  Cant find route for path %s in file %s" %
                          (ur.path, ur.file))
                return False  # no function to call found, return error
            log.debug("Call function %s() in %s" % (func.__name__, ur.abspath))
            try:
                os.chdir(self.app.documentroot)
                func(self.request, self.response, **kwargs)
            except:     # yes, we catch all errors
                # todo: make this a custom error page
                # todo: if debug, show additional info, stacktrace
                self.response.content_type = 'text/plain'
                traceback.print_exc()
                sys.stdout = old_stdout
                sys.stderr = old_stderr
                traceback.print_exc()
            finally:
                sys.stdout = old_stdout
                sys.stderr = old_stderr

            # unload any dynamically imported modules
            # todo: add a debug/devel flag, only needed during development
            if self.app._reload:
                modules_to_unload = list(set(sys.modules) - set(current_modules))
                log.debug("Unloading modules %s" % ", ".join(modules_to_unload))
                for module in modules_to_unload:
                    del(sys.modules[module])
                # clear the Page() in the module routes
                self.app.flushePageRoutes(module_name)

        else:
            f = open(ur.abspath, 'rb')
            data = f.read()
            if self.response.content_type.startswith("image/"):
                self.response.write(data, encoding=False)  # always binary
                self.response.content_encoding = None
            else:
                # assumes file is in utf-8 format
                self.response.write(data, encoding=False)
            f.close()
        return True

    def handleError(self):
        """File does not exist"""
        self.response._out = []
        self.response.write("404 Page not found\n")
        self.response.status_code = "404 Page not found"

    def __call__(self, environ, start_response):
        """
        Main entrypoint for HTTP requests
        """
        log.debug("basium_wsgihandler.__call__(), PATH_INFO %s" %
                  environ["PATH_INFO"])

        if not self.handleRequest(environ):
            self.handleError()

        self.response.content_type += "; charset=utf-8"
        self.response.addHeader('Content-type', self.response.content_type)
        self.response.addHeader('Content-Length', str(self.response.content_length))

        start_response(self.response.status_code, self.response.headers)
        return self.response.iter()


class WSGIloghandler(wsgiref.simple_server.WSGIRequestHandler):
    """log to db handler"""
    def log_message(self, *args):
        log.info(args[0] % args[1:])


class Server(threading.Thread):
    """
    Standalone WSGI server

    Will respond to the basic request needed by the API, mainly used
    for development and functional tests
    Note: Does not implement authentification, not suitable for production
    """

    def __init__(self, basium=None, documentroot=None, host='0.0.0.0', port=8051):
        super(Server, self).__init__()
        self.running = True
        self.ready = False
        self.host = host
        self.port = port
        if documentroot is None:
            documentroot = os.path.dirname(os.path.abspath(sys.argv[0]))
        self.app = wsgi.common.App(documentroot=documentroot, db=basium)

    def run(self):
        log.info("-" * 79)
        log.info("Starting WSGI server, press Ctrl-c to quit")
        log.info("Using %s as documentroot" % self.app.documentroot)

        appServer = AppServer(app=self.app)

        # Instantiate the WSGI server.
        # It will receive the request, pass it to the application
        # and send the application's response to the client
        httpd = wsgiref.simple_server.make_server(self.host, self.port, appServer, handler_class=WSGIloghandler)
        self.ready = True
        while self.running:
            try:
                httpd.handle_request()
            except KeyboardInterrupt:
                self.log.info("Ctrl-C, server shutting down...")
                break
            except:
                self.running = False
        self.ready = False
        self.log.info("WSGI server stopping")


if __name__ == "__main__":
    import argparse
    import test_tables

    log.info("Starting embedded WSGI server. Note: use only for development and evaluation")



    parser = argparse.ArgumentParser()
    parser.add_argument("--docroot",  dest="documentroot", default="/opt/basium/app" )
    parser.add_argument("--host",     dest="host",     default="127.0.0.1")
    parser.add_argument("--port",     dest="port",     default=8888, type=int)
    parser.add_argument("--dbdriver", dest="dbdriver", default="psql")
    parser.add_argument("--dbname",   dest="dbname",   default="basium_db")
    parser.add_argument("--dbuser",   dest="dbuser",   default="basium_user")
    parser.add_argument("--dbpass",   dest="dbpass",   default="secret")
    
    args = parser.parse_args()
    # (opt, args) = parser.parse_args()

    print("Using:")
    for key in vars(args):
        print("  %13s: %s" % (key, getattr(args, key)))

    app = wsgi.common.App(documentroot=args.documentroot)

    app.dbconf = basium.DbConf(host=args.host, port=args.port, username=args.dbuser, password=args.dbpass, database=args.dbname)

    app.db = basium.Basium(driver=args.dbdriver, dbconf=app.dbconf, checkTables=True)
    app.db.setDebug(bc.DEBUG_ALL)
    app.db.addClass(test_tables.BasiumTest)
    if not app.db.start():
        log.error("Cannot start database driver for wsgi server")

    appServer = AppServer(app=app)
    httpd = wsgiref.simple_server.make_server(args.host, args.port, appServer, handler_class=WSGIloghandler)
    httpd.serve_forever()
