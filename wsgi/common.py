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
Basium 
- Application
- Request
- Response
- Routes
"""

import os
import sys
import collections
import inspect
import importlib.machinery

import urllib.parse


class Param:
    def __init__(self, name=None, typ='str', optional=False):
        self.name = name
        self.typ = typ
        self.optional = optional

    def __str__(self):
        return "Param(name=%s, typ=%s, optional=%s)" % (
            self.name, self.typ, self.optional)


class Route:
    def __init__(self, methods=None, func=None):
        self.methods = methods
        self.func = func
        self.param = []

    def __str__(self):
        s = "Route(methods=%s, func=%s" % (self.methods, self.func.__name__)
        if len(self.param):
            s += "["
            for p in self.param:
                s += "%s, " % p
            s += "]"
        else:
            s += ", param=[]"
        return s + ")"


# class Pfile:
#     def __init__(self, filename):
#         self.filename = filename
#         self._module = None

#     @property
#     def module(self):
#         if not self.module:
#             self._module = ""
#         return self._module


class AddSysPath:
    def __init__(self, path):
        self.path = path

    def __enter__(self):
        self.savedPath = sys.path.copy()
        sys.path.insert(0, self.path)

    def __exit__(self, typ, value, tb):
        sys.path = self.savedPath


def importFile(pythonFile):
    log.debug("Importing module '%s'" % (pythonFile))
    module_name = os.path.basename(pythonFile)
    module_name = os.path.splitext(module_name)[0]
    loader = importlib.machinery.SourceFileLoader(module_name, pythonFile)
    with AddSysPath(app.app_dir):
        return loader.load_module()


def pathToPythonModule(base, name):
    name = name[len(base):]  # strip the controller path
    if name[0] == "/":
        name = name[1:]
    if name.endswith(".py"):
        name = name[:-3]
    name = name.replace("/", ".")
    return name


class WsgiError(Exception):
    def __init__(self, message, status_code=403, *args):
        self.message = message
        self.status_code = status_code
        super().__init__(message, status_code, *args)


class ArgsHandler:
    """
    Handle arguments sent in URL
    Try to decode type, and store decoded type/value in a cache
    """
    def __init__(self, path):
        self._path = path
        self._patharray = list(filter(None, path.split("/")))

        self._args = {}

    def __len__(self):
        return len(self._patharray)

    def getUndecoded(self, ix):
        return self._patharray[ix]

    def getTypeValue(self, ix):
        try:
            return self._args[ix]
        except KeyError:
            pass

        while True:
            try:
                v = int(self._patharray[ix])
                t = 'int'
                break
            except ValueError:
                pass

            try:
                v = float(self._patharray[ix])
                t = 'float'
                break
            except ValueError:
                pass

            v = self._patharray[ix]
            t = 'str'
            break
        self._args[ix] = t, v
        return t, v


class Page:
    """
    Each html page using render_view has one instance of this class.
    Contains the current request & response data
    """
    def __init__(self):
        self._routerFunctions = []
        self.frozen = False

    def add(self, path=None, methods=None, func=None):
#        log.debug("  Page.add(path='%s', methods=%s, func='%s')" %
#                  (path, methods, func.__name__))

        rf = Route(methods=methods, func=func)

        paths = filter(None, path.split("/"))
        for p in paths:
            if len(p) > 3 and p[0] == "<" and p[-1] == ">":
                # argument
                # format is <name:type:flag>
                # name:  valid python variable name
                # type:  int, string (default), float
                # flags: o - optional
                args = p[1:-1].split(":")
                if len(args) > 3:
                    raise WsgiError("Too many arguments %s" % args)
                param = Param(name=args[0])
                if len(args) > 1:
                    param.typ = args[1]
                    if param.typ not in ['str', 'int', 'float']:
                        raise WsgiError("Unknown datatype %s" % param.typ)
                if len(args) > 2:
                    param.optional = args[2] == 'o'
            else:
                # static text
                param = Param(p, typ='static')
 #           log.debug("    %s" % param)
            rf.param.append(param)

        self._routerFunctions.append(rf)

    def getFunction(self, path, request):
        """
        Based on a function name, return the function to call and the arguments.
        All required args and arg-type need to match
        """
        args = ArgsHandler(path)
        kwargs = {}

        log.debug("Searching for a matching function. method=%s path=%s"  %
                  (request.method, path))
        for rf in self._routerFunctions:
#            log.debug("  trying to match function %s" % rf)
            if not request.method in rf.methods:
#                log.debug("    no match, method %s not in %s" % (request.method, rf.methods))
                continue
            ix = 0
            found = True
            for param in rf.param:
                ix += 1

                if ix > len(args):
                    # we are out of arguments
                    if not param.optional:
                        found = False
                    break

#                log.debug("    param %s ix=%s" % (param, ix))

                if param.typ == 'static' and param.name == args.getUndecoded(ix-1):
#                    log.debug("      found a match, static, continue")
                    continue

                typ, val = args.getTypeValue(ix-1)
                if param.typ == typ:
#                    log.debug("      found a match, typ=%s, value=%s" % (param.typ, val))
                    kwargs[param.name] = val
                    continue
#                log.debug("      No match, wanted type %s got type %s" % (param.typ, typ))
                found = False
                break

            if found:
#                log.debug("  Function found")
                return rf.func, kwargs

#        log.debug("No matching function found")
        return None, None


class App:
    """
    Each wsgi server has one instance of this class.
    Contains global variables that are useful in dynamic web pages
    """
    def __init__(self, documentroot=None, controller_dir=None, app_dir=None, 
                 view_dir=None, view_code_dir=None, model_dir=None, db=None):
        self.documentroot = documentroot
        
        if app_dir is None:
            app_dir = documentroot
        self.app_dir = app_dir

        if view_dir is None:
            view_dir = app_dir + "/view"
        self.view_dir = view_dir
        
        if view_code_dir is None:
            view_code_dir = app_dir + "/view/code"
        self.view_code_dir = view_code_dir

        if model_dir is None:
            model_dir = app_dir + "/model"        
        self.model_dir = model_dir
        
        if controller_dir is None:
            controller_dir =  app_dir + "/controller"
        self.controller_dir = controller_dir

        self.dbconf = None
        self.db = db
        
        self._modules = {}  # key is module name, value is instance of Page()

    def route(self, path, methods=["GET"]):
        # log.debug("App.route(path='%s', methods=%s)" % (path, methods))

        def add(func):
            calling_module = inspect.stack()[1][1]
            calling_module = pathToPythonModule(
                app.controller_dir, calling_module)
            # log.debug("App.route.add(func=%s, calling module=%s)" % (func, calling_module))

            if calling_module not in self._modules:
                self._modules[calling_module] = Page()
            page = self._modules[calling_module]
            if not page.frozen:
                page.add(path=path, methods=methods, func=func)

        return add

    def freezePageRoutes(self, module_name):
        try:
            self._modules[module_name].frozen = True
        except KeyError:
            pass
            
    def flushePageRoutes(self, module_name):
        try:
            del self._modules[module_name]
        except KeyError:
            pass

    def getMethodFunction(self, module_name, path, request):
        """
        Search in the specified module for a function to call,
        matching path and method
        """

        if module_name not in self._modules:
            log.debug("No such module %s" % module_name)
            return None, None

        return self._modules[module_name].getFunction(path, request)


class Request:
    """
    Contains information on the HTTP request, from the client
    """
    def __init__(self):
        self.body_size = None   # valid if method is POST or PUT
        self.path = None
        self.method = None

        self.scheme = None      # http / https

        self.headers = {}

        self.accept = None
        self.args = None        # passed URL parameters

        self._form = None

    def form(self, key=None, defaultdict=False):
        # lazy decode form data, valid for POST, PUT
        if self._form is None:
            if self.method not in ['POST', 'PUT']:
                raise WsgiError("Cannot access form data with method %s" % self.method, 403)

            # get the posted data
            # the environment variable CONTENT_LENGTH may be empty or missing
            try:
                self.body_size = int(self.environ.get('CONTENT_LENGTH', 0))
            except (ValueError):
                self.body_size = 0

            # When the method is POST/PUT the query string will be sent
            # in the HTTP request body which is passed by the WSGI server
            # in the file like wsgi.input environment variable.
            self.body = self.environ['wsgi.input'].read(self.body_size)

            # decode the data
            if defaultdict:
                self._form = collections.defaultdict(str)
            else:
                self._form = {}

            tmp = urllib.parse.parse_qs(self.body.decode())
            for key_, val in tmp.items():
                self._form[key_] = val[0]

        if key is None:
            return self._form

        if key in self._form:
            return self._form[key]
        return None


class Response:
    """
    Stores the HTTP response, sent back to the user
    """
    def __init__(self):
        self.status_code = "200 OK"
        self.content_type = 'text/plain'
        self.content_encoding = "utf-8"

        self.headers = []

        self._out = []
        self.content_length = 0

    def write(self, msg, encoding=True):
        if msg is not None:
            msg = str(msg)
            msg = msg.encode(self.content_encoding)
            self.content_length += len(msg)
            self._out.append(msg)

    def addHeader(self, header, value):
        self.headers.append((header, value))

    def iter(self):
        for line in self._out:
            yield line


# These are mostly for IDEs so they can autocomplete classes
if 0:
    log = None
    db = basium.Basium()
    app = App()
    request = Request()
    response = Response()
    