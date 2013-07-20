#! /usr/bin/env python

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
Main entrypoint for all basium functionality

This file does all the heavy lifting, setting up and initializing
everything that is needed to use the persistence framework, together
with some common code for all modules

Usage:
 Create a new instance of this class, with the correct driver name
 Register the tables that should be persisted
 Call start
"""

from __future__ import print_function
from __future__ import unicode_literals
__metaclass__ = type

import sys
import types
import json
import datetime
import decimal
import logging.handlers
import base64

import urllib

#
# for python2/3 compability
#
major = sys.version_info[0]
minor = sys.version_info[1]

if major < 3:
    
    """Python 2 compability"""
    
    import httplib
    import urllib2
    import urlparse     # p3 uses urllib.parse instead
    import codecs
    
    def b(x):
        return codecs.latin_1_encode(x)[0]

    importlib_import = __import__
    
    importlib_reload = reload

    def isstring(obj):
        return isinstance(obj, basestring)

    rawinput = raw_input
    
    class RequestWithMethod(urllib2.Request):
        """Helper class, to implement HTTP GET, POST, PUT, DELETE"""
        def __init__(self, *args, **kwargs):
            self._method = kwargs.pop('method', None)
            urllib2.Request.__init__(self, *args, **kwargs)
    
        def get_method(self):
            return self._method if self._method else super(RequestWithMethod, self).get_method()

    def urllib_request_urlopen(url, method, username=None, password=None, data=None, decode=None):
        response = Response()
        req = RequestWithMethod(url, method=method)
        if username != None:
            base64string = base64.standard_b64encode('%s:%s' % (username, password))
            req.add_header("Authorization", "Basic %s" % base64string)
        try:
            if data:
                o = urllib2.urlopen(req, urllib.urlencode(data))
            else:
                o = urllib2.urlopen(req)
            response.set('info', o.info)
        except urllib2.HTTPError as e:
            response.setError(1, "HTTPerror %s" % e)
            return response
        except urllib2.URLError as e:
            response.setError(1, "URLerror %s" % e)
            return response
        except httplib.HTTPException as e:
            response.setError(1, 'HTTPException %s' % e)
            return response

        if decode:
            try:
                tmp = o.read()
                res = json.loads(tmp)
                o.close()
            except ValueError:
                response.setError(1, "JSON ValueError for " + tmp)
                return response
            except TypeError:
                response.setError(1, "JSON TypeError for " + tmp)
                return response

            try:
                if res['errno'] == 0:
                    response.set('data', res['data'])
                else:
                    response.setError(res['errno'], res['errmsg'])
            except KeyError:
                response.setError(1, "Result keyerror, missing errno/errmsg")

        return response

       
    def urllib_quote(s, safe=None):
        if safe:
            return urllib.quote(s, safe)
        return urllib.quote(s)
    
#     def urllib_encode(query, doseq=None):
#         if doseq:
#             return urllib.urlencode(query, doseq)
#         return urllib.urlencode(query)
    
    def urllib_parse_qs(data):
        return urlparse.parse_qs(data, keep_blank_values=True)

    def urllib_parse_qsl(data):
        return urlparse.parse_qsl(data, keep_blank_values=True)

else:
    """Python 3 compability"""
    
    import urllib.request
    import importlib

    def b(x):
        return x

    importlib_import = importlib.__import__

    if minor < 4:
        import imp
        importlib_reload = imp.reload
    else:
        importlib_reload = importlib.reload    
    
    def isstring(obj):
        return isinstance(obj, str)
    
    rawinput = input

    class RequestWithMethod(urllib.request.Request):
        """Helper class, to implement HTTP GET, POST, PUT, DELETE"""
        
        def __init__(self, *args, **kwargs):
            self._method = kwargs.pop('method', None)
            urllib.request.Request.__init__(self, *args, **kwargs)
    
        def get_method(self):
            return self._method if self._method else super(RequestWithMethod, self).get_method()
    
    def urllib_request_urlopen(url, method, username=None, password=None, data=None, decode=None):
        response = Response()
        req = RequestWithMethod(url, method=method)
        if username != None:
            auth = '%s:%s' % (username, password)
            auth = auth.encode("utf-8")
            req.add_header(b"Authorization", b"Basic " + base64.b64encode(auth))
        try:
            if data:
                resp = urllib.request.urlopen(req, urllib.parse.urlencode(data).encode("utf-8"))
            else:
                resp = urllib.request.urlopen(req)
            response.set('info', resp.info)
        except urllib.error.HTTPError as e:
            response.setError(1, "HTTPerror %s" % e)
            return response
        except urllib.error.URLError as e:
            response.setError(1, "URLerror %s" % e)
            return response
        
        if decode:
            encoding = resp.headers.get_content_charset()
            if encoding == None:
                encoding = "utf-8"
            try:
                tmp = resp.read().decode(encoding)
                res = json.loads(tmp)
                resp.close()
            except ValueError:
                response.setError(1, "JSON ValueError for " + tmp)
                return response
            except TypeError:
                response.setError(1, "JSON TypeError for " + tmp)
                return response

            try:
                if res['errno'] == 0:
                    response.set('data', res['data'])
                else:
                    response.setError(res['errno'], res['errmsg'])
            except KeyError:
                response.setError(1, "Result keyerror, missing errno/errmsg")

        return response
    
    def urllib_quote(s, safe=None):
        if safe:
            return urllib.parse.quote(s, safe)
        return urllib.parse.quote(s)
    
#     def urllib_encode(query, doseq=None):
#         if doseq:
#             return urllib.parse.urlencode(query, doseq)
#         return urllib.parse.urlencode(query)
    
    def urllib_parse_qs(data):
        return urllib.parse.parse_qs(data, keep_blank_values=True)
    
    def urllib_parse_qsl(data):
        return urllib.parse.parse_qsl(data, keep_blank_values=True)
    


class Logger():

    def __init__(self, loglevel=None):
        logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s ')
        self.log = logging.getLogger('candor')
        if loglevel != None:
            self.setLevel(loglevel)
        else:
            self.log.setLevel(logging.DEBUG)  # log everything as default

    def activateSyslog(self, mod=None):
        syslogger = logging.handlers.SysLogHandler(address='/dev/log')
        if mod == None:
            formatter = logging.Formatter('%(module)s [%(process)d]: %(levelname)s %(message)s')
        else:
            formatter = logging.Formatter(mod + '[%(process)d]: %(levelname)s %(message)s')
        syslogger.setFormatter(formatter)
        
        # remove all other handlers
        for hdlr in self.log.handlers:
            self.log.removeHandler(hdlr)
            
        self.log.addHandler(syslogger)

    def setLevel(self, loglevel):
        if loglevel == 'info':
            self.log.setLevel(logging.INFO)
        elif loglevel == 'warning':
            self.log.setLevel(logging.WARNING)
        elif loglevel == 'error':
            self.log.setLevel(logging.ERROR)
        elif loglevel == 'debug':
            self.log.setLevel(logging.DEBUG)
        else:
            self.log.error("Unknown log level %s" % loglevel)
            sys.exit(1)

    def info(self, msg):
        if isstring(msg):
            msg = msg.replace('\n', ', ')
        self.log.info(msg)

    def warning(self, msg):
        if isstring(msg):
            msg = msg.replace('\n', ', ')
        self.log.warning(msg)

    def error(self, msg):
        if isstring(msg):
            msg = msg.replace('\n', ', ')
        self.log.error(msg)

    def debug(self, msg):
        if isstring(msg):
            msg = msg.replace('\n', ', ')
        self.log.debug(msg)

class DbConf:
    """Information to the selected database driver, how to connect to database"""
    def __init__(self, host=None, port=None, username=None, password=None, database=None, debugSQL=False):
        self.host = host
        self.port = None
        self.username = username
        self.password = password
        self.database = database
        self.debugSQL = debugSQL

log = Logger()
log.info("Basium logger started.")

import basium_orm
import basium_model


class Basium(basium_orm.BasiumOrm):

    def __init__(self, driver=None, checkTables=True, dbconf=None):
        self.cls = {}
        self.drivername = driver
        self.checkTables = checkTables
        self.dbconnection = dbconf

    def addClass(self, cls):
        if not isinstance(cls, type):
            log.error('Fatal: addClass() called with an instance of an object')
            sys.exit(1)
        if not issubclass(cls, basium_model.Model):
            log.error("Fatal: addClass() called with object that doesn't inherit from basium_model.Model")
            sys.exit(1)
        self.cls[cls._table] = cls
    
    def start(self):
        driverfile = "basium_driver_%s" % self.drivername
        try:
            self.drivermodule = __import__(driverfile)
        except ImportError:
            log.error('Unknown driver %s, cannot find file %s.py' % (self.drivername, driverfile))
            sys.exit(1) # todo, return error instead of exit
            
        self.driver = self.drivermodule.Driver(self.dbconnection)
        if not self.startOrm(self.driver, self.drivermodule):
            log.error("Fatal: cannot continue")
            return None
        if not self.isDatabase(self.dbconnection.database):
            log.error("Fatal: Database %s does not exist" % self.dbconnection.database)
            return None
    
        if self.checkTables:
            for cls in self.cls.values():
                obj = cls()
                if not self.isTable(obj):
                    if not self.createTable(obj):
                        return None
                else:
                    actions = self.verifyTable(obj)
                    if actions != None and len(actions) > 0:
                        self.modifyTable(obj, actions)
    
        return True

class Response():
    """
    Main result object from functions etc.
    
    Makes it possible to return both status and the result data
    """
    
    def __init__(self, errno=0, errmsg=''):
        self.data = {}
        self.data['errno'] = errno
        self.data['errmsg'] = errmsg

    def __str__(self):
        res = []
        for key, val in self.data.items():
            res.append('%s=%s' % (key, val))
        return ", ".join(res)

    def isError(self):
        return self.data['errno'] != 0

    def getError(self):
        return "Errno: %d Errmsg: '%s'" % (self.data['errno'], self.data['errmsg'])

    def setError(self, errno=1, errmsg=''):
        self.data['errno'] = errno
        self.data['errmsg'] = errmsg

    def get(self, key=None):
        if key == None:
            return self.data
        return self.data[key]

    def set(self, key, val):
        self.data[key] = val

def dateFromStr(s):
    """Take a date formatted as a string and return a datetime object"""
    return datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')

def strFromDate(d):
    """Take a date object and return a string"""
    return d.strftime('%Y-%m-%d')

def strFromDatetime(d):
    """Take a datetime object and return a string"""
    return d.strftime('%Y-%m-%d %H:%M:%S')

class JsonOrmEncoder(json.JSONEncoder):
    """Handle additional types in JSON encoder"""
    def default(self, obj):
        # print( "JsonOrmEncoder::default() Type =", type(obj) )
        if isinstance(obj, Response):
            return obj.data
        if isinstance(obj, datetime.date):
            return strFromDatetime(obj)
        if isinstance(obj, datetime.datetime):
            return strFromDatetime(obj)
        if isinstance(obj, decimal.Decimal):
            return str(obj)
        if isinstance(obj, basium_model.Model):
            return obj.getStrValues()
        return json.JSONEncoder.default(self, obj)
