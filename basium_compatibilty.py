#! /usr/bin/env python
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
module that implement functionality that is different between
various python versions
"""

from __future__ import absolute_import, division, print_function, unicode_literals
__metaclass__ = type

import sys
import json
import base64
import urllib
import logging.handlers


class Logger():

    def __init__(self, loglevel=logging.DEBUG, formatstr='%(asctime)s %(levelname)s %(message)s ', syslog=False):
        self.logger = logging.getLogger('basium')
        self.logger.setLevel(loglevel)

        # remove all handlers
        for hdlr in self.logger.handlers:
            self.logger.removeHandler(hdlr)

        if syslog:
            self.syslogger = logging.handlers.SysLogHandler(address='/dev/logger')
            self.syslogger.setLevel(loglevel)
            
            self.formatter = logging.Formatter('%(module)s [%(process)d]: %(levelname)s %(message)s')
            self.syslogger.setFormatter(self.formatter)
            self.logger.addHandler(self.syslogger)
        else:
            self.consolehandler = logging.StreamHandler()
            self.consolehandler.setLevel(loglevel)
            
            self.formatter = logging.Formatter(formatstr)
            self.consolehandler.setFormatter(self.formatter)
            self.logger.addHandler(self.consolehandler)

    def info(self, msg):
        msg = str(msg).replace('\n', ', ')
        self.logger.info(to_bytes(msg))

    def warning(self, msg):
        msg = str(msg).replace('\n', ', ')
        self.logger.warning(to_bytes(msg))

    def error(self, msg):
        msg = str(msg).replace('\n', ', ')
        self.logger.error(to_bytes(msg))

    def debug(self, msg):
        try:
            msg = str(msg).replace('\n', ', ')
        except UnicodeDecodeError:
            return
        self.logger.debug(to_bytes(msg))


class Response():
    """
    Main result object from functions etc.
    
    Makes it possible to return both status and the result data
    """
    
    def __init__(self, errno=0, errmsg=''):
        self.errno = errno
        self.errmsg = errmsg
        self.data = None

    def __str__(self):
        return "errno=%s, errmsg=%s, data=%s" % (self.errno, self.errmsg, self.data)

    def ok(self):
        return self.errno == 0

    def isError(self):
        return self.errno != 0

    def getError(self):
        return "Errno: %d Errmsg: '%s'" % (self.errno, self.errmsg)

    def setError(self, errno=1, errmsg=''):
        self.errno = errno
        self.errmsg = errmsg
        
    def dict(self):
        return { "errno": self.errno, "errmsg": self.errmsg, "data": self.data}


class Error(Exception):
    def __init__(self, errno=1, errmsg=""):
        self.errno = errno
        self.errmsg = errmsg
    
    def __str__(self):
        return "errno=%s, errmsg=%s" % (self.errno, self.errmsg)        


major, minor = sys.version_info[0:2]

if major < 3:
    
    """
    Python 2 compability
    
    unicode is array of unicode characters
    str     is array of bytes, with implicit encoding
    bytes   is sames as str (does not exist in python2)
    """
    
    import httplib
    import urllib2
    import urlparse     # p3 uses urllib.parse instead
    import codecs
    
    def b(x):
        return codecs.latin_1_encode(x)[0]

    importlib_import = __import__
    
    importlib_reload = reload

    def isbytes(obj):
        return False

    def isstring(obj):
        return isinstance(obj, basestring)

    def isunicode(obj):
        return isinstance(obj, unicode)
    
    def to_str(x):
        return codecs.latin_1_encode(x)[0]

    def to_unicode(obj):
        return unicode(obj)

    def to_bytes(obj, encoding="utf-8"):
        if isinstance(obj, unicode):
            return obj.encode(encoding)
        return obj

    rawinput = raw_input
    
    class RequestWithMethod(urllib2.Request):
        """Helper class, to implement HTTP GET, POST, PUT, DELETE"""
        def __init__(self, *args, **kwargs):
            self._method = kwargs.pop('method', None)
            urllib2.Request.__init__(self, *args, **kwargs)
    
        def get_method(self):
            return self._method if self._method else super(RequestWithMethod, self).get_method()

    def urllib_request_urlopen(url, method, username=None, password=None, data=None, decode=None):
        respdata = None
        info = None
        req = RequestWithMethod(url, method=method)
        if username != None:
            base64string = base64.standard_b64encode('%s:%s' % (username, password))
            req.add_header("Authorization", "Basic %s" % base64string)
        try:
            if data:
                for k, v in data.items():
                    if isinstance(v, unicode):
                        data[k] = v.encode("utf-8")
                resp = urllib2.urlopen(req, urllib.urlencode(data))
            else:
                resp = urllib2.urlopen(req)
            info = resp.info
        except urllib2.HTTPError as e:
            raise Error(1, "HTTPerror %s" % e)
        except urllib2.URLError as e:
            raise Error(1, "URLerror %s" % e)
        except httplib.HTTPException as e:
            raise Error(1, 'HTTPException %s' % e)

        if decode:
            encoding = None # resp.headers.get_content_charset()
            if encoding == None:
                encoding = "utf-8"
            try:
                tmp = resp.read()
                res = json.loads(tmp.decode(encoding))
                resp.close()
            except ValueError:
                raise Error(1, "JSON ValueError for " + tmp)
            except TypeError:
                raise Error(1, "JSON TypeError for " + tmp)

            try:
                if res['errno'] != 0:
                    raise Error(res['errno'], res['errmsg'])
                respdata = res["data"]
            except KeyError:
                raise Error(1, "Result keyerror, missing errno/errmsg")

        return respdata, info

       
    def urllib_quote(s, safe=None):
        if safe:
            return urllib.quote(s, to_str(safe))
        return urllib.quote(s)
    
    def urllib_parse_qs(data, encoding="utf-8"):
        data = urlparse.parse_qs(data, keep_blank_values=True)
        for k, v in data.items():
            for i in range(len(v)):
                data[k][i] = unicode(v[i], encoding)
        return data

    def urllib_parse_qsl(data, encoding="utf-8"):
        data = urlparse.parse_qsl(data, keep_blank_values=True)
        return data

else:
    """
    Python 3 compability
    
    unicode is array of unicode characters
    str     is same as unicode
    bytes   is array of bytes, with implicit encoding
    """
    
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
    
    def isbytes(obj):
        return isinstance(obj, bytes)

    def isstring(obj):
        return isinstance(obj, str)
    
    def isunicode(obj):
        return isinstance(obj, str)

    def to_str(x):
        return x

    def to_unicode(obj):
        return str(obj)

    def to_bytes(obj, encoding="utf-8"):
        if isinstance(obj, str):
            return obj.encode(encoding)
        return str

    rawinput = input

    class RequestWithMethod(urllib.request.Request):
        """Helper class, to implement HTTP GET, POST, PUT, DELETE"""
        
        def __init__(self, *args, **kwargs):
            self._method = kwargs.pop('method', None)
            urllib.request.Request.__init__(self, *args, **kwargs)
    
        def get_method(self):
            return self._method if self._method else super(RequestWithMethod, self).get_method()
    
    def urllib_request_urlopen(url, method, username=None, password=None, data=None, decode=None):
        respdata = None
        info = None
        req = RequestWithMethod(url, method=method)
        if username != None:
            auth = '%s:%s' % (username, password)
            auth = auth.encode("utf-8")
            req.add_header(b"Authorization", b"Basic " + base64.b64encode(auth))
        try:
            if data:
                resp = urllib.request.urlopen(req, urllib.parse.urlencode(data, encoding="utf-8").encode("ascii") )
            else:
                resp = urllib.request.urlopen(req)
            info = resp.info
        except urllib.error.HTTPError as e:
            raise Error(1, "HTTPerror %s" % e)
        except urllib.error.URLError as e:
            raise Error(1, "URLerror %s" % e)
        
        if decode:
            encoding = resp.headers.get_content_charset()
            if encoding == None:
                encoding = "utf-8"
            try:
                tmp = resp.read().decode(encoding)
                res = json.loads(tmp)
                resp.close()
            except ValueError:
                raise Error(1, "JSON ValueError for " + tmp)
            except TypeError:
                raise Error(1, "JSON TypeError for " + tmp)

            try:
                if res['errno'] != 0:
                    raise Error(res['errno'], res['errmsg'])
                respdata = res["data"]
            except KeyError:
                raise Error(1, "Result keyerror, missing errno/errmsg")

        return respdata, info
    
    def urllib_quote(s, safe=None):
        if safe:
            return urllib.parse.quote(s, safe)
        return urllib.parse.quote(s)
    
    def urllib_parse_qs(data, encoding="utf-8"):
        if isinstance(data, bytes):
            data = data.decode("ascii)")
        return urllib.parse.parse_qs(data, keep_blank_values=True, encoding=encoding)
    
    def urllib_parse_qsl(data, encoding="utf-8"):
        if isinstance(data, bytes):
            data = data.decode("ascii)")
        return urllib.parse.parse_qsl(data, keep_blank_values=True, encoding=encoding)
