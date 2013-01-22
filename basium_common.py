#! /usr/bin/env python


#
# This file does all the heavy lifting, setting up and initializing
# everything that is needed to use the persistence framework, together
# with some common code for all modules
#
# Usage:
#  Create a new instance of the class, with the correct driver
#  Register the tables that should be persisted
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
import types
import json
import datetime
import decimal

import logging
import logging.handlers

#
#
#
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
        if isinstance(msg, basestring):
            msg = msg.replace('\n', ', ')
        self.log.info(msg)

    def warning(self, msg):
        if isinstance(msg, basestring):
            msg = msg.replace('\n', ', ')
        self.log.warning(msg)

    def error(self, msg):
        if isinstance(msg, basestring):
            msg = msg.replace('\n', ', ')
        self.log.error(msg)

    def debug(self, msg):
        if isinstance(msg, basestring):
            msg = msg.replace('\n', ', ')
        self.log.debug(msg)

log = Logger()
log.info("basium logger started.")

import basium_model
import basium_orm
from basium_model import *

from test_tables import *


class Basium(object):


    #
    #
    #
    def __init__(self, driver=None, checkTables=True, conn=None):
        self.cls = {}
        self.driver = driver
        self.checkTables = checkTables
        self.conn = conn


    #
    #
    #    
    def addClass(self, cls):
        if not isinstance(cls, (type, types.ClassType)):
            log.error('Fatal: addClass() called with an instance of an object')
            sys.exit(1)
        if not issubclass(cls, Model):
            log.error('Fatal: addClass() does not inherit from basium_model.Model')
            sys.exit(1)
        self.cls[cls._table] = cls

    
    #
    #
    #
    def start(self):
        if self.driver == 'mysql':
            import basium_driver_mysql as driver
        elif self.driver == 'psql':
            import basium_driver_psql as driver
        elif self.driver == 'json':
            import basium_driver_json as driver
        else:
            log.error('Unknown driver %s' % (self.driver))
            sys.exit(1)

        self.driver = driver.Driver(self.conn['host'], 
                                    self.conn['port'], 
                                    self.conn['user'],
                                    self.conn['pass'],
                                    self.conn['name'])
        self.db = basium_orm.BasiumDatabase(self.driver)
        if self.db == None:
            log.error('Fatal: Cannot check if database exist')
            sys.exit(1)
        if not self.db.isDatabase(self.conn['name']):
            log.error("Fatal: Database %s does not exist" % self.conn['name'])
            return None
    
        if self.checkTables:
            for cls in self.cls.values():
                obj = cls()
                if not self.db.isTable(obj):
                    self.db.createTable(obj)
                else:
                    actions = self.db.verifyTable(obj)
                    if actions != None and len(actions) > 0:
                        self.db.modifyTable(obj, actions)
    
        return self.db

#
# Main result object from functions etc. Makes it possible to return both status
# and the result data
#
class Response():
    def __init__(self):
        self.data = {}
        self.data['errno'] = 0
        self.data['errmsg'] = ''

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
        



#
# Take a date formatted as a string and return a datetime object
#
def dateFromStr(s):
    return datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')


#
# Take a date object and return a string
#
def strFromDate(d):
    return d.strftime('%Y-%m-%d')


#
# Take a datetime object and return a string
#
def strFromDatetime(d):
    return d.strftime('%Y-%m-%d %H:%M:%S')


#
# Handle additional types in JSON encoder
#
class JsonOrmEncoder(json.JSONEncoder):
    def default(self, obj):
        # print "JsonOrmEncoder::default() Type =", type(obj)
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

