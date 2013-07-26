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

import json
import datetime
import decimal
import logging.handlers

import basium_compatibilty as c

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
        if c.isstring(msg):
            msg = msg.replace('\n', ', ')
        self.logger.info(msg)

    def warning(self, msg):
        if c.isstring(msg):
            msg = msg.replace('\n', ', ')
        self.logger.warning(msg)

    def error(self, msg):
        if c.isstring(msg):
            msg = msg.replace('\n', ', ')
        self.logger.error(msg)

    def debug(self, msg):
        if c.isstring(msg):
            msg = msg.replace('\n', ', ')
        self.logger.debug(msg)


log = Logger()
log.info("Basium default logger started")

# These must be after definition of the logger instance
import basium_orm
import basium_model


class DbConf:
    """Information to the selected database driver, how to connect to database"""
    def __init__(self, host=None, port=None, username=None, password=None, database=None, debugSQL=False, log=None):
        self.host = host
        self.port = None
        self.username = username
        self.password = password
        self.database = database
        self.debugSQL = debugSQL


class Basium(basium_orm.BasiumOrm):
    """Main class for basium usage"""
    
    def __init__(self, logger=None, driver=None, checkTables=True, dbconf=None):
        global log
        if logger:
            self.log = logger
            log = logger
            log.debug("Switching to external logger")
        else:
            self.log = log # use simple logger
        self.log.info("Basium logging started.")
        self.drivername = driver
        self.checkTables = checkTables
        self.dbconf = dbconf
        
        self.cls = {}
        self.drivermodule = None
        self.Response = Response    # for convenience in dynamic pages

    def addClass(self, cls):
        if not isinstance(cls, type):
            self.log.error('addClass() called with an instance of an object')
            return False
        if not issubclass(cls, basium_model.Model):
            self.log.error("Fatal: addClass() called with object that doesn't inherit from basium_model.Model")
            return False
        if cls._table in self.cls:
            self.log.error("addClass() already called for %s" % cls._table)
            return False
        self.cls[cls._table] = cls
        return True
    
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

    def start(self):
        if self.drivermodule:
            self.log.error("basium::start() already called")
            return None
            
        driverfile = "basium_driver_%s" % self.drivername
        try:
            self.drivermodule = __import__(driverfile)
        except ImportError:
            self.log.error('Unknown driver %s, cannot find file %s.py' % (self.drivername, driverfile))
            return None
            
        self.driver = self.drivermodule.Driver(log=self.log, dbconf=self.dbconf)
        if not self.startOrm(self.driver, self.drivermodule):
            log.error("Cannot initialize ORM")
            return None
        if not self.isDatabase(self.dbconf.database):
            log.error("Database %s does not exist" % self.dbconf.database)
            return None
    
        for cls in self.cls.values():
            obj = cls()
            if not self.isTable(obj):
                if self.checkTables:
                    if not self.createTable(obj):
                        return None
            else:
                if self.checkTables:
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
        self.errno = errno
        self.errmsg = errmsg
        self.data = None

    def __str__(self):
        return "errno=%s, errmsg=%s, data=%s" % (self.errno, self.errmsg, self.data)

    def isError(self):
        return self.errno != 0

    def getError(self):
        return "Errno: %d Errmsg: '%s'" % (self.errno, self.errmsg)

    def setError(self, errno=1, errmsg=''):
        self.errno = errno
        self.errmsg = errmsg
        
    def dict(self):
        return { "errno": self.errno, "errmsg": self.errmsg, "data": self.data}

        
def dateFromStr(s):
    """Take a date formatted as a string and return a datetime object"""
    return datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')

def strFromDate(d):
    """Take a date object and return a string"""
    return d.strftime('%Y-%m-%d')

def strFromDatetime(d):
    """Take a datetime object and return a string"""
    return d.strftime('%Y-%m-%d %H:%M:%S')

