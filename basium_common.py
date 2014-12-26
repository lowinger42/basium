#! /usr/bin/env python3
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

DEBUG_SQL        = 1 <<  0
DEBUG_TABLE_MGMT = 1 <<  1
DEBUG_ALL        = 1 <<  2 - 1

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
        self.logger.info(msg)

    def warning(self, msg):
        msg = str(msg).replace('\n', ', ')
        self.logger.warning(msg)

    def error(self, msg):
        msg = str(msg).replace('\n', ', ')
        self.logger.error(msg)

    def debug(self, msg):
        try:
            msg = str(msg).replace('\n', ', ')
        except UnicodeDecodeError:
            return
        self.logger.debug(msg)



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

