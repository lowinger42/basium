#!/usr/bin/env python
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
Unit testing of the basium object persistence code

Uses the json driver, to communicate with the server that does the
actual database/table operations
Runs a WSGI server as a separate thread to do the tests
"""

from __future__ import print_function
from __future__ import unicode_literals
__metaclass__ = type

import sys
import time

import basium
import basium_wsgihandler

import test_tables
import test_util

log = basium.log

def runServer():
    """Start an WSGI server as a separate process"""
    log.info("Starting embedded WSGI server")
    driver = 'psql'
    dbconf, bas = test_util.getDbConf(driver, checkTables=True)
    server = basium_wsgihandler.Server(basium=bas)
    server.daemon = True
    server.start()    # run in thread
    while not server.ready:
        time.sleep(0.1)

if __name__ == "__main__":
    """Main"""
    
    embeddedServer = True
    if len(sys.argv) == 2 and sys.argv[1] == 'noserver':
        embeddedServer = False

    if embeddedServer:
        runServer()

    # we need a database connection(json), for the api test
    driver = "json"
    dbconf, bas = test_util.getDbConf(driver, checkTables=False)

    test_util.doTests(bas, test_tables.BasiumTest)
