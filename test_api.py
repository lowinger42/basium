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

from test_tables import *
from test_util import *

def runServer():
    """Start an WSGI server as a separate process"""
    log.info("Starting embedded WSGI server")
    driver = 'psql'

    if driver == 'psql':
        conn={'host':'localhost', 
              'port':'5432',
              'user':'basium_user', 
              'pass':'secret', 
              'name': 'basium_db'}
    elif driver == 'mysql':
        conn={'host':'localhost', 
              'port':'3306', 
              'user':'basium_user', 
              'pass':'secret', 
              'name': 'basium_db'}
    else:
        print("Fatal: Unknown driver %s" % driver)
        sys.exit(1)

    bas = basium.Basium(driver=driver, checkTables=True, conn=conn)
    bas.addClass(BasiumTest)
    db = bas.start()
    if db == None:
        sys.exit(1)
    
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
    conn={'host':'http://localhost:8051', 
          'port':'', 
          'user':'basium_user', 
          'pass':'secret', 
          'name': 'basium_db'}
    bas = basium.Basium(driver='json', checkTables=False, conn=conn)
    bas.addClass(BasiumTest)
    db = bas.start()
    if db == None:
        sys.exit(1)

    doTests(db, BasiumTest)
