#!/usr/bin/env python

# -----------------------------------------------------------------------------
# Unit testing of the object persistence code
# Uses any supported driver to communicate directly with the database
# -----------------------------------------------------------------------------

#
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
#

__metaclass__ = type

import sys

import basium_common

from test_tables import *
from test_util import *
    
    
# ----------------------------------------------------------------------------
#
#    Run all the tests
#
# ----------------------------------------------------------------------------

if __name__ == "__main__":

    driver = 'psql'

    # psql
    if driver == 'psql':
        conn={'host':'localhost', 
              'port':'5432',
              'user':'basium_user', 
              'pass':'secret', 
              'name': 'basium_db'}
        basium = basium_common.Basium(driver='psql', checkTables=True, conn=conn) 
    
    elif driver == 'mysql':
        conn={'host':'localhost', 
              'port':'8051', 
              'user':'basium_user', 
              'pass':'secret', 
              'name': 'basium_db'}
        basium = basium_common.Basium(driver='mysql', checkTables=True, conn=conn)
    else:
        print "Unknown driver %s" % driver
        sys.exit(1)

    basium.addClass(BasiumTest)
    db = basium.start()
    if db == None:
        sys.exit(1)
    
    doTests(db, BasiumTest)
