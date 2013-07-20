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
Unit testing of the object persistence code

Uses any supported driver to communicate directly with the database

to test postgresql driver:
    create database, user, fix permissions
        su - postgres
        psql 
        create user basium_user with password 'secret';
        create database basium_db;
        grant all privileges on database basium_db to basium_user;
        \q
    
    enable md5 (in addition of ident) for localhost
        fedora 18
        edit /var/lib/pgsql/data/pg_hba.conf
        
        under 
            host    all             postgres             127.0.0.1/32            ident
        add
            host    all             all                  127.0.0.1/32            md5

        then restart mysql

    test created user
        psql -U basium_user -W 

to test mysql driver
    create database, user, fix permissions
    
        create database basium_db;
        grant all on basium_db.* to basium_user@localhost identified by 'secret';
        flush privileges;

    test created user
        mysql -u basium_user -p basium_db


run with python2
    python2 test_db.py
    
run with python3
    python3 test_db.py
"""

from __future__ import print_function
from __future__ import unicode_literals
__metaclass__ = type

import basium
import test_tables
import test_util

log = basium.log

if __name__ == "__main__":

    drivers = ["psql", "mysql", "sqlite"]

    for driver in drivers:
        log.info(">>> Testing database driver %s" % driver)
        dbconf, bas = test_util.getDbConf(driver, checkTables=True)
        test_util.doTests(bas, test_tables.BasiumTest)
        print()
        print()
