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
Common code used for testing
"""

from __future__ import print_function
from __future__ import unicode_literals
__metaclass__ = type

import sys
import decimal
import datetime
import traceback

import basium
import basium_model
import test_tables

# ----- Start of module globals

log = basium.log
log.info("Python version %s" % str(sys.version_info))
errcount = 0
drivers = ["psql", "mysql", "sqlite"]

# ----- End of module globals


def fatal(msg=None):
    if msg:
        log.error("Fatal %s" % msg)
    traceback.print_exc()
    sys.exit(1)



def getDbConf(driver, logger=None, checkTables=False):
    """Based on driver, return a dbconf object, used in function test"""
    if driver == 'psql':
        dbconf = basium.DbConf(host='localhost', port=5432, username='basium_user', password='secret', database='basium_db')
    elif driver == 'mysql':
        dbconf = basium.DbConf(host='localhost', port=3306, username='basium_user', password='secret', database='basium_db')
    elif driver == 'sqlite':
        dbconf = basium.DbConf(database='/tmp/basium_db.sqlite')
    elif driver == 'json':
        dbconf = basium.DbConf(host='http://localhost:8051', username='basium_user', 
                           password='secret', database='basium_db')
    else:
        fatal("Unknown driver %s" % driver)

    bas = basium.Basium(driver=driver, dbconf=dbconf, checkTables=checkTables, logger=logger)
    bas.addClass(test_tables.BasiumTest)
    if not bas.start():
        return None, None
    return dbconf, bas
    
class ObjectFactory:
    """
    Create a new object and initialize the columns with decent mostly
    unique values that can be used during testing
    """
    
    def __init__(self):
        pass
    
    def new(self, cls, p):
        obj = cls()
        for colname, column in obj._iterNameColumn():
            if column.primary_key:
                continue
            val = None
            if isinstance(column, basium_model.BooleanCol):
                val = (p & 1) == 0
            elif isinstance(column, basium_model.DateCol):
                year = 2012
                month = (p % 12) + 1
                day = (p % 28) + 1 
                val = datetime.date(year,month,day)
            elif isinstance(column, basium_model.DateTimeCol):
                year = 2012
                month = (p % 12) + 1
                day = (p % 28) + 1 
                hour = (p % 24)
                minute = (p % 60)
                second = (p % 60)
                val = datetime.datetime(year,month,day,hour,minute,second)
            elif isinstance(column, basium_model.DecimalCol):
                val = decimal.Decimal( "%d.%02d" % ( p, p % 100 ) )
            elif isinstance(column, basium_model.FloatCol):
                val = float(str(p) + '.' + str(p))
            elif isinstance(column, basium_model.IntegerCol):
                val = p
            elif isinstance(column, basium_model.VarcharCol):
                val = 'text ' + str(p)
            else:
                fatal('Unknown column type: %s' % column)
            obj._values[colname] = val
        
        return obj
    
def logHeader(text):
    log.info("---------- %s ----------" % (text))


class RunTest1:
    """
    Run test
     Create an object, store in db
     Read out an object with the _id from above
     Compare and see if the two are equal
    """
    
    def __init__(self):
        pass

    def run(self, bas, obj1, obj2):
        global errcount

        log.info("Store object in table '%s'" % obj1._table )
        response1 = bas.store(obj1)
        if response1.isError():
            log.error('Could not store object')
            errcount += 1
            return
        
        log.info("Load same object from table '%s'" % (obj1._table) )
        obj2._id = obj1._id
        response2 = bas.load(obj2)
        
        if not response2.isError():
            rows = response2.data
            if len(rows) == 1:
                obj2 = rows[0]
                if obj1 == obj2:
                    log.info("  Ok: Same content!")
                else:
                    log.error("  Error: Not same content")
                    errcount += 1
            else:
                log.error("  Error: expected one object returned, got %d objects" % (len(rows)) )
                errcount += 1
        else:
            log.error( response2.getError() )
            errcount += 1
    
        log.info("  There is a total of %i rows in the '%s' table" % (bas.count(obj1), obj1._table ) )

def test1(bas, runtest, Cls):
    """Store an object, read it out again and compare if they are equal"""

    logHeader('Test of %s, store/load' % (Cls.__name__))
    objFactory = ObjectFactory()
    
    #
    obj1 = objFactory.new( Cls, 1 )
    obj2 = Cls()
    runtest.run(bas, obj1, obj2)

    #    
    obj3 = objFactory.new( Cls, 2 )
    obj4 = Cls()
    runtest.run(bas, obj3, obj4)

def test2(bas, Cls):
    """Test the query functionality"""
    global errcount
    logHeader('Test of %s, query' % (Cls.__name__))

    query = bas.query()
    obj = Cls()
    query.filter(obj.q._id, '>', 10).filter(obj.q._id, '<', 20)
    response = bas.load(query)
    if response.isError():
        log.error( response.getError() )
        errcount += 1
        return
    
    data = response.data
    for obj in data:
        log.info("%s %s" % ( obj._id, obj.varcharTest ) )
    log.info("Found %i objects" % len(data) )


def testUpdate(bas, Cls):
    """Test the update functionality"""
    global errcount

    logHeader('Test of %s, update' % (Cls.__name__))

    objFactory = ObjectFactory()
    
    #
    test1 = objFactory.new( Cls, 1 )
    res = bas.store(test1)
    if res.isError():
        errcount += 1
        log.error( res.getError() )
        return
    
    test1.varcharTest += " more text"
    res = bas.store(test1)
    if res.isError():
        errcount += 1
        log.error( res.getError() )
        return
    
    test2 = Cls()
    test2._id = test1._id
    res = bas.load(test2)
    if res.isError():
        errcount += 1
        log.error( res.getError() )
        return
    test2 = res.data[0]
    if test1.varcharTest != test2.varcharTest:
        log.error( "Update failed, expected '%s' in field, got '%s'" % (test1.varcharTest, test2.varcharTest) )


def testDelete(bas, Cls):
    global errcount

    logHeader('Test of %s, delete' % (Cls.__name__))

    objFactory = ObjectFactory()
    
    #
    test1 = objFactory.new( Cls, 1 )
    log.info("Store object in table '%s'" % test1._table )
    res = bas.store(test1)
    if res.isError():
        errcount += 1
        log.error( res.getError() )
        return
    _id = test1._id

    log.info("Delete object in table '%s'" % test1._table )
    res = bas.delete(test1)
    if res.isError():
        errcount += 1
        log.error( res.getError() )
        return
    rowsaffected = res.data
    if rowsaffected != 1:
        errcount += 1
        log.error("Expected delete to affect one row, %s got affected" % rowsaffected)
        return

    # Try to get the object we just deleted        
    log.info("Trying to get deleted object in table '%s' (should fail)" % test1._table )
    test2 = Cls()
    test2._id = _id
    res = bas.load(test2)
    if not res.isError():
        errcount += 1
        log.error( res.getError() )
        return

def doTests(bas, Cls):
    """main testrunner"""
    runtest1 = RunTest1()

    test1(bas, runtest1, Cls)

    test2(bas, Cls)

    testUpdate(bas, Cls)
    
    testDelete(bas, Cls)

    log.info( "All done, a total of %i errors" % errcount )

if __name__ == "__main__":
    print("This is a library, there is nothing to run")
