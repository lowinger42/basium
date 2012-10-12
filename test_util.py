#!/usr/bin/env python

#
# Common code used during unit testing
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

import pprint
import decimal
from inspect import getmembers

import basium_orm
import basium_common
from basium_model import *

# ----- Module globals
log = basium_common.log
errcount = 0


#
# Create a new object and initialize the columns with decent mostly
# unique values that can be used during testing
#
class ObjectFactory(object):
    
    def __init__(self):
        pass
    
    def new(self, cls, p):
        obj = cls()
        for colname, column in obj._columns.items():
            if column.primary_key:
                continue
            val = None
            if isinstance(column, BooleanCol):
                val = (p & 1) == 0
            elif isinstance(column, DateCol):
                year = 2012
                month = (p % 12) + 1
                day = (p % 28) + 1 
                val = datetime.date(year,month,day)
            elif isinstance(column, DateTimeCol):
                year = 2012
                month = (p % 12) + 1
                day = (p % 28) + 1 
                hour = (p % 24)
                minute = (p % 60)
                second = (p % 60)
                val = datetime.datetime(year,month,day,hour,minute,second)
            elif isinstance(column, DecimalCol):
                l = p / 100
                r = p % 100
                val = decimal.Decimal( str(l) + '.' + str(r) )
            elif isinstance(column, FloatCol):
                val = float(str(p) + '.' + str(p))
            elif isinstance(column, IntegerCol):
                val = p
            elif isinstance(column, VarcharCol):
                val = 'text ' + str(p)
            else:
                log.error('Unknown column type: %s' % column)
                sys.exit(1)
#            obj.set(colname, val)
            obj._values[colname] = val
        
        return obj
    

#
#
#
def logHeader(text):
    log.info("##### %s #####" % (text))


#
# Run test
#  Create an object, store in db
#  Read out an object with the id from above
#  Compare and see if the two are equal
#
class RunTest1():
    
    def __init__(self):
        pass


    #
    #
    #    
    def run(self, db, obj1, obj2):
        global errcount

        log.info("Store object in table '%s'" % obj1._table )
        db.store(obj1)
        log.info("Load same object from table '%s'" % (obj1._table) )
        obj2.id = obj1.id
        response = db.load(obj2)
        if not response.isError():
            rows = response.get('data')
            if len(rows) == 1:
                obj2 = rows[0]
                if obj1 == obj2:
                    log.info("  Ok: Same content!")
                else:
                    log.error("  Error: Not same content")
                    errcount += 1
            else:
                log.error("  Error: expected one object returned, got %d" % (len(rows)) )
                errcount += 1
        else:
            log.error( response.getError() )
            errcount += 1
    
        log.info("  There is a total of %i rows in the '%s' table" % (db.count(obj1), obj1._table ) )


#
# Store an object, read it out again and compare if they are equal
#
def test1(db, runtest, Cls):

    logHeader('Test of %s, store/load' % (Cls.__name__))
    objFactory = ObjectFactory()
    
    #
    test1 = objFactory.new( Cls, 1 )
    test2 = Cls()
    
    runtest.run(db, test1, test2)

    #    
    test3 = objFactory.new( Cls, 2 )
    test4 = Cls()
    
    runtest.run(db, test3, test4)


#
# Test the query functionality
#  
def test2(db, runtest, Cls):
    logHeader('Test of %s, query' % (Cls.__name__))

    query = basium_orm.Query( db, Cls )
    query.filter('id', '>', '10').filter('id', '<', '13')
    response = db.load(query)
    if response.isError():
        log.error( response.getError() )
        errcount += 1
        return
    
    data = response.get('data')
    for obj in data:
        log.info("%s %s" % ( obj.id, obj.varcharTest ) )
    log.info("Found %i objects" % len(data) )


#
# Test the update functionality
#  
def testUpdate(db, runtest, Cls):
    logHeader('Test of %s, update' % (Cls.__name__))

    objFactory = ObjectFactory()
    
    #
    test1 = objFactory.new( Cls, 1 )
    res = db.store(test1)
    if res.isError():
        errcount += 1
        log.error( res.getError() )
        return
    
    test1.varcharTest += " more text"
    res = db.store(test1)
    if res.isError():
        errcount += 1
        log.error( res.getError() )
        return
    
    test2 = Cls()
    test2.id = test1.id
    res = db.load(test2)
    if res.isError():
        errcount += 1
        log.error( res.getError() )
        return
    test2 = res.get('data')[0]
    if test1.varcharTest != test2.varcharTest:
        log.error( "Update failed, expected '%s' in field, got '%s'" % (test1.varcharTest, test2.varcharTest) )


#
# Main testrunner
#
def doTests(db, Cls):
    runtest1 = RunTest1()

    test1(db, runtest1, Cls)

    test2(db, runtest1, Cls)

    testUpdate(db, runtest1, Cls)

    log.info( "All done, a total of %i errors" % errcount )

#
#
#
if __name__ == "__main__":
    print "This is a library, there is nothing to run"
