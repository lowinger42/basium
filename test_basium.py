#!/usr/bin/env python3
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

To run this script without embeded http/json server, start with --noserver
Example
    
    ./test_basium --noserver

A suitable standalone server can be started with

    export PYTHONPATH=/opt/basium
    wsgi/handler --port 8051


Preparation to database before testing:

    sqlite3
        No preparation is needed, included in python

    mysql
        create a database called basium_db
            CREATE DATABASE basium_db;

        create a user username basium_user with password secret, with full rights to the basium_db database
            GRANT ALL PRIVILEGES ON basium_db.* To 'basium_user'@'localhost' IDENTIFIED BY 'secret';

    psql
        create a user username basium_user with password secret, with full rights to the basium_db database
            sudo -u postgres createuser basium_user --pwprompt

        create a database called basium_db
            sudo -u postgres createdb basium_db --owner=basium_user

    json
        uses the psql driver on the server side, see psql


"""

import sys
import time
import decimal
import datetime
import unittest
import logging

import basium_common as bc
import basium
import basium_model
import basium_wsgihandler

import test_tables

# ----- Start of module globals

log = basium.log
log.info("Python version %s" % str(sys.version_info))

drivers = [
    "psql", 
    "mysql",
    "sqlite", 
    "json",
]

# ----- End of module globals


log.logger.setLevel(logging.ERROR)  # Keep the basium logger quiet


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
                val = datetime.date(year, month, day)
            elif isinstance(column, basium_model.DateTimeCol):
                year = 2012
                month = (p % 12) + 1
                day = (p % 28) + 1
                hour = (p % 24)
                minute = (p % 60)
                second = (p % 60)
                val = datetime.datetime(year, month, day, hour, minute, second)
            elif isinstance(column, basium_model.DecimalCol):
                val = decimal.Decimal("%d.%02d" % (p, p % 100))
            elif isinstance(column, basium_model.FloatCol):
                val = float(str(p) + '.' + str(p))
            elif isinstance(column, basium_model.IntegerCol):
                val = p
            elif isinstance(column, basium_model.VarcharCol):
                val = "text räksmörgås RÄKSMÖRGÅS" + str(p)
            else:
                print("Unknown column type: %s" % column)
                sys.exit(1)
            obj._values[colname] = val
        return obj

objFactory = ObjectFactory()


class TestFunctions(unittest.TestCase):
    """
    TestFunctions, test the store, load, delete, filter orm functions
    """
    def setUp(self):
        # Based on driver, create a dbconf object
        self.dbconf = None
        if self.driver == 'psql':
            self.dbconf = basium.DbConf(host='localhost', port=5432, username='basium_user', password='secret', database='basium_db')
        elif self.driver == 'mysql':
            self.dbconf = basium.DbConf(host='localhost', port=3306, username='basium_user', password='secret', database='basium_db')
        elif self.driver == 'sqlite':
            self.dbconf = basium.DbConf(database='/tmp/basium_db.sqlite')
        elif self.driver == 'json':
            self.dbconf = basium.DbConf(host='http://localhost:8051', username='basium_user', 
                               password='secret', database='basium_db')
        else:
            self.fail("Unknown driver %s" % self.driver)

        self.db = basium.Basium(driver=self.driver, dbconf=self.dbconf, checkTables=True) #, logger=logger)
        self.db.log.logger.setLevel(logging.ERROR)
        self.db.addClass(self.Cls)
        if not self.db.start():
            self.fail("Cannot start database driver")

    def runtest2(self, obj1, obj2):
        """
        Run test
         Create an object, store in db
         Read out an object with the _id from above
         Compare and see if the two are equal
        """
        log.info("Store object in table '%s'" % obj1._table)
        try:
            # data1 = self.db.store(obj1)
            self.db.store(obj1)
        except bc.Error as e:
            self.assertFalse(True, msg="Could not store object %s" % e)

        log.info("Load same object from table '%s'" % (obj1._table))
        obj2._id = obj1._id
        try:
            rows = self.db.load(obj2)
        except bc.Error as e:
            self.assertFalse(True, msg="Could not load object %s" % e)

        self.assertEqual( len(rows), 1, msg="Only expected one row in result, got %s" % len(rows))

        obj2 = rows[0]
        self.assertEqual(obj1, obj2, msg = "Stored and loaded object does not have same content")

        log.info("  There is a total of %i rows in the '%s' table" % (self.db.count(obj1), obj1._table ) )

    def testInsert(self):
        """
        Store an object, read it out again and compare if they are equal
        """
        obj1 = objFactory.new(self.Cls, 1)
        obj2 = self.Cls()
        self.runtest2(obj1, obj2)

        obj3 = objFactory.new(self.Cls, 2)
        obj4 = self.Cls()
        self.runtest2(obj3, obj4)

    def testUpdate(self):
        """
        Test the update functionality
        """
        test1 = objFactory.new(self.Cls, 1)
        try:
            data = self.db.store(test1)
        except bc.Error as e:
            self.assertFalse(True, msg="Can't store new object %s" % e)

        test1.varcharTest += " more text"
        try:
            _id = self.db.store(test1)
        except bc.Error as e:
            self.assertFalse(True, msg="Can't update object %s" % e)

        test2 = self.Cls(_id)
        try:
            data = self.db.load(test2)
        except bc.Error as e:
            self.assertFalse(True, msg="Can't load updated object %s" % e)

        test2 = data[0]
        self.assertEqual(test1.varcharTest, test2.varcharTest, msg=
            "Update failed, expected '%s' in field, got '%s'" % (test1.varcharTest, test2.varcharTest))

    def testQuery(self):
        """
        Test the query functionality
        """
        # first create the objects in the database
        first = None
        for rowid in range(100, 115):
            obj1 = objFactory.new(self.Cls, rowid)
            try:
                data = self.db.store(obj1)
            except bc.Error as e:
                self.assertFalse(True, msg="Could not store object %s" % e)
            if not first:
                first = obj1._id

        query = self.db.query()
        obj = self.Cls()
        query.filter(obj.q._id, '>', first + 2).filter(obj.q._id, '<', first + 13)
        try:
            data = self.db.load(query)
        except bc.Error as e:
            self.assertFalse(True, msg="Can't query objects %s" % e)

        self.assertEqual(len(data), 10, msg="Wrong number of objects returned, expected %s got %s" % (10, len(data)))
        if len(data) == 10:
            for i in range(0, 10):
                self.assertEqual(data[i].intTest, i+103)

    def testDelete(self):
        """
        Test the delete functionality
        """
        test1 = objFactory.new(self.Cls, 1)
        log.info("Store object in table '%s'" % test1._table)
        try:
            # _id = self.db.store(test1)
            self.db.store(test1)
        except bc.Error as e:
            self.assertFalse(True, msg="Can't store new object %s" % e)

        rowsaffected = None
        log.info("Delete object in table '%s'" % test1._table)
        try:
            rowsaffected = self.db.delete(test1)
        except bc.Error as e:
            self.assertFalse(True, msg="Can't delete object %s" % e)

        self.assertEqual(rowsaffected, 1)

        # Try to get the object we just deleted
        log.info("Trying to get deleted object in table '%s' (should fail)" % test1._table)
        test2 = self.Cls()
        try:
            # data = self.db.load(test2)
            self.db.load(test2)
        except bc.Error as e:
            self.assertTrue(True, msg="Expected error when loading deleted object %s" % e)


class TestModel(unittest.TestCase):
    """
    Test the ORM model class
    """

    class TestModel(basium_model.Model):
        booleanTest = basium_model.BooleanCol()
        dateTest = basium_model.DateCol()
        datetimeTest = basium_model.DateTimeCol()
        decimalTest = basium_model.DecimalCol()
        floatTest = basium_model.FloatCol()
        intTest = basium_model.IntegerCol()
        varcharTest = basium_model.VarcharCol()

    class TestModelDefault(basium_model.Model):
        booleanTest = basium_model.BooleanCol(default=True)
        dateTest = basium_model.DateCol(default="NOW")
        datetimeTest = basium_model.DateTimeCol(default="NOW")
        decimalTest = basium_model.DecimalCol(default=decimal.Decimal("1.23"))
        floatTest = basium_model.FloatCol(default=2.78)
        intTest = basium_model.IntegerCol(default=42)
        varcharTest = basium_model.VarcharCol(default="default string")

    def setUp(self):
        pass

    def test(self):
        t = self.TestModel()
        self.assertEqual(t.booleanTest, None)
        self.assertEqual(t.dateTest, None)
        self.assertEqual(t.datetimeTest, None)
        self.assertEqual(t.decimalTest, None)
        self.assertEqual(t.floatTest, None)
        self.assertEqual(t.intTest, None)
        self.assertEqual(t.varcharTest, None)

    def testDefault(self):
        t = self.TestModelDefault()
        self.assertEqual(t.booleanTest, True)
        self.assertEqual(t.dateTest, datetime.datetime.now().date())
        self.assertEqual(t.datetimeTest, datetime.datetime.now().replace(microsecond=0))
        self.assertEqual(t.decimalTest, decimal.Decimal("1.23"))
        self.assertEqual(t.floatTest, 2.78)
        self.assertEqual(t.intTest, 42)
        self.assertEqual(t.varcharTest, "default string")


def get_suite():
    """
    Return a testsuite with this modules all tests
    """
    suite = unittest.TestSuite()
    testloader = unittest.TestLoader()

    suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestModel))

    for driver in drivers:
        testnames = testloader.getTestCaseNames(TestFunctions)
        for name in testnames:
            t = TestFunctions(name)
            setattr(t, "driver", driver)
            setattr(t, "Cls", test_tables.BasiumTest)
            suite.addTest(t)

    return suite


def runServer():
    """
    Start an WSGI server as a separate thread,
    needed for the json driver test
    """
    log.info("Starting embedded WSGI server")

    driver = "psql"
    dbconf = basium.DbConf(host='localhost', port=5432, username='basium_user', password='secret', database='basium_db')
    db = basium.Basium(driver=driver, dbconf=dbconf, checkTables=True)
    db.setDebug(bc.DEBUG_ALL)
    db.log.logger.setLevel(logging.ERROR)
    db.addClass(test_tables.BasiumTest)
    if not db.start():
        log.error("Cannot start database driver for wsgi server")
    
    server = basium_wsgihandler.Server(basium=db)
    server.daemon = True
    server.start()    # run in thread
    while not server.ready:
        time.sleep(0.1)


if __name__ == "__main__":
    if "json" in drivers:
        runServer()
        
    suite = get_suite()
    runner = unittest.TextTestRunner()
    runner.run(suite)
