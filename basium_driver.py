#! /usr/bin/env python
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
#

"""
Basium base class for all driver implementations
"""

from __future__ import absolute_import, division, print_function, unicode_literals
__metaclass__ = type

import datetime
import decimal

import basium_compatibilty as c


#
# These are shadow classes from the basium_model
# handles the database specific functions such
# as converting to/from SQL types
#
# No __init__(), these classes/methods are inserted during runtime
# into the basium_model classes
#

class Column:

    def toPython(self, value):
        return value

    def toSql(self, value):
        return value

    # todo: this is mysql specific, fix!
    # Convert a 'describe table' to sqltype
    #
    #    from "describe <table"
    #    {   'Default': 'CURRENT_TIMESTAMP',
    #        'Extra': 'on update CURRENT_TIMESTAMP',
    #        'Field': 'start',
    #        'Key': '',
    #        'Null': 'NO',
    #        'Type': 'timestamp'},
    #
    def tableTypeToSql(self, tabletype):
        if  tabletype['Key'] == 'PRI':
            tmp = 'serial'
        else:
            tmp = tabletype['Type']
            if tabletype['Null'] == 'NO':
                tmp += " not null"
            else:
                tmp += " null"
        return tmp


class BooleanCol(Column):

    def toPython(self, value):
        return value == 1

    def toSql(self, value):
        if value == None:
            return "NULL"
        if value:
            return 'TRUE'
        return 'FALSE'
 
class DateCol(Column):
    """Stores a date"""
    
    def toPython(self, value):
        if isinstance(value, datetime.datetime):
            value = value.date()
        elif c.isstring(value):
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S').date()
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return value

class DateTimeCol(Column):
    """
    stores date+time
    ignores microseconds
    if default is 'NOW' the current date+time is stored
    """
    
    def getDefault(self):
        if self.default == 'NOW':
            return datetime.datetime.now().replace(microsecond=0)
        return self.default

    def toPython(self, value):
        if c.isstring(value):
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        return value

class DecimalCol(Column):
    """Stores a fixed precision number"""
    
    def typeToSql(self):
        sql = 'decimal(%d,%d)' % (self.maxdigits, self.decimal)
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            sql += " default '%s'" % str(self.default)
        return sql

    def toPython(self, value):
        if value == None:
            return None
        if isinstance(value, decimal.Decimal):
            return value
        return decimal.Decimal(value)
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return value

class FloatCol(Column):
    """Stores a floating point number"""
    
    def toPython(self, value):
        if c.isstring(value):
            value = float(value)
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return str(value)

class IntegerCol(Column):
    """Stores an integer"""
    
    def toPython(self, value):
        if c.isstring(value):
            value = int(value)
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return value

class VarcharCol(Column):
    """Stores a string"""
    def toPython(self, value):
        if c.isunicode(value):
            return value
        try:
            return c.to_unicode(value)
        except:
            return value


class Driver:
    """
    Driver base class, Mostly stubs, needs to be overridden by the specific driver
    """

    def connect(self):
        raise c.Error(1, 'Not implemented')

    def execute(self, method=None, url=None, data=None, decode=False):
        raise c.Error(1, 'Not implemented')

    def isDatabase(self, dbName):
        return True

    def isTable(self, tableName):
        return True

    def createTable(self, obj):
        return True

    def verifyTable(self, obj):
        return []
    
    def modifyTable(self, obj, actions):
        return True

    def count(self, query):
        raise c.Error(1, 'Not implemented')
    
    def select(self, query):
        raise c.Error(1, "Not implemented")

    def insert(self, table, values):
        raise c.Error(1, 'Not implemented')

    def update(self, table, values):
        raise c.Error(1, 'Not implemented')

    def delete(self, query):
        raise c.Error(1, 'Not implemented')
