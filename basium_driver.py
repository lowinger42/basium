#! /usr/bin/env python

# ----------------------------------------------------------------------------
#
# Basium base class for all driver implementations
#
# ----------------------------------------------------------------------------

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

import datetime
import urllib
import urllib2
import urlparse
import json
import decimal

import basium_common
# from basium_model import *

log = basium_common.log


#
# These are shadow classes from the basium_model
# handles the database specific functions such
# as converting to/from SQL types
#
# No __init__(), these classes/methods are inserted during runtime
# into the basium_model classes
#

class Column(object):

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
 
# stores a date
class DateCol(Column):
    
    def toPython(self, value):
        if isinstance(value, datetime.datetime):
            value = value.date()
        elif isinstance(value, basestring):
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S').date()
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return value

# stores date+time
# ignores microseconds
# if default is 'NOW' the current date+time is stored
class DateTimeCol(Column):
    
    def getDefault(self):
        if self.default == 'NOW':
            return datetime.datetime.now().replace(microsecond=0)
        return self.default

    def toPython(self, value):
        if isinstance(value, basestring):
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        return value

# stores a fixed precision number
class DecimalCol(Column):
    
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

# stores a floating point number
class FloatCol(Column):
    
    def toPython(self, value):
        if isinstance(value, basestring):
            value = float(value)
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return str(value)

# stores an integer
class IntegerCol(Column):
    
    def toPython(self, value):
        if isinstance(value, basestring):
            value = int(value)
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return value

# stores a string
class VarcharCol(Column):

    def toPython(self, value):
        if isinstance(value, unicode):
            value = str(value)
        return value


# ----------------------------------------------------------------------------
#
#  Mostly stubs, needs to be overridden by the implementation specific driver
#
# ----------------------------------------------------------------------------
class Driver:
#    def __init__(self, host=None, port=None, username=None, password=None, name=None, debugSql=False):
#        self.host = host
#        self.port = port
#        self.username = username
#        self.password = password
#        self.name = name
#        self.debugSql = debugSql

    def connect(self):
        return basium_common.Response()

    def execute(self, method=None, url=None, data=None, decode=False):
        return basium_common.Response(1, 'Not implemented')

    def isDatabase(self, dbName):
        response = basium_common.Response()
        response.set('data', True)
        return response

    def isTable(self, tableName):
        response = basium_common.Response()
        response.set('data', True)
        return response

    def createTable(self, obj):
        response = basium_common.Response()
        return response

    def verifyTable(self, obj):
        response = basium_common.Response()
        response.set('actions', [])
        return response
    
    def modifyTable(self, obj, actions):
        return True

    def count(self, query):
        return basium_common.Response(1, 'Not implemented')
    
    def select(self, query):
        return basium_common.Response(1, 'Not implemented')

    def insert(self, table, values):
        return basium_common.Response(1, 'Not implemented')

    def update(self, table, values):
        return basium_common.Response(1, 'Not implemented')

    def delete(self, query):
        return basium_common.Response(1, 'Not implemented')
