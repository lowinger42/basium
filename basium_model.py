#! /usr/bin/env python

#
# Object persistence for MySQL
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

import sys
import inspect
import datetime
import pprint
import types
import decimal

# import MySQLdb
# import urllib
# import urllib2
# import urlparse
# import json

# import basium_common


#
# Base class for all different column types
#
class Column(object):

    value = None

    def getDefault(self):
        return self.default

    def set(self, value):
        self.value = value

    def toPython(self, value):
        return value

    def toSql(self, value):
        return value
    
    
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


# stores boolean as number: 0 or 1
class BooleanCol(Column):
    def __init__(self, primary_key=False, nullable=True, default=None):
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default

    def typeToSql(self):
        sql = "tinyint(1)"
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            if self.default:
                sql += " default 1"
            else:
                sql += " default 0"
        return sql

    def toPython(self, value):
        return value == 1

    def toSql(self, value):
        if value == None:
            return "NULL"
        if value:
            return 1
        return 0
        

# stores a date
class DateCol(Column):
    def __init__(self, primary_key=False, nullable=False, default=None):
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default

    def typeToSql(self):
        sql = "date"
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            sql += " default %s" % self.default
        return sql

    def toPython(self, value):
        if isinstance(value, datetime.datetime):
            value = value.date()
        elif isinstance(value, basestring):
            print value
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
    
    def __init__(self, primary_key=False, nullable=True, default=None):
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default

    def getDefault(self):
        if self.default == 'NOW':
            return datetime.datetime.now().replace(microsecond=0)
        return self.default

    def typeToSql(self):
        sql = 'datetime'
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None and self.default != 'NOW':
            sql += " default %s" % self.default
        return sql

    def toPython(self, value):
        if isinstance(value, basestring):
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        return value


# stores a fixed precision number
# we cheat and represent this as a float in python
class DecimalCol(Column):
    def __init__(self, primary_key=False, nullable=True, default=None, maxdigits=5, decimal=2):
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default
        self.maxdigits = maxdigits
        self.decimal = decimal

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
    def __init__(self, primary_key=False, nullable=True, default=None):
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default

    def typeToSql(self):
        sql = "float"
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            sql += " default %s" % str(self.default)
        return sql

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
    def __init__(self, primary_key=False, nullable=True, default=None, length=11):
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default
        self.length = length

    def typeToSql(self):
        if self.primary_key:
            return "serial";
        sql = 'int(%s)' % self.length
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            if self.default: 
                sql += " default %i" % self.default
        return sql

    def toPython(self, value):
        if isinstance(value, basestring):
            print value
            value = int(value)
        return value
        
    def toSql(self, value):
        if value == None:
            return "NULL"
        return value


# stores a string
class VarcharCol(Column):
    def __init__(self, primary_key=False, nullable=True, default=None, length=255):
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default
        self.length = length

    def typeToSql(self):
        sql = 'varchar(%d)' % self.length
        if self.nullable:
            sql += " null"
        else:
            sql += " not null" 
        if self.default != None:
            if self.default != '':
                sql += " default '%s'" % self.default
        return sql

    def toPython(self, value):
        if isinstance(value, unicode):
            value = str(value)
        return value


#
# This metaclass helps constructing the classes that should be persisted
#
class ModelMetaClass(type):

#    def __new__(cls, name, bases, dct):
#        return super(ModelMetaClass, cls).__new__(cls, name, bases, dct)

    def __init__(cls, name, bases, dct):
        super(ModelMetaClass, cls).__init__(name, bases, dct)
        setattr(cls, '_primary_key', [ 'id' ])
        setattr(cls, '_table', name.lower())
        columns = { 'id': IntegerCol(primary_key=True, default=-1) }
        values = { 'id': -1 }
#        for (colname, column) in inspect.getmembers(cls):
#            if isinstance(column, Column):
#                column.name = colname
#                columns[colname] = column
#                values[colname] = column.getDefault()
        setattr(cls, '_columns', columns)
        setattr(cls, '_values', values)

                
#
# Base class for all classes that should be persistable
#
class Model(object):
    __metaclass__ = ModelMetaClass
    
    def __init__(self):
        # print "name =", self.__class__.__name__
        object.__setattr__(self, '_primary_key', [ 'id' ])
        columns = { 'id': IntegerCol(primary_key=True, default=-1) }
        values = { 'id': -1 }
        # create instance variables of the class columns
        for (colname, column) in inspect.getmembers(self):
            if colname[0] != '_' and isinstance(column, Column):
                column.name = colname
                columns[colname] = column
                values[colname] = column.getDefault()
        object.__setattr__(self, '_columns', columns)
        object.__setattr__(self, '_values', values)
    
    def __setattr__(self, attr, value):
        if attr in self._columns:
            self._values[attr] = value
        else:
            object.__setattr__(self, attr, value)

    def __getattribute__(self, attr):
        if attr in object.__getattribute__(self, '_values'):
            return object.__getattribute__(self, '_values')[attr]
        else:
            return object.__getattribute__(self, attr)

    def __str__(self):
        return pprint.pformat(self.getValues(), indent=4)

    def __eq__(self, other):
        for colname in self._columns.keys():
#        for (colname, col) in self._columns.items():
            if colname != '_':
                if getattr(self, colname) != getattr(other, colname):
                    return False
        return True

    def get(self, attr):
        return self.__getattribute__(attr)

    def set(self, attr, value):
        self.__setattr__(attr, value)

    # return all columns as a dictionary, data presented in sql format
    def getValues(self):
        res = {}
        for colname, column in self._columns.items():
            res[colname] = column.toSql(self._values[colname])
        return res
    
    # return all columns as a dictionary, data presented as strings
    def getStrValues(self):
        res = {}
        for colname in self._columns.keys():
            res[colname] = str(self._values[colname])
        return res

    def isPrimaryKey(self, pkey):
        if pkey != None:
            if self._primary_key != None:
                return pkey in self._primary_key
        return False


#
# Future tests
#
if __name__ == "__main__":
    pass
