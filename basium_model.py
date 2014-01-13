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
Model classes, for each SQL datatype

Metaclass, that initalizes each instance of a Model class
"""

from __future__ import absolute_import, division, print_function, unicode_literals
__metaclass__ = type

import inspect
import pprint

import basium_compatibilty as c

class Column:
    """Base class for all different column types"""

    def getDefault(self):
        return self.default

class BooleanCol(Column):

    def __init__(self, primary_key=False, nullable=True, default=None):
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default

class DateCol(Column):
    """Stores a date"""
    def __init__(self, primary_key=False, nullable=False, default=None):
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default

class DateTimeCol(Column):
    """
    Stores date+time
    ignores microseconds
    if default is 'NOW' the current date+time is stored
    """
    
    def __init__(self, primary_key=False, nullable=True, default=None):
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default

class DecimalCol(Column):
    """
    Stores a fixed precision number
    we cheat and represent this as a float in python
    """
    def __init__(self, primary_key=False, nullable=True, default=None, maxdigits=5, decimal=2):
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default
        self.maxdigits = maxdigits
        self.decimal = decimal

class FloatCol(Column):
    """Stores a floating point number"""
    def __init__(self, primary_key=False, nullable=True, default=None):
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default
    
class IntegerCol(Column):
    """Stores an integer"""
    def __init__(self, primary_key=False, nullable=True, default=None, length=11):
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default
        self.length = length

class VarcharCol(Column):
    """Stores a string"""
    def __init__(self, primary_key=False, nullable=True, default=None, length=255):
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default
        self.length = length


class Q:
    pass
    
class ModelMetaClass(type):
    """Metaclass that helps constructing the classes that should be persisted"""

    def __init__(cls, name, bases, dct):
        super(ModelMetaClass, cls).__init__(name, bases, dct)
        cls._primary_key = ['_id']
        cls._table = name.lower()
        cls._columns = {}
        cls._values = {}


# handle python 2 & 3
ModelMetaClass2 = ModelMetaClass(c.b("ModelMetaClass2"), (object, ), {})
               
class Model(ModelMetaClass2):
    """Base class for all classes that should be persistable"""
    __metaclass__ = ModelMetaClass

    def __init__(self, id = -1):
        _id = IntegerCol(primary_key=True)
        _id._model = self
        _id.name = c.b('_id')
        columns = { c.b('_id'): _id }
        values = { c.b('_id'): id }
        
        # create instance variables of the class columns
        q = Q()
        q._id = _id
        for colname, column in inspect.getmembers(self):
            if colname[0] != "_" and isinstance(column, Column):
                column.name = colname
                column._model = self  # backpointer from column to model class
                columns[colname] = column
                values[colname] = column.getDefault()
                setattr(q, colname, column)
        object.__setattr__(self, '_columns', columns)
        object.__setattr__(self, '_values', values)
        object.__setattr__(self, 'q', q)
    
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
        if other == None:
            return False
        for colname in self._iterName():
            if colname != '_id':
                if getattr(self, colname) != getattr(other, colname):
                    return False
        return True

    def get(self, attr):
        return self.__getattribute__(attr)

    def set(self, attr, value):
        self.__setattr__(attr, value)

    def getValues(self):
        """return all columns as a dictionary, data presented in sql format"""
        res = {}
        for colname, column in self._iterNameColumn():
            res[colname] = column.toSql(self._values[colname])
        return res
    
    def getStrValues(self):
        """return all columns as a dictionary, data presented as strings"""
        res = {}
        for colname in self._iterName():
            res[colname] = str(self._values[colname])
        return res

    def isPrimaryKey(self, pkey):
        if pkey != None:
            if self._primary_key != None:
                return pkey in self._primary_key
        return False
    
    def _iterName(self):
        for colname in self._columns.keys():
            yield colname
            
    def _iterNameColumn(self):
        for colname,column in self._columns.items():
            yield colname, column
