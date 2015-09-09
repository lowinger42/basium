#! /usr/bin/env python3
# -*- coding: utf-8 -*-

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

"""
Basium database driver that handles remote JSON server

the toSql/toPython methods are different compared to a standard SQL driver
toSQL, converts data so it can be sent from client->server
toPython, convert in the server from wire format->python format

api.py is the code running on the server, that handles all requests from
this driver
"""

import datetime
import decimal
import urllib
import urllib.request
import base64
import json

import basium_common as bc
import basium_driver

#
# These are shadow classes from the basium_model
# handles the database specific functions such
# as converting to/from SQL types
#
# The JSON driver handles toSql differently compared to a standard sql driver,
# it converts to string and utf-8 encodes the data, so it can be sent in a
# HTTP POST/PUT message
#


class BooleanCol(basium_driver.BooleanCol):
    """
    stores a boolean
    """

    @classmethod
    def toPython(self, value):
        if isinstance(value, str):
            return value.lower() == "true"
        return value

    def toSql(self, value):
        if value is None:
            return "NULL"
        if value:
            return "True"
        return "False"


class DateCol(basium_driver.DateCol):
    """
    stores a date
    """

    @classmethod
    def toPython(self, value):
        if isinstance(value, datetime.datetime):
            value = value.date()
        if isinstance(value, str):
            value = datetime.datetime.strptime(value[:10], '%Y-%m-%d').date()
        return value

    def toSql(self, value):
        if value is None:
            return "NULL"
        return str(value)


class DateTimeCol(basium_driver.DateTimeCol):
    """
    stores date+time, ignores microseconds
    """

    @classmethod
    def toPython(self, value):
        if value == "NULL":
            return None
        if isinstance(value, str):
            value = datetime.datetime.strptime(value, '%Y-%m-%d %H:%M:%S')
        return value

    def toSql(self, value):
        if value is None:
            return "NULL"
        return value.strftime('%Y-%m-%d %H:%M:%S')


# stores a fixed precision number
class DecimalCol(basium_driver.DecimalCol):

    def typeToSql(self):
        sql = 'decimal(%d,%d)' % (self.maxdigits, self.decimal)
        if self.nullable:
            sql += " null"
        else:
            sql += " not null"
        if self.default is not None:
            sql += " default '%s'" % str(self.default)
        return sql

    @classmethod
    def toPython(self, value):
        if value is None:
            return None
        if isinstance(value, decimal.Decimal):
            return value
        return decimal.Decimal(value)

    def toSql(self, value):
        if value is None:
            return "NULL"
        return str(value)


class FloatCol(basium_driver.FloatCol):
    """
    stores a floating point number
    """

    @classmethod
    def toPython(self, value):
        if isinstance(value, str):
            value = float(value)
        return value

    def toSql(self, value):
        if value is None:
            return "NULL"
        return str(value)


class IntegerCol(basium_driver.IntegerCol):
    """
    stores an integer
    """

    @classmethod
    def toPython(self, value):
        if isinstance(value, str):
            value = int(value)
        return value

    def toSql(self, value):
        if value is None:
            return "NULL"
        return str(value)


class VarcharCol(basium_driver.VarcharCol):
    """
    stores a string
    """

    @classmethod
    def toPython(self, value):
        if isinstance(value, str):
            return value
        try:
            return str(value)
        except:
            return value

    def toSql(self, value):
        if value is None:
            return "NULL"
        return value


class RequestWithMethod(urllib.request.Request):
    """
    Helper class, to implement HTTP GET, POST, PUT, DELETE
    """
    def __init__(self, *args, **kwargs):
        self._method = kwargs.pop('method', None)
        urllib.request.Request.__init__(self, *args, **kwargs)

    def get_method(self):
        return self._method if self._method else super(RequestWithMethod, self).get_method()


class BasiumDriver(basium_driver.BaseDriver):
    def __init__(self, log=None, dbconf=None):
        self.log = log
        self.dbconf = dbconf

        self.uri = '%s/api' % (self.dbconf.host)

    def connect(self):
        """
        dummy, json api is stateless, we don't need connect

        todo, could potentially check if server is reachable
        """
        pass

    def execute(self, method=None, url=None, data=None, decode=False):
        if self.debug & bc.DEBUG_SQL:
            self.log.debug('Method=%s URL=%s Data=%s' % (method, url, data))
        respdata = None
        req = RequestWithMethod(url, method=method)
        if self.dbconf.username is not None:
            auth = '%s:%s' % (self.dbconf.username, self.dbconf.password)
            auth = auth.encode("utf-8")
            req.add_header(b"Authorization", b"Basic " + base64.b64encode(auth))
        try:
            if data:
                resp = urllib.request.urlopen(req, urllib.parse.urlencode(data, encoding="utf-8").encode("ascii") )
            else:
                resp = urllib.request.urlopen(req)
        except urllib.error.HTTPError as e:
            raise bc.Error(1, "HTTPerror %s" % e)
        except urllib.error.URLError as e:
            raise bc.Error(1, "URLerror %s" % e)

        if decode:
            encoding = resp.headers.get_content_charset()
            if encoding is None:
                encoding = "utf-8"
            try:
                tmp = resp.read().decode(encoding)
                res = json.loads(tmp)
                resp.close()
            except ValueError:
                raise bc.Error(1, "JSON ValueError for " + tmp)
            except TypeError:
                raise bc.Error(1, "JSON TypeError for " + tmp)

            try:
                if res['errno'] != 0:
                    raise bc.Error(res['errno'], res['errmsg'])
                respdata = res["data"]
            except KeyError:
                raise bc.Error(1, "Result keyerror, missing errno/errmsg")

        return respdata, resp

    def isDatabase(self, dbName):
        """
        Check if a database exist
        """
        url = '%s/_database/%s' % (self.uri, dbName)
        data, resp = self.execute(method='GET', url=url, decode=True)
        return data

    def isTable(self, tableName):
        """
        Check if a table exist
        """
        url = '%s/_table/%s' % (self.uri, tableName)
        data, resp = self.execute(method='GET', url=url, decode=True)
        return data

#     def createTable(self, obj):
#         """This is not valid for JSON API due to security issues"""
#         response = c.Response()
#         return response

#     def verifyTable(self, obj):
#         """
#         Verify that a table is equal the objects attributes and types
#         Todo: This should be possible to check over http
#            One idea is to calulate a checksum on all columns and their
#            attributes, and then compare client and server checksum
#         """
#         response = c.Response()
#         response.data = []
#         return response

#     def modifyTable(self, obj, actions):
#         """
#         Modify a database table so it corresponds to a object attributes and types
#         Not valid for JSON, due to security issues
#         """
#         return True

    def count(self, query):
        """
        Count the number of objects, filtered by query
        """
        if len(query._where) == 0:
            url = '%s/%s' % (self.uri, query.table())
        else:
            url = '%s/%s/filter?%s' % (self.uri, query.table(), query.encode())
        data, resp = self.execute(method='HEAD', url=url)
        count = resp.getheader("X-Result-Count")
        return int(count)

    def select(self, query):
        """
        Fetch one or multiple rows from a database
        Returns an object that can be iterated over, returning rows
        If there is any errors, an DriverError exception is raised

        two different formats:
          simple: <url>/<table>/<id>
          query : <url>/<table>/filter?column=oper,value[&column=oper,value]
        """

        if query.isId():
            # simple
            url = '%s/%s/%i' % (self.uri, query.table(), query._where[0].value)
        else:
            # real query
            url = '%s/%s/filter?%s' % (self.uri, query.table(), query.encode())
        data, resp = self.execute(method='GET', url=url, decode=True)
        return data

    def insert(self, table, values):
        url = '%s/%s' % (self.uri, table)
        data, resp = self.execute(method='POST', url=url, data=values, decode=True)
        return data

    def update(self, table, values):
        url = '%s/%s/%s' % (self.uri, table, values['_id'])
        data, resp = self.execute(method='PUT', url=url, data=values, decode=True)
        return data

    def delete(self, query):
        """
        delete a row from a table
        "DELETE FROM EMPLOYEE WHERE AGE > '%d'" % (20)
        refuses to delete all rows in a table (empty query)
        returns number of rows deleted
        """
        if query.isId():
            # simple
            url = '%s/%s/%i' % (self.uri, query.table(), query._where[0].value)
        else:
            # real query
            url = '%s/%s/filter?%s' % (self.uri, query.table(), query.encode())
        data, resp = self.execute('DELETE', url, decode=True)
        return data
