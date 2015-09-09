#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Copyright (c) 2013, Anders Lowinger, Abundo AB
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

# from wsgi.util import *

def select(name, options, selected=None):
    print('<select name="%s">' % name)
    for value in options:
        if value == selected:
            print('  <option value="%s" selected>%s</option>' % (value, value))
        else:
            print('  <option value="%s">%s</option>' % (value, value))
    print('</select>')

def checkbox(name, values, selected=None):
    pass
    
def radio(name, values, selected=None):
    for value in values:
        if value == selected:
            print('<input type="radio" name="%s" value="%s" checked>' % (name, value))
        else:
            print('<input type="radio" name="%s" value="%s">' % (name, value))

def dbtable_select():
    pass

def dbtable_radio():
    pass

def textarea(name, value, rows=None, cols=None):
    s = '"<textarea name="%s" ' %  name
    if rows:
        s += 'rows="%s" ' % rows
    if cols:
        s += 'cols="%s" ' % cols
    s += '>%s</textarea>' % value
    print(s)

# Unordered list
def ul(values):
    print("<ul>")
    for value in values:
        print("<li>%s</li>" & value)
    print("</ul>")

# Ordered list
def ol(values):
    print("<ol>")
    for value in values:
        print("<li>%s</li>" & value)
    print("</ol>")

class Cell:
    def __init__(self, data, attr):
        self.data = data
        self.attr = attr


class Table:
    def __init__(self, attr = ""):
        self.attr = attr
        self.ingress = None
        self.header = None
        self.footer = None
        self.heading = []
        self.data = None  # 2D array with data
        self.rows = 0   # total rows
        self.cols = 0   # total columns
        self.row = 0    # current row, when adding
        self.col = 0    # current col, when adding
        self.emptyCell = Cell("&nbsp", "")

    def setHeader(self, value):
        self.header = value
        return self
    
    def setFooter(self, value):
        self.footer = value
        return self
    
    def th(self, value, attr = ""):
        cell = Cell(value, attr)
        self.heading.append(cell)
        self.cols = max(self.cols, len(self.heading))
        return self

    def tr(self, attr = ""):
        if self.data:
            self.data.append([])
        else:
            self.data = []
        self.row += 1
        self.rows = max(self.rows, self.row)
        self.col = 0
        return self

    def td(self, value, attr = ""):
        cell = Cell(value, attr)
        row = self.row - 1 
        if len(self.data) < self.row:
            self.data.append([])
        self.data[row].append(cell)

        self.col += 1
        self.cols = max( self.cols, self.col )
        return self

#    def set(row, col, value, attr = ""):
#        cell = Cell(value, attr)
#        self.data[row][col] = cell
#        self.cols = max( self.cols, col)
#        self.rows = max( self.rows, row)
#        return self

    def get_th(self, col = None):
        if col == None:
            return self.heading
        return self.heading[col]

    def getRows(self):
        return self.rows

    def getCols(self):
        return self.cols

    def getRow(self, row):
        if array_key_exists(row, self.data):
            return self.data[row]
        return []

    def getCell(self, row, col):
        try:
            return self.data[row][col]
        except:
            return self.emptyCell
    
    #
    # Output the table as a string
    #
    def toString(self):
        s = ""
        if self.ingress:
            s += self.ingress
        if self.header:
            s += self.header
        s += "<table %s>\n" % self.attr
        if self.heading:
            s += "<tr>\n"
            for cell in self.heading:
                s += "<th %s>%s</th>" % (cell.attr, cell.data)
            s += "</tr>\n"
        for row in range(0, self.rows):
            s += "<tr>\n"
            for col in range(0, self.cols):
                cell = self.getCell(row, col)
                s += "<td %s>%s</td>\n" % (cell.attr, cell.data)
            s += "</tr>\n"
        s += "</table>\n"
        if self.footer != None:
            s += self.footer
        return s
