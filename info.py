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
Page for the Basium WSGI handler
Show basic info on http request
"""

from __future__ import print_function
from __future__ import unicode_literals
__metaclass__ = type

def run(request, response, basium):
    response.contentType = 'text/html'
    print('<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">')
    print('<html>')
    print('<head>')
    print('<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">')
    print('<title>Basium Info</title>')
    print('</head>')
    print("<body>")
    print("<h1>Hello from web page</h1>")
    print("<p>Request</p>")
    print("<table border='1' cellspacing='0' cellpadding='2' width='1024'>")
    print("<tr><th>Key</th><th>Value</th></tr>")
    for key, val in request.__dict__.items():
        if key != 'environ':
            print("<tr>")
            print("<td>%s</td><td>%s</td>" % (key, val))
            print("</tr>")
    print("</table>")
    print()
    
    print("<p>Request.environ</p>")
    print("<table border='1' cellspacing='0' cellspacing='2' width='1024'>")
    print("<tr><th>Key</th><th>Value</th></tr>")
    for key, val in request.environ.items():
        print("<tr>")
        print("<td>%s</td><td>%s</td>" % (key, val))
        print("</tr>")
    print("</table>")
    
    print("</body>")
    print("</html>")
