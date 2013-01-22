import sys

def run(request, response, basium):
    response.contentType = 'text/html'
    print '<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">'
    print '<html>'
    print '<head>'
    print '<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">'
    print '<title>Basium Info</title>'
    print '</head>'
    print "<body>"
    print "<h1>Hello from web page</h1>"
    print "<p>Request</p>"
    print "<table border='1' cellspacing='0' cellpadding='2' width='1024'>"
    print "<tr><th>Key</th><th>Value</th></tr>"
    for key, val in request.__dict__.items():
        if key != 'environ':
            print "<tr>"
            print "<td>%s</td><td>%s</td>" % (key, val)
            print "</tr>"
    print "</table>"
    print
    
    print "<p>Request.environ</p>"
    print "<table border='1' cellspacing='0' cellspacing='2' width='1024'>"
    print "<tr><th>Key</th><th>Value</th></tr>"
    for key, val in request.environ.items():
        print "<tr>"
        print "<td>%s</td><td>%s</td>" % (key, val)
        print "</tr>"
    print "</table>"
    
    print "</body>"
    print "</html>"
    