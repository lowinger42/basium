import json

import basium_orm
import basium_common

log = basium_common.log
db = None


class API(object):
    def __init__(self, request, response, basium):
        self.request = request
        self.response = response
        self.basium = basium
        self.db = basium.db
        self.write = response.write # convenience
    
    def handleGet(self, classname, id_, attr):
        obj = classname()
        if id_ == None:
            log.debug('Get all rows in table %s' % obj._table)
            # all rows (put some sane limit here maybe?)
            dbquery = basium_orm.Query(self.db, obj)
        elif id_ == 'filter':
            # filter out specific rows
            dbquery = basium_orm.Query(self.db, obj)
            dbquery.decode(self.request.querystr)
            log.debug("Get all rows in table '%s' matching query %s" % (obj._table, dbquery.toSql()))
        else:
            # one row, identified by rowID
            dbquery = basium_orm.Query(self.db).filter(obj.q.id, '=', id_)
            log.debug("Get one row in table '%s' matching query %s" % (obj._table, dbquery.toSql()))

        response = self.db.driver.select(dbquery)  # we call driver direct for efficiency reason
        if response.isError():
            msg = "Could not load objects from table '%s'. %s" % (obj._table, response.getError())
            log.debug(msg)
            self.write(msg)
            self.response.status = '404 ' + msg
            return
        lst = response.get('data')
        if id_ != None and id_ != 'filter' and len(lst) == 0:
            msg = "Unknown ID %s in table '%s'" % (id_, obj._table)
            response.setError(1, msg)
            log.debug(msg)
            self.response.status = '404 ' + msg
        self.write( json.dumps(response, cls=basium_common.JsonOrmEncoder) )

    #
    #
    #    
    def handlePost(self, classname, id_, attr):
        obj = classname()
        if id_ != None:
            self.response.status = "400 Bad Request, no ID needed to insert a row"
            return

        postdata = json.loads(self.request.body)    # decode data that should be stored in database
        response = self.db.driver.insert(obj._table, postdata) # we call driver direct for efficiency reason
        print json.dumps(response.get(), cls=basium_common.JsonOrmEncoder)

    #
    #
    #    
    def handlePut(self, classname, id_, attr):
        if id_ == None:
            self.response.status = "400 Bad Request, need ID to update a row"
            return
        # update row
        obj = classname()
        putdata = json.loads(self.request.body)    # decode data that should be stored in database
        response = self.db.driver.update(obj._table, putdata) # we call driver direct for efficiency reason
        self.write( json.dumps(response.get(), cls=basium_common.JsonOrmEncoder) )

    #
    #
    #    
    def handleDelete(self, classname, id_, attr):
        if id_ == None:
            self.response.status = "400 Bad Request, need ID to delete a row"
            return
            self.response.status = "400 Bad Request, delete not implemented yet"
#        obj = classname()


    #
    # Count the number of rows matching a query
    # Return data in a HTML header
    #
    def handleHead(self, classname, id_, attr):
        obj = classname()
        if id_ == None:
            log.debug('Count all rows in table %s' % obj._table)
            # all rows (put some sane limit here maybe?)
            dbquery = basium_orm.Query(self.db, obj)
        elif id_ == 'filter':
            # filter out specific rows
            dbquery = basium_orm.Query(self.db, obj)
            dbquery.decode(self.querystr)
            log.debug("Count all rows in table '%s' matching query %s" % (obj._table, dbquery.toSql()))

        response = self.db.driver.count(dbquery)  # we call driver direct for efficiency reason
        if response.isError():
            msg = "Could not count objects in table '%s'. %s" % (obj._table, response.getError())
            log.debug(msg)
            self.status = '404 ' + msg
            return
        self.response.addHeader('X-Result-Count', str(response.get('data') ))

    
    #
    #
    #    
    def handleAPI(self):
        attr = self.request.attr
        ix = 0
        if not attr[ix] in self.basium.cls:
            self.response.status = "404 table '%s' not found" % (attr[ix])
            return
        classname = self.basium.cls[attr[ix]]
        ix += 1
        if len(attr) > ix:
            id_ = attr[ix]
            ix += 1
        else:
            id_ = None
        if self.request.method == 'GET':
            self.handleGet(classname, id_, attr[ix:])
        elif self.request.method == "POST":
            self.handlePost(classname, id_, attr[ix:])
        elif self.request.method == "PUT":
            self.handlePut(classname, id_, attr[ix:])
        elif self.request.method == "DELETE":
            self.handleDelete(classname, id_, attr[ix:])
        elif self.request.method == "HEAD":
            self.handleHead(classname, id_, attr[ix:])
        else:
            # not a request we understand
            self.response.status = "400 Unknown request %s" % self.request.method

#
#
#    
def run(request, response, basium):
    api = API(request, response, basium)
    api.handleAPI()
