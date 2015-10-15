#!/usr/bin/env python
from tornado.wsgi import WSGIContainer
from tornado.httpserver import HTTPServer
from tornado.ioloop import IOLoop
#from yourapplication import app


from app import app
#app.run(host='0.0.0.0', debug = True)

http_server = HTTPServer(WSGIContainer(app))
http_server.listen(5000)
IOLoop.instance().start()
