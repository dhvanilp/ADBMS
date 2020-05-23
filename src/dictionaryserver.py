import sys
from optparse import OptionParser
from Queue import Queue, Empty

from twisted.python import log
from twisted.internet import reactor

from generic.genericserver import FixedLengthMessageServer
from generic.protocol import BinaryMessageProtocol

from dictionary.handler import DictionaryRequestHandler
from dictionary.server import LocationHandler
from dictionary.admin import DictionaryAdminServer
from dictionary.replicanotifier import ReplicaNotifier

from twisted.web import server, resource
from twisted.internet import reactor

class Simple(resource.Resource):
    isLeaf = True
    def __init__(self, server):
        self.server = server

    def render_GET(self, request):
        if self.server.factory.isMaster:
            return """
            <html>
                <h1>Dictionary server maintenance page: </h1>
                <h2>I am a master.</h2>
                <ul>
                    <li>My replicas: %s </li>
                    <li>My data(locations): %s</li>
                </ul>
            </html>
            """ % (self.server.factory.replicaList, self.server.factory.delegate.filetable)
        else:
            return """
            <html>
                <h1>Dictionary server maintenance page: </h1>
                <h2>I am a replica</h2>
                <ul>
                    <li>My master: %s </li>
                    <li>My data(locations): %s</li>
                </ul>
            </html>
            """ % (self.server.factory.master, self.server.factory.delegate.filetable)

class DictionaryServer(FixedLengthMessageServer):
    def __init__(self, options, args):
        super(DictionaryServer, self).__init__(options, args)
        self.factory.handlerClass = DictionaryRequestHandler
        self.factory.protocol = BinaryMessageProtocol
        self.factory.protocolVersion = 0b1
        self.factory.delegate = LocationHandler('localhost', 8888)# TODO: should be set automatically
        self.factory.master = {}
        self.factory.isMaster = False
        self.factory.replicaList = []
        self.factory.replicaNotifier = ReplicaNotifier()

if __name__ == '__main__':
    parser = OptionParser()
    DictionaryServer.addServerOptions(parser)

    parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=True, help="don't print status messages to stdout")
    parser.add_option("-a", "--adminport", type="int", dest="admin_port", help="Port of admin server")
    parser.add_option("-w", "--webport", type="int", dest="webport", help="Set the web management port of this server")
    
    (options, args) = parser.parse_args()
    
    if options.verbose:
        log.startLogging(sys.stdout)
    
    # The actual DictServer that does all the work
    dserver = DictionaryServer(options, args)
    dserver.listen()
    # The manager that receives messages from the dictionaryManager
    adminServer = DictionaryAdminServer(options, args, dserver)
    adminServer.listen()
    
    site = server.Site(Simple(dserver))
    reactor.listenTCP(options.webport, site)

    # Run all services
    reactor.run()
