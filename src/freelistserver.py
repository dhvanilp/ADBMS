import sys
from optparse import OptionParser
from twisted.python import log
from twisted.internet import reactor

from generic.genericserver import FixedLengthMessageServer
from generic.protocol import BinaryMessageProtocol
from freelist.handler import FreelistRequestHandler
from freelist.spacetable import FreeList


class FreelistServer(FixedLengthMessageServer):
    def __init__(self, options, args):
        super(FreelistServer, self).__init__(options, args)
        self.factory.handlerClass = FreelistRequestHandler
        self.factory.protocol = BinaryMessageProtocol
        self.factory.protocolVersion = 0b1
        self.factory.freelist = FreeList()
        

if __name__ == '__main__':
    parser = OptionParser()
    FreelistServer.addServerOptions(parser)
    
    parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=True, help="don't print status messages to stdout")
    
    #parser.add_option("-a", "--adminport", type="int", dest="admin_port", help="Port of admin server")
    
    
    (options, args) = parser.parse_args()
    
    if options.verbose:
        log.startLogging(sys.stdout)
    
    server = FreelistServer(options, args)
    server.listen()
    reactor.run()
