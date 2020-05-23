import sys
from optparse import OptionParser
from twisted.python import log
from twisted.internet import reactor

from generic.genericserver import FixedLengthMessageServer
from generic.protocol import BinaryMessageProtocol
from storage.handler import StorageRequestHandler
from storage.storagedb import StorageDatabase
from storage.admin import StorageAdminServer

DEFAULT_DB_SIZE = 10*1024*1024 # 10mb, small for testing

class StorageServer(FixedLengthMessageServer):
    def __init__(self, options, args):
        super(StorageServer, self).__init__(options, args)
        self.factory.db = StorageDatabase(options.databasefile, options.databasesize)
        self.factory.db.start()
        self.factory.handlerClass = StorageRequestHandler
        self.factory.protocol = BinaryMessageProtocol
        self.factory.protocolVersion = 0b1
        self.factory.xor_server_connection = None

if __name__ == '__main__':
    parser = OptionParser()
    StorageServer.addServerOptions(parser)
    
    parser.add_option("-d", "--db", dest="databasefile", default="storagedb.bin", help="loctation of database file", metavar="FILE")
    parser.add_option("-s", "--dbsize", dest="databasesize", type="int", default=DEFAULT_DB_SIZE, help="size of database in bytes")
    
    parser.add_option("-q", "--quiet", action="store_false", dest="verbose", default=True, help="don't print status messages to stdout")
    
    parser.add_option("-a", "--adminport", type="int", dest="admin_port", help="Port of admin server")
    
    
    (options, args) = parser.parse_args()
    
    if options.verbose:
        log.startLogging(sys.stdout)
    
    server = StorageServer(options, args)
    server.listen()
    adminServer = StorageAdminServer(options, args, server)
    adminServer.listen()
    reactor.run()
