from os import sep

from twisted.python import log
from twisted.internet import ssl, reactor
from twisted.internet.protocol import Factory, Protocol

class FixedLengthMessageServer(object):
    
    @staticmethod
    def addServerOptions(parser):
        parser.add_option("-c", "--cert", dest="certificateFile", default="sslcert/cert.pem", help="load certificate from FILE (PEM format)", metavar="FILE")
        parser.add_option("-k", "--key", dest="privateKeyFile", default="sslcert/key.pem", help="load private key from FILE (PEM format)", metavar="FILE")
        parser.add_option("-p", "--port", type="int", dest="port", default=8989, help="set server PORT", metavar="PORT")
        
        
    def __init__(self, options, args, port=None):
        if port:
            self.port = port
        else:
            self.port = options.port
        self.sslCertificateFile = options.certificateFile
        self.sslPrivateKeyFile = options.privateKeyFile
        self._initFactory()
        
        
    def _initFactory(self):
        self.factory = Factory()
        self.factory.connections = []
        self.factory.server = self
        
    def listen(self):
        reactor.listenSSL(self.port, self.factory,
                          ssl.DefaultOpenSSLContextFactory(
                            self.sslPrivateKeyFile, self.sslCertificateFile))
        
    def run(self):
        self.listen()
        reactor.run()
