from generic.communication_pb2 import StorageAdminResponse, StorageAdminRequestContainer, StorageAdminRecoveryOperation, StorageAdminServerLocation

from storage.xorpartnerconnection import XORPartnerConnection

from generic.genericserver import FixedLengthMessageServer
from generic.protocol import BinaryMessageProtocol
from storageclient import SimpleStorageTestClient
from twisted.python import log

from util import xorBytes

RECOVER_CHUNK_SIZE = 1024*1024*1 #10mb

class StorageAdminRequestHandler(object):
    def __init__(self, protocol):
        self.protocol = protocol
    
    
    def parsedMessage(self, msgData):
        incoming = StorageAdminRequestContainer()
        incoming.ParseFromString(msgData)
        if incoming.operation == StorageAdminRequestContainer.RECOVER_FROM:
            msg = StorageAdminRecoveryOperation()
            msg.ParseFromString(incoming.messageData)
            self._handleRecovery(msg)
        elif incoming.operation == StorageAdminRequestContainer.SET_XOR_SERVER:
            msg = StorageAdminServerLocation()
            msg.ParseFromString(incoming.messageData)
            self._handleSetXORServer(msg)
        else:
            raise Exception("Unkown storage admin operation")
    
    def _reply(self, error=None):
        reply = StorageAdminResponse()
        if error:
            reply.status = StorageAdminResponse.ERROR
            reply.errorMsg = error
        else:
            reply.status = StorageAdminResponse.OK
        self.protocol.writeMsg(reply)
        
    def _handleSetXORServer(self, serverMsg):
        #log.msg("Handle set XOR server")
        storageServer = self.protocol.factory.storageServer
        if storageServer.factory.xor_server_connection is not None:
            storageServer.factory.xor_server_connection.stop()
        #log.msg('create xor partner connection')
        storageServer.factory.xor_server_connection = XORPartnerConnection(serverMsg.host, serverMsg.port)
        #log.msg('.start()')
        storageServer.factory.xor_server_connection.start()
        #log.msg('relpy')
        self._reply()
        #log.msg('reply finished')
    
    def _recoverPiece(self, connA, connB, offset, length):
        log.msg("Recover from %d-%d (max: %d)" % (offset, offset+length, self.protocol.factory.databasesize))
        a = connA.readData(offset, length)
        #log.msg("A:", repr(a))
        b = connB.readData(offset, length)
        #log.msg("B:", repr(b))
        result = xorBytes(a, b)
        #log.msg("result:", repr(result))
        db = self.protocol.factory.storageServer.factory.db
        db.pushWrite(offset, result)
        
    def _recover(self, connA, connB):        
        current_offset = 0
        while current_offset + RECOVER_CHUNK_SIZE < self.protocol.factory.databasesize:
            self._recoverPiece(connA, connB, current_offset, RECOVER_CHUNK_SIZE)
            current_offset += RECOVER_CHUNK_SIZE
        # recover rest that didn't fit into the last chunk
        restLength = self.protocol.factory.databasesize - current_offset - 1
        if restLength != 0:
            self._recoverPiece(connA, connB, current_offset, restLength)
        log.msg("send reply")
        self._reply()
    
    def _handleRecovery(self, recoveryMsg):
        log.msg("Handle recovery")
        a = recoveryMsg.serverA
        b = recoveryMsg.serverB
        log.msg("Recover from %s:%d and %s:%d" % (a.host, a.port, b.host, b.port)) 
        connectionA = SimpleStorageTestClient(a.host, a.port)
        connectionB = SimpleStorageTestClient(b.host, b.port)
        self._recover(connectionA, connectionB)
        log.msg("recovery finished")


class StorageAdminServer(FixedLengthMessageServer):
    def __init__(self, options, args, server):
        super(StorageAdminServer, self).__init__(options, args, options.admin_port)
        self.factory.handlerClass = StorageAdminRequestHandler
        self.factory.protocol = BinaryMessageProtocol
        self.factory.protocolVersion = 0b1
        self.factory.storageServer = server
        self.factory.databasesize = options.databasesize
        
        
        