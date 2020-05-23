from generic.protobufconnection import BlockingProtoBufConnection
from generic.communication_pb2 import HashedStorageHeader, StorageHeader, StorageResponseHeader


from generic.crypto import signAndTimestampHashedStorageHeader # for testing only

HOST = 'localhost'    # The remote host
PORT = 8080           # The same port as used by the server

"""
Note: this class does almost nothing with the response
header data since it is blocking, and everything is send
and received order.
"""
class SimpleStorageTestClient(object):
    
    def __init__(self, host, port):
        self.connection = BlockingProtoBufConnection(StorageResponseHeader)
        self.connection.start(host, port)
        
    def _sendHeader(self, offset, length, opp):
        # NOTE: in real world this is created by the
        # dictionary service.
        msg = HashedStorageHeader()
        msg.header.operation = opp
        msg.header.offset = offset
        msg.header.length = length
        signAndTimestampHashedStorageHeader(msg)
        #print msg
        self.connection.sendMsg(msg)
        
    def writeData(self, offset, data):
        #print 'writeData(%d, %s)' % (offset, data)
        self._sendHeader(offset, len(data), StorageHeader.WRITE)
        self.connection.sendRawBytes(data)
        responseHeader = self.connection.readMsg()
        if responseHeader.status == StorageResponseHeader.OK:
            return True
        print responseHeader.errorMsg
        self.stop()
        return False
        
    def readData(self, offset, length):
        self._sendHeader(offset, length, StorageHeader.READ)
        responseHeader = self.connection.readMsg()
        if responseHeader.status == StorageResponseHeader.OK:
            return self.connection.readNBytes(responseHeader.header.length)
        print responseHeader.errorMsg
        self.stop()
        return None
        
    def stop(self):
        self.connection.stop()
    

if __name__ == '__main__':
    """
    VERY simple and stupid test....
    """
    client = SimpleStorageTestClient(HOST, PORT)
    
    data = "HELLO WORLD!!!!!"
    
    client.writeData(0, data)
    print client.readData(0, len(data))
    client.stop();
    
    print 'closed..'
    
    
