from generic.protobufconnection import BlockingProtoBufConnection
from generic.communication_pb2 import FreelistRequest, FreeListResponse, SpaceLocation

from twisted.python import log


HOST = 'localhost'    # The remote host
PORT = 8989           # The same port as used by the server

"""
Note: this class does almost nothing with the response
header data since it is blocking, and everything is send
and received order.
"""
class SimpleFreelistTestClient(object):
    
    def __init__(self, host, port):
        self.connection = BlockingProtoBufConnection(FreeListResponse)
        self.connection.start(host, port)
        
    def _handleResponse(self):
        responseHeader = self.connection.readMsg()
        if responseHeader.status == FreeListResponse.OK:
            return responseHeader
        print responseHeader.errorMsg
        return None
        
    def moveHost(self, fromHost, fromPort, toHost, toPort):
        msg = FreelistRequest()
        msg.operation = FreelistRequest.MOVE_HOST
        msg.moveFrom.host = fromHost
        msg.moveFrom.port = fromPort
        msg.moveTo.host = toHost
        msg.moveTo.port = toPort
        self.connection.sendMsg(msg)
        return self._handleResponse() is not None
        
        
    def allocateSpace(self, numberOfBytes):
        assert numberOfBytes > 0, "allocate request should always be larger than 1 byte"
        msg = FreelistRequest()
        msg.operation = FreelistRequest.ALLOCATE
        msg.numberOfBytes = numberOfBytes
        self.connection.sendMsg(msg)
        responseMsg = self._handleResponse()
        if responseMsg is None:
            return False
        return [(loc.host, loc.port, loc.offset, loc.length) for loc in responseMsg.freeSpace]
        
        
    def releaseSpace(self, releaseTupleList): #[(host, port, offset, length)]
        msg = FreelistRequest()
        msg.operation = FreelistRequest.RELEASE
        for entry in releaseTupleList:
            l = SpaceLocation()
            l.host, l.port, l.offset, l.length = entry
            msg.releasedSpace.extend([l])
        self.connection.sendMsg(msg)
        return self._handleResponse() is not None
        
    def stop(self):
        self.connection.stop()
    

if __name__ == '__main__':
    
    import sys
    log.startLogging(sys.stdout)
    log.msg('Performing simple tests...')
    
    client = SimpleFreelistTestClient(HOST, PORT)
    
    hostA = ("A", 8080, 0, 10) # 100mb
    hostB = ("B", 8080, 0, 10) # 100mb
    
    client.releaseSpace([hostA, hostB])
    log.msg("Released space")
    log.msg(client.allocateSpace(3))
    log.msg(client.allocateSpace(12))
    log.msg(client.allocateSpace(8))
    log.msg(client.allocateSpace(4))
    client.stop();
    log.msg('closed..')
    
    
