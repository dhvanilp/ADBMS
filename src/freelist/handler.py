from generic.communication_pb2 import FreelistRequest, FreeListResponse, SpaceLocation

from twisted.python import log
from twisted.internet import reactor


class FreelistRequestHandler():
    def __init__(self, protocol):
        self.protocol = protocol
        self.freelist = protocol.factory.freelist
        self.request = None
        self.response = FreeListResponse()
        
    """
    Send reply to client, if error is None state is set to OK
    otherwise the error message is set and state is ERROR.
    """
    def _sendReply(self, error=None):
        if error:
            self.response.status = FreeListResponse.ERROR
            self.response.errorMsg = error
            log.msg("Send error to client: %s" % error)
        else:
            self.response.status = FreeListResponse.OK
        self.protocol.writeMsg(self.response)
            
        
    def _handleRelease(self):
        for loc in self.request.releasedSpace:
            self.freelist.releaseSpace(loc.host, loc.port, loc.offset, loc.length)
        self._sendReply()
        
    def _copySpaceToResponse(self, freeSpace):
        for (host, port, offset, length) in freeSpace:
            location = SpaceLocation()
            location.host = host
            location.port = port
            location.offset = offset
            location.length = length
            self.response.freeSpace.extend([location])
            
    def _handleAllocate(self):
        result = self.freelist.allocSpace(self.request.numberOfBytes)
        if result:
            self._copySpaceToResponse(result)
            self._sendReply()
        else:
            self._sendReply("Not enough space available")
        
    def _handleMoveHost(self):
        fr = self.request.moveFrom
        to = self.request.moveTo
        self.freelist.moveHost(fr.host, fr.port, to.host, to.port)
        
    """
    Message received by parser
    """
    def parsedMessage(self, msgData):
        self.request = FreelistRequest()
        self.request.ParseFromString(msgData)
        if self.request.operation == FreelistRequest.RELEASE and len(self.request.releasedSpace) > 0:
            self._handleRelease()
        elif self.request.operation == FreelistRequest.ALLOCATE and self.request.HasField("numberOfBytes"):
            self._handleAllocate()
        elif self.request.operation == FreelistRequest.MOVE_HOST and self.request.HasField("moveFrom") and self.request.HasField("moveTo"):
            self._handleMoveHost()
        else:
            self._sendReply("Invalid freelist message type, do nothing...")
    
    