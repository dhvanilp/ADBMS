from generic.communication_pb2 import HashedStorageHeader, StorageHeader, StorageResponseHeader
from generic.crypto import validateHashedStorageHeader

from twisted.python import log
from twisted.internet import reactor

import sys, traceback


# TODO timeout for XOR writes, what if it takes to long before a response
# is received from xor partner.
# or maybe even entire timeout for writes (also if a clients stops sending
# bytes but keeps connection open...)

"""
Helper function to copy the entire header data from
a given header to another.
The protobuf python library doesn't support this by
default?
"""
def copyHeaderData(fromHeader, toHeader):
    # TODO CHECK CopyFrom()
    toHeader.operation = fromHeader.operation;
    toHeader.offset = fromHeader.offset;
    toHeader.length = fromHeader.length;
    toHeader.requestTimestamp = fromHeader.requestTimestamp;
    

"""
Request handler for extrnal storage requests, every connection
has its own handler, so it is save to store private properties
per connection.
The database is shared accros other StorageRequestHandlers.
"""
class StorageRequestHandler():
    
    def __init__(self, protocol):
        self.protocol = protocol
        self.db = protocol.factory.db
        self.signedHeader = None
        self.currentWriteOffset = None
        self.currentXORWrittenOffset = None
    
    """
    Validate signedHeader, throws exception if hash is invalid
    """
    def _validateHash(self):
        validationResult = validateHashedStorageHeader(self.signedHeader)
        # explicit type checing of True is necessary since a string
        # is returned on error
        if validationResult is not True:
            self._sendExceptionAndDie("Hash validation error: %s" % validationResult)
        
    def _handleStorgeHeader(self):
        header = self.signedHeader.header
        opp = header.operation
        length = header.length
        offset = header.offset
        if opp == StorageHeader.READ:
            log.msg('Parsed READ header,  read operation')
            self.db.pushRead(header.offset, header.length, self.diskReadFinished)
            return 0 # we don't want to receive any raw data
        elif opp == StorageHeader.WRITE:
            if self.protocol.factory.xor_server_connection is None:
                self._sendExceptionAndDie("XOR server doesn't support replicated WRITE's")
            self.currentWriteOffset = offset
            self.currentXORWrittenOffset = offset
            log.msg('Parsed WRITE header, waiting for %d bytes' % length)
            return length
        elif opp == StorageHeader.XOR_WRITE:
            self.currentWriteOffset = offset
            log.msg('Parsed XOR_WRITE header, waiting for %d bytes' % length)
            return length
        raise Exception("Unkown operation")
    
    """
    Signaled by database if read data is finished
    self.protocol.writeMsg can be called since only one thread
    is allowed to execute this.
    """
    def diskReadFinished(self, offset, length, data):
        log.msg('diskReadFinished: %s...' % data[:30])
        self._sendACK()
        self.protocol.writeRaw(data)
        
    """
    Close connection and send error response
    """
    def _sendExceptionAndDie(self, errorString):
        self._sendACK(errorString)
        log.msg("ERROR: %s (is send to client)" % errorString)
        self.protocol.transport.loseConnection()
        
    """
    Send storage acknowledge to client
    if an error string is passed, an error response is send
    """
    def _sendACK(self, error=None):
        responseHeader = StorageResponseHeader()
        copyHeaderData(self.signedHeader.header, responseHeader.header)
        if error:
            responseHeader.status = StorageResponseHeader.ERROR
            responseHeader.errorMsg = error
        else:
            responseHeader.status = StorageResponseHeader.OK
        self.protocol.writeMsg(responseHeader)
        
    """
    Message received by parser
    """
    def parsedMessage(self, msgData):
        self.signedHeader = HashedStorageHeader()
        self.signedHeader.ParseFromString(msgData)
        log.msg(self.signedHeader)
        self.currentWriteOffset = 0
        self._validateHash()
        try:
            return self._handleStorgeHeader()
        except Exception, e:
            self._sendExceptionAndDie(repr(e))
            
    
    """
    Callback from _handleWrite that is called if the partner has 
    received and written the bytes.
    """
    def xorBytesWrittenToPartner(self, offset, length, errorMsg):
        # Check if something went wrong by sending xor update and
        # close connection, client will have to resend entire request.
        # Every WRITE request on a single channel is FIFO ordered, so
        # is enough to check the last offset.
        if errorMsg:
            self._sendExceptionAndDie("XOR Replicate error: %s" % errorMsg)
        if self.currentXORWrittenOffset != offset:
            self._sendExceptionAndDie("XOR Replicate error")
        self.currentXORWrittenOffset += length
        # check if entire file is written
        if self.currentXORWrittenOffset == self._finalOffset():
            # TODO: notify dictionary service that this file
            # is written and can be read after that send
            # acknowledge, thus NOT HERE...
            self._sendACK()
    
    """
    Handle bytes of write request from client.
    Write bytes locally and send to xor partner
    """
    def _handleWrite(self, bytes):
        log.msg('Write raw bytes of length %d' % len(bytes))
        def sendXORResult(offset, data, xored):
            self.protocol.factory.xor_server_connection.sendXORUpdate(
                offset, xored, self.xorBytesWrittenToPartner
            )
        self.db.pushXORRead(self.currentWriteOffset, bytes, sendXORResult)
        # it is FIFO, so we can now send the write request
        self.db.pushWrite(self.currentWriteOffset, bytes)
    
    """
    Handle bytes of xor write request
    """
    def _handleXORWrite(self, bytes):
        log.msg('Write the XORED result raw bytes of length %d' % len(bytes))
        self.db.pushXORWrite(self.currentWriteOffset, bytes)
        
        if self.currentWriteOffset + len(bytes) == self._finalOffset():
            self._sendACK()
        
    """
    A series of raw bytes is received by the parser.
    This are the actual storage bytes for a WRITE
    or XOR_WRITE request.
    """
    def parsedRawBytes(self, bytes):
        log.msg("Received %d raw bytes" % len(bytes))
        if self.signedHeader.header.operation == StorageHeader.WRITE:
            self._handleWrite(bytes)
        elif self.signedHeader.header.operation == StorageHeader.XOR_WRITE:
            self._handleXORWrite(bytes)
        # increase written offset
        self.currentWriteOffset += len(bytes)
        
    """
    Helper function to calculate index of the byte
    after last byte.
    """
    def _finalOffset(self):
        return self.signedHeader.header.offset + self.signedHeader.header.length
        
        