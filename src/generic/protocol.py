from struct import pack, unpack

from twisted.python import log
from twisted.internet.protocol import Protocol

from generic.parser import Parser


STRUCT_BYTE = "!B"


"""
This class represents the input stream.
If data is received the .push() method is
called with the new data.
The pop() methods are used the pop byte(s)
from the beginning of the stream.
"""
class ByteStream(object):
    
    def __init__(self):
        self.buf = ""
        
    """
    Push new bytes to the end of the stream.
    """
    def push(self, newData):
        self.buf += newData
        
    """
    Pop one numerical byte from the beginning
    of the stream.
    """
    def popByte(self):
        return unpack(STRUCT_BYTE, self.popToken())[0]
        
    """
    Pop one token from the beginning of the 
    stream.
    """
    def popToken(self):
        return self.popTokens(1)
        
    """
    Pop length tokens from the beginning of the
    stream.
    """
    def popTokens(self, length):
        res = self.buf[:length]
        self.buf = self.buf[length:]
        return res
    
    """
    Return if at least numBytes are available
    on the stream.
    """
    def numBytesAvailable(self, numBytes):
        return len(self.buf) >= numBytes
    
    """
    Returns if there are bytes available on
    the stream.
    """
    def bytesAvailable(self):
        return len(self.buf) > 0


"""
This class is used as the twisted protocol.
The twisted reactor will call the connectionMade(),
connectionLost() and dataReceived() methods  (see
twisted manual for this).
The protocol will call the parser.parse() method as
long as there is data available and the parser
doesn't block on read.
"""
class BinaryMessageProtocol(Protocol):
    
    """
    A new connection is made (called by twisted factory).
    """
    def connectionMade(self):
        self.stream = ByteStream()
        self.parser = Parser(self)
        
        self.factory.connections.append(self)
        log.msg('New connection, total activive connections: %d' % len(self.factory.connections))
        if len(self.factory.connections) > 100: # or check memory in use or something like that
            self.transport.write("Too many connections, try later") 
            self.transport.loseConnection()
        
    """
    A connection is closed (called by twisted factory).
    """
    def connectionLost(self, reason):
        # TODO: maybe more efficient connection list...
        self.factory.connections.remove(self)
        log.msg('Connection disconnected, total activive connections: %d' % len(self.factory.connections))
    
    """
    Received data at input stream (called by twisted factory).
    """
    def dataReceived(self, data):
        self.stream.push(data)
        # loop terminates if no bytes available or parser blocks
        while self.stream.bytesAvailable() and self.parser.parse(self.stream):
            pass
    
    """
    Write raw bytes to the output stream.
    """
    def writeRaw(self, rawData):
        self.transport.write(rawData)
        
    """
    Write protocol buffer message to output stream.
    """
    def writeMsg(self, protoBufMsg):
        msgData = protoBufMsg.SerializeToString()
        self.writeRaw(pack(STRUCT_BYTE, len(msgData)))
        log.msg('Send protoBufMsg of length %d to client.' % len(msgData))
        self.writeRaw(msgData)        
        
