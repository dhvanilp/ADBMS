import sys, ssl, socket
from struct import pack, unpack
from twisted.python import log


STRUCT_BYTE = "!B"
PROTOCOL_VERSION = 0b1

class BlockingProtoBufConnection(object):
    def __init__(self, incomingMessageType):
        self.incomingMessageType = incomingMessageType
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket = ssl.wrap_socket(s, ca_certs="sslcert/cert.pem", cert_reqs=ssl.CERT_REQUIRED, ssl_version=ssl.PROTOCOL_SSLv23)
    
    def start(self, host, port):
        #log.msg("before socket connection")
        self.socket.connect((host, port))
        #log.msg("socket connected")
        # send protocol version
        self.socket.send(pack(STRUCT_BYTE, PROTOCOL_VERSION))
        #log.msg("version byte sent")
        
    def stop(self):
        self.socket.close()
        
    def resetSocket(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket = ssl.wrap_socket(s, ca_certs="sslcert/cert.pem", cert_reqs=ssl.CERT_REQUIRED, ssl_version=ssl.PROTOCOL_SSLv23)
        
    def _readNBytes(self, numBytes):
        #print 'read %d bytes' % numBytes
        msgData = ''
        while len(msgData) != numBytes:
            restLength = numBytes - len(msgData)
            received = self.socket.read(restLength)
            msgData = msgData + received
        return msgData
        
    def readMsg(self):
        responseLength = unpack(STRUCT_BYTE, self.readNBytes(1))[0]
        responseData = self.readNBytes(responseLength)
        responseMsg = self.incomingMessageType()
        responseMsg.ParseFromString(responseData)
        return responseMsg
        
    def readNBytes(self, numBytes):
        return self._readNBytes(numBytes)
        
    def sendMsg(self, msg):
        msgData = msg.SerializeToString()
        assert len(msgData) < 256
        self.socket.send(pack(STRUCT_BYTE, len(msgData)))
        self.socket.send(msgData)
        
    """
    Only call if a write header is send
    """
    def sendRawBytes(self, bytes):
        self.socket.send(bytes)
        
        
