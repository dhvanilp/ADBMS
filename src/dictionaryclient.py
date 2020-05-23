import sys, ssl, socket
from struct import pack, unpack

from generic.communication_pb2 import DictionaryHeader, DictionaryResponseHeader
from generic.protobufconnection import *

HOST = 'localhost'    # The remote host
PORT = 8989           # The same port as used by the server

STRUCT_BYTE = "!B"

class DictionaryClient(object):
    def __init__(self, host, port):
        self.connection = BlockingProtoBufConnection(DictionaryResponseHeader)
        self.connection.start(host, port)
        self.key = ""

    def sendHeartbeat(self, heartbeat):
        self.connection.sendMsg(heartbeat)

    def sendMsg(self, msg):
        self.connection.sendMsg(msg)

    def readMsg(self):
        return self.connection.readMsg()

    def sendRequest(self, request):
        self.connection.sendMsg(request)
        
    def getKey(self):
        return self.key
    
    def doHeartbeat(self):
        req = DictionaryHeader()
        req.operation = DictionaryHeader.HEARTBEAT
        req.issuer = "client"

        self.sendRequest(req)
        response = self.readMsg()
        if response.status == DictionaryResponseHeader.OK:
            self.key = response.key
            return True, response.key
        
        print responseHeader.status
        self.stop()
        return False, None
    
    def doADD(self, sizeOfData, key=None):
        req = DictionaryHeader()
        req.size = sizeOfData
        req.issuer = "client"
        if key is not None:
            print "key", key
            req.key = key
        #req.key = "foobar"  # REMOVE AFTER TESTING, RESULTS IN BAD BEHAVIOUR IN PRODUCTION ENV
        req.operation = DictionaryHeader.ADD
        #print req
        self.sendRequest(req)
        response = self.readMsg()
        if response.status == DictionaryResponseHeader.OK:
            self.key = response.key
            return True, response.key, response.locations
        
        print responseHeader.status
        self.stop()
        return False, None
        
    def doGET(self, key):
        req = DictionaryHeader()
        req.operation = DictionaryHeader.GET
        req.issuer = "client"
        req.key = key

        self.sendRequest(req)
        
        response = self.readMsg()
        if response.status == DictionaryResponseHeader.OK:
            return True, response.locations
            
        print "Error:", response.status
        self.stop()
        return False, None
        
    def doDELETE(self, key):
        req = DictionaryHeader()
        req.operation = DictionaryHeader.DELETE
        req.issuer = "client"
        req.key = key

        self.sendRequest(req)
        response = self.readMsg()
        
        if response.status == DictionaryResponseHeader.OK:
            return True, response
        
        print "Error:", response.status
        self.stop()
        return False, None

    def stop(self):
        self.connection.stop()
        

if __name__ == '__main__':
    print 'Connecting \n'
    dictCLI = DictionaryClient(HOST, PORT)
    
    size = 59863
    print dictCLI.doADD(size)
    print dictCLI.doGET(dictCLI.getKey())
    print dictCLI.doDELETE(dictCLI.getKey())
   

    print 'closed'
    
    
