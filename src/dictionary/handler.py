from generic.communication_pb2 import DictionaryHeader, DictionaryResponseHeader
from generic.protobufconnection import BlockingProtoBufConnection
from twisted.python import log

from dictionaryclient import DictionaryClient

PROTOCOL_VERSION = 0b1

"""
Message handler that parses a message and delegates request
"""
class DictionaryRequestHandler():
    def __init__(self, protocol):
        self.protocol = protocol
        
    def status(self):
        print "Done replicating..."

    def putReplicaUpdates(self, msg):
        """
         start the replicaNotifier() it might have been stoppedd when it's empty.
         -> We know that it might be filled later
        """
        try:
            self.protocol.factory.replicaNotifier.start()
        except:
            print "Already running!?"
        # Put replication message in Queue of Dictserver
        for server in self.protocol.factory.replicaList:
            self.protocol.factory.replicaNotifier.sendReplicaUpdate(server['host'], server['port'], msg, self.status)
        

    def replicate(self, requestMessage):
        if requestMessage.operation != DictionaryHeader.HEARTBEAT:
            self.putReplicaUpdates(requestMessage)

    def redirect(self, redirectMsg):
        master = self.protocol.factory.master

        connection = BlockingProtoBufConnection(DictionaryResponseHeader)
        connection.start(master['host'], master['port'])
        connection.sendMsg(redirectMsg)
        msg = connection.readMsg()
        connection.stop()
        
        return msg
        

    def parsedMessage(self, msgData):
        requestMessage = DictionaryHeader()
        requestMessage.ParseFromString(msgData)
        
        log.msg(">>> got message: ", requestMessage)
        
        isMaster = self.protocol.factory.isMaster
        opp = requestMessage.operation
        status = None
        if isMaster:
            status = self.protocol.factory.delegate.handleRequest(requestMessage)
            requestMessage.issuer = "master"
            if requestMessage.operation == DictionaryHeader.ADD:
                requestMessage.key = status.key
            if opp != DictionaryHeader.GET and opp != DictionaryHeader.HEARTBEAT and len(self.protocol.factory.replicaList) != 0:
                self.replicate(requestMessage)
        else:
            if requestMessage.issuer == "master":
                status = self.protocol.factory.delegate.handleRequest(requestMessage)
            elif requestMessage.issuer == "client" and requestMessage.operation == DictionaryHeader.GET:
                status = self.protocol.factory.delegate.handleRequest(requestMessage)
            else:
                status = self.redirect(requestMessage)

        # Respond with status
        self.protocol.writeMsg(status)

        

