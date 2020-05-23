from generic.communication_pb2 import RequestContainer, DictionaryLocation, AdminResponse, DictionaryKeys, MoveHostOperation

from generic.genericserver import FixedLengthMessageServer
from generic.protocol import BinaryMessageProtocol
from storageclient import SimpleStorageTestClient
from twisted.python import log


class DictionaryAdminRequestHandler(object):
    def __init__(self, protocol):
        self.protocol = protocol
        self.dictServer = self.protocol.factory.dictionaryServer

    def becomeMaster(self, masterFlag):
        if self.dictServer.factory.isMaster == masterFlag:
            self._reply("This doesn't make any sense to me. I'm alread a master")
        else:
            self.dictServer.factory.isMaster = masterFlag
            self._reply()
        
        

    def setSlave(self, host, port):
        replica = {"host": host, "port": port}
        if replica not in self.dictServer.factory.replicaList:
            log.msg("added replica to mys list")
            self.dictServer.factory.replicaList.append(replica)
            self._reply()
        else: 
            self._reply("Replica already exists")
            
    def setMaster(self, host, port):
        self.dictServer.factory.master = {"host": host, "port": port}
        self._reply()

    def resetGroupState(self):
        if not self.dictServer.factory.isMaster:
            self.dictServer.factory.master = {}     # reset my master
        self.dictServer.factory.replicaList = []    # empty my replicas
        self.dictServer.factory.isMaster = False

    def _reply(self, error=None):
        reply = AdminResponse()
        if error:
            reply.status = AdminResponse.ERROR
            reply.errorMsg = error
        else:
            reply.status = AdminResponse.OK
        self.protocol.writeMsg(reply)
        
    def moveHost(self, msg):
        self.dictServer.factory.delegate.filetable.moveHost(
            msg.moveFrom.host,
            msg.moveFrom.port,
            msg.moveTo.host,
            msg.moveTo.port
        )

    def parsedMessage(self, msgData):
        log.msg(">>> admin message received")
        incoming = RequestContainer()
        incoming.ParseFromString(msgData)
        if incoming.notification == RequestContainer.NEW_SLAVE:
            # notify self.protocol.dictServer of new replica slave
            msg = DictionaryLocation()
            msg.ParseFromString(incoming.messageData)
            self.setSlave(msg.host, msg.port)
            log.msg("New slave set: ", msg.host, " at ", msg.port)
        elif incoming.notification == RequestContainer.NEW_MASTER:
            msg = DictionaryLocation()
            msg.ParseFromString(incoming.messageData)
            self.setMaster(msg.host, msg.port)
            log.msg("New master set: ", msg.host, " at ", msg.port)
        elif incoming.notification == RequestContainer.IS_MASTER:
            self.becomeMaster(True)
            log.msg("I became new master!")
        elif incoming.notification == RequestContainer.IS_SLAVE:
            msg = DictionaryLocation()
            msg.ParseFromString(incoming.messageData)
            # set my master to the specified master
            self.setMaster(msg.host, msg.port)
            log.msg("I am a humble slave to :", msg.host, ":", msg.port)
            self._reply()
        elif incoming.notification == RequestContainer.RESET_STATE:
            self.resetGroupState()
            log.msg("Resetting state...")
            self._reply()
        elif incoming.notification == RequestContainer.MOVE_HOST:
            msg = MoveHostOperation()
            msg.ParseFromString(incoming.messageData)
            self.moveHost(msg)
            self._reply()
        else:
            self._reply("Unknown dictionary admin operation")
            raise Exception("Unkown dictionary admin operation")
    
class DictionaryAdminServer(FixedLengthMessageServer):
    def __init__(self, options, args, server):
        super(DictionaryAdminServer, self).__init__(options, args, options.admin_port)
        self.factory.handlerClass = DictionaryAdminRequestHandler
        self.factory.protocol = BinaryMessageProtocol
        self.factory.protocolVersion = 0b1

        # the dictServer it manages
        self.factory.dictionaryServer = server
        
        
        
