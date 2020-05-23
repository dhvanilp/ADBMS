"""
The server manager that initiates actions by sending messages to adminDictClients
"""
from dictionaryclient import DictionaryClient

from generic.communication_pb2 import AdminResponse, RequestContainer, DictionaryLocation, DictionaryKeys, MoveHostOperation
from generic.protobufconnection import BlockingProtoBufConnection

from threading import Thread
from time import time, sleep
import socket

from twisted.python import log

REPLICA_GROUP_SIZE = 3

STAND_BY_LIST = []   # Connections
REPLICA_LIST = []    # Replicagroups

TEST_MODE = True

class DictionaryAdminClient(object):
    """
    Host and port are the Admin client to which we want to connect
    """
    def __init__(self, host, port):
        self.connection = BlockingProtoBufConnection(AdminResponse)
        self.connection.start(host, port)
    
    """
    A message to send to a dictManager
    """
    def _send(self, msg, notification):
        internal = RequestContainer()
        internal.notification = notification
        if msg is not None:
            internal.messageData = msg.SerializeToString()
        self.connection.sendMsg(internal)
    
    
    """
    Check response
    """
    def _checkResponse(self):
        response = self.connection.readMsg()
        if response.status == AdminResponse.OK:
            return True
        print response.errorMsg
        return False
    
    def resetState(self):
        self._send(None, RequestContainer.RESET_STATE)
        return self._checkResponse()
    
    """
    Give the DictServer a new slave located at host:port
    """
    def notifyMasterOfNewSlave(self, connection):
        serverMsg = DictionaryLocation()
        serverMsg.host = connection.host
        serverMsg.port = connection.clientPort

        self._send(serverMsg, RequestContainer.NEW_SLAVE)
        return self._checkResponse()
        
    """
    Give the DictServer a new Master located at host:port
    """
    def notifySlaveOfNewMaster(self, connection):
        serverMsg = DictionaryLocation()
        serverMsg.host = connection.host
        serverMsg.port = connection.clientPort

        self._send(serverMsg, RequestContainer.NEW_MASTER)
        return self._checkResponse()
    
    """
    Tell the dictServer that he is a slave
    """
    def setSlave(self, masterConnection):
        serverMsg = DictionaryLocation()
        serverMsg.host = masterConnection.host
        serverMsg.port = masterConnection.clientPort

        # tell dictserver that he is a slave
        self._send(serverMsg, RequestContainer.IS_SLAVE)
        return self._checkResponse()
    
    """
    Tell the dictServer that he is a Master
    """    
    def setMaster(self):
        # tell dictserver that he is a master
        self._send(None, RequestContainer.IS_MASTER)
        return self._checkResponse()

    def moveHost(self, fromHost, fromPort, toHost, toPort):
        mh = MoveHostOperation()
        mh.moveFrom.host = fromHost
        mh.moveFrom.port = fromPort
        mh.moveTo.host = toHost
        mh.moveTo.port = toPort
        self._send(mh, RequestContainer.MOVE_HOST)
        
    """
    Close the connection
    """
    def stop(self):
        #print "Con closed"
        self.connection.stop()

class Connection(object):
    def __init__(self, host, clientPort, adminPort):
        self.host = host
        self.clientPort = clientPort
        self.adminPort = adminPort
        self.client = DictionaryClient(host, clientPort)
        self.adminClient = DictionaryAdminClient(host, adminPort)
        
    def sendHeartbeat(self):
        try:
            self.client.doHeartbeat()
            log.msg("HEARTBEAT SENT")
        except socket.error:
            return False
        return True
        

    """
    Set new replica and notify master of this replica
    """
    def addNewSlaveServer(self, connection):
        # I assume you know what you are doing and won't set a new slave that already exists
        self.adminClient.notifyMasterOfNewSlave(connection)
        
    """
    Set new master and notify master of this replica
    """
    def addNewMasterServer(self, connection):
        # I assume you know what you are doing and won't set a new master that already exists
        self.adminClient.notifySlaveOfNewMaster(connection)

    # tell server that he is a slave
    def setSlave(self, masterConnection):
        self.adminClient.setSlave(masterConnection)
        #self.adminClient.notifySlaveOfNewMaster(masterConnection)

    # tell server that he is a master
    def setMaster(self):
        self.adminClient.setMaster()
        
    def stop(self):
        self.client.stop()
        self.adminClient.stop()
        
    def __repr__(self):
        return '%s:[%d|%d]' % (self.host, self.clientPort, self.adminPort)

class ReplicaGroup(object):
    def __init__(self, servers):
        if servers is []:
            raise Exception("Cannot create empty replica group")
        self.group = {"master": None, "slave":[]}
        for server in servers:
            if self.group['master'] == None:
                self.group['master'] = server
            else:
                self.group['slave'].append(server)
        self.initiate()
        
        
    """
    1. notify the master that he is a master
    2. for each slave:
        a. notify slave that he is a slave
        b. tell the slave who his master is
    3. notify the master of his new slave
    """
    def initiate(self):
        master = self.group['master']
        master.setMaster()

        for slave in self.group['slave']:
            slave.setSlave(master)              # Inform the the slave that it is a slave of masterConnection
            master.addNewSlaveServer(slave)     # inform the master of a new slave

    def addSlave(self, slaveCon):
        self.group['slave'].append(slaveCon)
        self.initiate()
    
    # We need to recover the master (aka set a new one)!! 
    def recoverMaster(self):
        if self.group['slave'] == []:
            raise Exception("I'm out of slaves cannot recover")
        
        self.group['master'] = self.group['slave'].pop(0)
        self.initiate()
        print "Successfully recovered. New group: ", self.group
    
    # function that WARNS that a slave is down
    def fallenSlave(self, slaveCon):
        self.group['slave'].remove(slaveCon)
        
        print "WARNING: slave is down! Attempting recovery"
        addSlave(REPLICA_LIST.index(self))
        
    def check(self):
        # go over all servers and check if we need to initiate change
        if self.group['master'] is not None and not self.group['master'].sendHeartbeat():
            self.recoverMaster()
        for slave in self.group['slave']:
            if slave is not None and not slave.sendHeartbeat():
                self.fallenSlave(slave)
    
    def stop(self):
        self.group['master'].stop()
        for slave in self.group['slave']:
            slave.stop()
        self.group= {'master': None, 'slave': []}
    
    def getGroup(self):
        return self.group
    
    def __repr__(self):
        return str(self.getGroup())

    
def connectServer(host, clientPort, adminPort):
    newServer = Connection(host, clientPort, adminPort)
    STAND_BY_LIST.append(newServer)
    
def checkAllDictionaryServers():
    #do something usefull
    for group in REPLICA_LIST:
        group.check()


def _createReplicaGroup(servers = None):
    if not servers:
        servers = []
    for standby in STAND_BY_LIST:
        if standby.host not in [server.host for server in servers] or TEST_MODE:
            #print 'add', standby, servers
            servers.append(standby)
            if len(servers) == REPLICA_GROUP_SIZE:
                return servers
    raise Exception("Not enough standby servers available...")

def startNewReplicaGroup():
    servers = _createReplicaGroup()
    for server in servers:
        STAND_BY_LIST.remove(server)
    newGroup = ReplicaGroup(servers)
    REPLICA_LIST.append(newGroup)
    
def addSlave(groupnum):
    if len(STAND_BY_LIST) > 0:
        if len(REPLICA_LIST) != 0:
            REPLICA_LIST[groupnum].addSlave(STAND_BY_LIST.pop(0))
    else:
        raise Exception("Not enough standby servers available...")
    
def stop():
    for server in STAND_BY_LIST:
        server.stop()
    del STAND_BY_LIST[:]
    for group in REPLICA_LIST:
        group.stop()
    del REPLICA_LIST[:]
