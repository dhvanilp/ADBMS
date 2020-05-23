from storageclient import SimpleStorageTestClient

from storageserver import DEFAULT_DB_SIZE # TODO FOR TESTING ONLY...
from generic.protobufconnection import BlockingProtoBufConnection
import socket

from generic.communication_pb2 import StorageAdminResponse, StorageAdminRequestContainer, StorageAdminRecoveryOperation, StorageAdminServerLocation

STAND_BY_LIST = [] # Connections
ACTIVE_LIST = [] # Raidgroups

TEST_MODE = True # disables checking for hosts

FREELIST_CONNECTION = None
DICTIONARY_CONNECTION = None


class AdminStorageClient(object):
    def __init__(self, host, port):
        self.connection = BlockingProtoBufConnection(StorageAdminResponse)
        self.connection.start(host, port)
        
    def _send(self, msg, opp):
        container = StorageAdminRequestContainer()
        container.operation = opp
        container.messageData = msg.SerializeToString()
        self.connection.sendMsg(container)
        
    def _checkResponse(self):
        response = self.connection.readMsg()
        if response.status == StorageAdminResponse.OK:
            return True
        print response.errorMsg
        return False
        
    def setXORServer(self, host, port):
        serverMsg = StorageAdminServerLocation()
        serverMsg.host = host
        serverMsg.port = port
        self._send(serverMsg, StorageAdminRequestContainer.SET_XOR_SERVER)
        return self._checkResponse()
    
    def recoverDataFrom(self, hostA, portA, hostB, portB):
        recoveryMsg = StorageAdminRecoveryOperation()
        recoveryMsg.serverA.host = hostA
        recoveryMsg.serverA.port = portA
        recoveryMsg.serverB.host = hostB
        recoveryMsg.serverB.port = portB
        self._send(recoveryMsg, StorageAdminRequestContainer.RECOVER_FROM)
        return self._checkResponse()
        
    def stop(self):
        self.connection.stop()

class Connection(object):
    
    def __init__(self, host, clientPort, adminPort):
        self.host = host
        self.clientPort = clientPort
        self.adminPort = adminPort
        self.client = SimpleStorageTestClient(host, clientPort)
        self.adminClient = AdminStorageClient(host, adminPort)
        
    def sendHeartbeat(self):
        try:
            self.client.readData(0, 1)
        except socket.error:
            return False
        return True
        
    def setXORServer(self, connection):
        self.adminClient.setXORServer(connection.host, connection.clientPort)
        
    def recoverDataFrom(self, server1, server2):
        self.adminClient.recoverDataFrom(
            server1.host, server1.clientPort,
            server2.host, server2.clientPort
        )
        
    def stop(self):
        self.client.stop()
        self.adminClient.stop()
        
    def __repr__(self):
        return '%s:[%d|%d]' % (self.host, self.clientPort, self.adminPort)


class RaidGroup(object):
    
    def __init__(self, a, b, x):
        self.serverA = a
        self.serverB = b
        self.xorServer = x
        print 'set serverA xor'
        self.serverA.setXORServer(x)
        print 'set serverB xor'
        self.serverB.setXORServer(x)
        print 'servers started'
        
        FREELIST_CONNECTION.releaseSpace([
            (a.host, a.clientPort, 0, DEFAULT_DB_SIZE),
            (b.host, b.clientPort, 0, DEFAULT_DB_SIZE)
        ])
        
    def _recover(self, server1, server2):
        newServer = _createGroup([server1, server2])[2]
        STAND_BY_LIST.remove(newServer)
        print 'recover data to new server:', newServer
        newServer.recoverDataFrom(server1, server2)
        return newServer
        
    def _recoverServer(self, deadServer, runningServer, xor):
        newServer = self._recover(runningServer, xor)
        newServer.setXORServer(self.xorServer)
        # send update to dictionary service
        DICTIONARY_CONNECTION.moveHost(deadServer.host, deadServer.clientPort, newServer.host, newServer.clientPort)
        # inform freelist about server recovery
        FREELIST_CONNECTION.moveHost(deadServer.host, deadServer.clientPort, newServer.host, newServer.clientPort)
        return newServer
        
    def _recoverXORServer(self):
        newXORServer = self._recover(self.serverA, self.serverB)
        self.serverA.setXORserver(newXORServer)
        self.serverB.setXORserver(newXORServer)
        return newXORServer
        
    def stop(self):
        if self.serverA:
            self.serverA.stop()
        if self.serverB:
            self.serverB.stop()
        if self.xorServer:
            self.xorServer.stop()
        
    def check(self):
        if self.serverA is not None and not self.serverA.sendHeartbeat():
            print 'Recover server A', self.serverA
            self.serverA = self._recoverServer(self.serverA, self.serverB, self.xorServer)
        
        if self.serverB is not None and not self.serverB.sendHeartbeat():
            print 'Recover server B', self.serverB
            self.serverB = self._recoverServer(self.serverB, self.serverA, self.xorServer)
        
        if self.xorServer is not None and not self.xorServer.sendHeartbeat():
            print 'Recover XOR server', self.xorServer
            self.xorServer = self._recoverXORServer()
            
    def __repr__(self):
        return '(A=%s,B=%s,X=%s)' % (self.serverA, self.serverB, self.xorServer)
            
def checkAllStorageServers():
    for group in ACTIVE_LIST:
        group.check()
    
def addServer(host, clientPort, adminPort):
    newServer = Connection(host, clientPort, adminPort)
    STAND_BY_LIST.append(newServer)
    

def _createGroup(servers = None):
    if not servers:
        servers = []
    for standby in STAND_BY_LIST:
        if standby.host not in [server.host for server in servers] or TEST_MODE:
            print 'add', standby, servers
            servers.append(standby)
            if len(servers) == 3:
                return servers
    raise Exception("Not enough standby servers available...")


def startNewGroup():
    servers = _createGroup()
    print servers
    for server in servers:
        STAND_BY_LIST.remove(server)
    newGroup = RaidGroup(*servers)
    ACTIVE_LIST.append(newGroup)


def stop():
    # stop all standby servers
    for server in STAND_BY_LIST:
        server.stop()
    del STAND_BY_LIST[:]
    
    # stop all active groups
    for group in ACTIVE_LIST:
        group.stop()
    del ACTIVE_LIST[:]
    
    #stop freelist connection
    global FREELIST_CONNECTION, DICTIONARY_CONNECTION # prevents undefined var error...
    if FREELIST_CONNECTION:
        FREELIST_CONNECTION.stop()
    FREELIST_CONNECTION = None
    
    #stop dictionary connection
    if DICTIONARY_CONNECTION:
        DICTIONARY_CONNECTION.stop()
    DICTIONARY_CONNECTION = None
    
def setDictionaryConnection(conn):
    global DICTIONARY_CONNECTION
    DICTIONARY_CONNECTION = conn
    
def setFreelistConnection(conn):
    global FREELIST_CONNECTION
    FREELIST_CONNECTION = conn

    