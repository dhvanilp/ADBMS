"""
Import this module in a python terminal
"""

import sys, traceback
from time import sleep
from storageclient import SimpleStorageTestClient
from dictionaryclient import DictionaryClient
from subprocess import Popen
from generic.protobufconnection import BlockingProtoBufConnection
from generic.communication_pb2 import StorageResponseHeader
LAST_PORT = 8080

storage_instances = {} #shortkey -> Popen
connections = {} #shortkey -->SimpleStorageTestClient

dictionary_instances = {} #shortkey -> Popen
dictionary_connections = {}

def terminateAll():
    for instance in storage_instances.values():
        instance.terminate()
    storage_instances.clear()
    for instance in dictionary_instances.values():
        instance.terminate()
    dictionary_instances.clear()
    
def exit2():
    terminateAll()
    sleep(1)
    sys.exit()
    
    
def _setPort(port):
    if port is None:
        global LAST_PORT
        port = LAST_PORT
        LAST_PORT += 1
    return port
    
    
def startStorage(shortkey, port=None, adminPort=None):
    port = _setPort(port)
    adminPort = _setPort(adminPort)
    if shortkey in storage_instances:
        raise Exception('%s already in running instaces' % shortkey)
    storage_instances[shortkey] = Popen(["./storageserver.py", "-p", str(port), "-a", str(adminPort), "-d", shortkey + ".bin"])
    print 'Created new storage service on port %d' % port
    sleep(3)
    connect("localhost", port, shortkey)
    
def ss(*args):
    startStorage(*args)

"""
New connect message:
-> service = 'dictionary' of 'storage' (Lekker generiek :D )
"""
def connect(service, host, port, shortkey=None):
    if service is 'dictionary':
        connectDictionary(host, port, shortkey)
    elif service is 'storage':
        connectStorage(host, port, shortkey)
    else:
        raise Exception("cannot start that service")

def connectStorage(server, port, shortkey=None):
    if shortkey is None:
        shortkey = '%s:%d' % (server, port)
    if shortkey in connections:
        raise Exception('%s already in active connections, choose a new connection name' % shortkey)
    connections[shortkey] = SimpleStorageTestClient(server, port)

def connectDictionary(server, port, shortkey=None):
    if shortkey is None:
        shortkey = '%s:%d' % (server, port)
    if shortkey in dictionary_connections:
        raise Exception('%s already in active dictionary connections, choose a new connection name' % shortkey)
    dictionary_connections[shortkey] = DictionaryClient(server, port)

"""
Disconnect all possible services
"""
def disconnectAll():
    for conn in connections.values():
        conn.stop()
    connections.clear()
    for conn in dictionary_connections.values():
        conn.stop()
    dictionary_connections.clear()

def add(shortkey, size, key=None):
    if shortkey not in dictionary_connections:
        raise Exception('%s does not exist' % shortkey)
    status, key, locs = dictionary_connections[shortkey].doADD(size, key)
    if not status:
        del dictionary_connections[shortkey] # TODO DISCONNECT
    #print key, locs
    return key, locs
    
def delete(shortkey, key):
    if shortkey not in dictionary_connections:
        raise Exception('%s does not exist' % shortkey)
    status, key = dictionary_connections[shortkey].doDELETE(key)
    if not status:
        del dictionary_connections[shortkey]
    print key

def get(shortkey, key):
    if shortkey not in dictionary_connections:
        raise Exception('%s does not exist' % shortkey)
    status, locations = dictionary_connections[shortkey].doGET(key)
    if not status:
        del dictionary_connections[shortkey]
    #print locations
    return locations

def write(shortkey, offset, data):
    if shortkey not in connections:
        raise Exception('%s does not exist' % shortkey)
    if not connections[shortkey].writeData(offset, data):
        del connections[shortkey]
    
    
def read(shortkey, offset, length):
    if shortkey not in connections:
        raise Exception('%s does not exist' % shortkey)
    readData = connections[shortkey].readData(offset, length)
    if not readData:
        del connections[shortkey] 
    return readData


        