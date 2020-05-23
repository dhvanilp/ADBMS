from generic.communication_pb2 import StorageResponseHeader
from generic.protobufconnection import BlockingProtoBufConnection

from dictionaryclient import DictionaryClient

DICTIONARY_HOST = 'localhost'
DICTIONARY_PORT = 8000


class StorageClient(object):
    
    def __init__(self, locationMessage):
        self.connection = BlockingProtoBufConnection(StorageResponseHeader)
        self.connection.start(locationMessage.host, locationMessage.port)
        self.header = locationMessage.header
    
    def write(self, data):
        assert self.header.header.length == len(data)
        self.connection.sendMsg(self.header)
        self.connection.sendRawBytes(data)
        response = self.connection.readMsg()
        if response.status == StorageResponseHeader.ERROR:
            raise Exception("Write error: %s" % response.errorMsg)
            
    def read(self):
        self.connection.sendMsg(self.header)
        response = self.connection.readMsg()
        if response.status == StorageResponseHeader.ERROR:
            raise Exception("Read error: %s" % response.errorMsg)
        return self.connection.readNBytes(self.header.header.length)
        
    def stop(self):
        self.connection.stop()
        

def store(data, dictHost=DICTIONARY_HOST, dictPort=DICTIONARY_PORT):
    # get locations from dictionary client
    dictClient = DictionaryClient(dictHost, dictPort)
    status, key, locs = dictClient.doADD(len(data))
    dictClient.stop()
    if status:
        # send data to all clients
        curWritten = 0
        for loc in locs:
            client = StorageClient(loc)
            length = loc.header.header.length
            client.write(data[curWritten : curWritten + length])
            curWritten += length
            client.stop()
        dictClient.stop()
    return key
    
def retrieve(key, dictHost=DICTIONARY_HOST, dictPort=DICTIONARY_PORT):
    # get location from dictionary client
    dictClient = DictionaryClient(dictHost, dictPort)
    status, locs = dictClient.doGET(key)
    dictClient.stop()
    if status:
        # retrieve data from all clients
        result = ""
        for loc in locs:
            client = StorageClient(loc)
            result += client.read()
            client.stop()
        return result
    return None
    
