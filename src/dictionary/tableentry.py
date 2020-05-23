from generic.communication_pb2 import DataLocation, StorageHeader
from generic.crypto import *
from storage.handler import copyHeaderData

"""
An entry in the dictionaryTable
"""
class LocationEntry(object):
    """
    An entry is initialized with a key and a location where the data is stored
    """
    def __init__(self, host, port, offset, length):
        self.isWritten = False
        self.host = host
        self.port = port
        self.offset = offset
        self.length = length

    def toTuple(self):
        return(
            self.host,
            self.port,
            self.offset,
            self.length,
        )

    def toMessage(self):
        dataLocation = DataLocation()
        dataLocation.host = self.host
        dataLocation.port = self.port
        dataLocation.header.header.offset = self.offset
        dataLocation.header.header.length = self.length
        
        return dataLocation

    def __repr__(self):
        return '%s:%s Offset:%s Length:%s Written:%s' % (self.host, self.port, self.offset, self.length, self.isWritten)

    def toWriteMessage(self):
        msg = self.toMessage()
        msg.header.header.operation = StorageHeader.WRITE
        return self._toDataLocationMessage(msg)
        
    def toReadMessage(self):
        msg = self.toMessage()
        msg.header.header.operation = StorageHeader.READ
        return self._toDataLocationMessage(msg)

    def _toDataLocationMessage(self, msg):
        signAndTimestampHashedStorageHeader(msg.header)
        return msg
