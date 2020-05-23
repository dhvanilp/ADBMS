from Queue import Queue, Empty

from twisted.internet import reactor
from twisted.python import log

from generic.protobufconnection import BlockingProtoBufConnection
from generic.crypto import signAndTimestampHashedStorageHeader

from generic.communication_pb2 import HashedStorageHeader, StorageHeader, StorageResponseHeader

"""
TODO: this class is currently just a simple wrapper
arround the blocking protobuf connection, this
should in the future be changed....
Also: fix code clones with storagedb queue.
"""
class XORPartnerConnection(object):
    def __init__(self, host, port):
        self.connection = BlockingProtoBufConnection(StorageResponseHeader)
        log.msg('self.connection.start(%s,%d)' % (host, port))
        self.connection.start(host, port)
        log.msg('finished')
        self.cont = False
        self.work_queue = Queue() # threadsafe queue
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
            
    def sendXORUpdate(self, offset, bytes, callback):
        self.work_queue.put((offset, bytes, callback))
    
    def _handleXORUpdate(self, offset, bytes, callback):
        # send header
        msg = HashedStorageHeader()
        msg.header.operation = StorageHeader.XOR_WRITE
        msg.header.offset = offset
        msg.header.length = len(bytes)
        signAndTimestampHashedStorageHeader(msg)
        self.connection.sendMsg(msg)
        # send actual data
        self.connection.sendRawBytes(bytes)
        # read response message, and signal callback
        response = self.connection.readMsg()
        if response.status == StorageResponseHeader.OK:
            reactor.callFromThread(callback, offset, len(bytes), None)
        else:
            reactor.callFromThread(callback.offset, len(bytes), response.errorMsg)
    
    """
    Blocking request (with timeout) that tries to
    perform a piece of work that is inside the que.
    """
    def _handleOneRequest(self):
        try: # blocking for 1 second, after this Empty is thrown
            task = self.work_queue.get(True, 1)
            self._handleXORUpdate(*task)
        except Empty:
            return
        
    """
    Worker function that continues working until
    stop is called and the queue eventueally becomes
    empty.
    """
    def _workerFunction(self):
        while self.cont or not self.work_queue.empty():
            self._handleOneRequest()
        self.connection.stop()
    
    """
    Starts
    """
    def start(self):
        if self.cont:
            raise Exception("Already running")
        self.cont = True
        reactor.callInThread(self._workerFunction)
        
    """
    Stop
    """
    def stop(self):
        self.cont = False
