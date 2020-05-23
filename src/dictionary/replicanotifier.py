from Queue import Queue, Empty

from twisted.internet import reactor
from twisted.python import log

from generic.protobufconnection import BlockingProtoBufConnection
from generic.crypto import signAndTimestampHashedStorageHeader

from generic.communication_pb2 import DictionaryResponseHeader

"""
TODO: this class is currently just a simple wrapper
arround the blocking protobuf connection, this
should in the future be changed....
Also: fix code clones with storagedb queue.
"""
class ReplicaNotifier(object):
    def __init__(self):
        self.connection = BlockingProtoBufConnection(DictionaryResponseHeader)
        self.cont = False
        self.work_queue = Queue() # threadsafe queue
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
            
    def sendReplicaUpdate(self, host, port, msg, callback):
        self.work_queue.put((host, port, msg, callback))
    
    def _handleReplicaUpdate(self, host, port, msg, callback):
        self.connection.stop()
        self.connection.resetSocket()
        log.msg('self.connection.start(%s,%d)' % (host, port))
        self.connection.start(host, port)

        log.msg("handleReplica: ", msg)

        # send header
        self.connection.sendMsg(msg)
        # read response message, and signal callback
        response = self.connection.readMsg()
        if response.status == DictionaryResponseHeader.OK:
            log.msg("OK")
        else:
            log.msg("ERROR")
    
    """
    Blocking request (with timeout) that tries to
    perform a piece of work that is inside the que.
    """
    def _handleOneRequest(self):
        try: # blocking for 1 second, after this Empty is thrown
            task = self.work_queue.get(True, 1)
            self._handleReplicaUpdate(*task)
        except Empty:
            self.stop()
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
