#!/usr/bin/env python
from random import choice

from twisted.python import log


class FreeListEntry(object):
    def __init__(self, host, port, offset, length):
        self.host = host
        self.port = port
        self.offset = offset
        self.length = length
        
    def consume(self, numberOfBytes):
        assert numberOfBytes <= self.length, "Cannot consume more than the total size"
        # create consumed piece
        consumed = FreeListEntry(self.host, self.port, self.offset, numberOfBytes)
        # decrease existing piece
        self.offset += numberOfBytes
        self.length -= numberOfBytes
        return consumed
        
    def toTuple(self):
        return (self.host, self.port, self.offset, self.length)
        
    def __repr__(self):
        return "(%s:%d %d-%d)" % (self.host, self.port, self.offset, self.length)

class FreeList(object):
    def __init__(self):
        self.index = 0
        self.memtable = [] # free list entries
        
    def _roundRobinIndex(self, idx):
        return (self.index + idx) % len(self.memtable)
    
    """
    Return minimum number of memory pieces (starting from self.index)
    that are necessary to alocate at least numberOfBytes bytes.
    self._roundRobinIndex transforms the index to the real index.
    None is returned if not enough memory is avialable.
    """
    def _getEnoughMemoryPieces(self, numberOfBytes):
        consumedSize = 0
        for idx in range(len(self.memtable)):
            consumedSize += self.memtable[self._roundRobinIndex(idx)].length
            if consumedSize >= numberOfBytes:
                log.msg("Requires %d pieces" % (idx + 1))
                return idx + 1
        return None
        
        
    def _consumeEntirePieces(self, pieces):
        consumed = []
        for idx in range(pieces):
            # walk over blocks that must be totally consumed, can be
            # empty in case of 1 block
            realIdx = self._roundRobinIndex(idx) # get real index in memtable
            consumed.append(self.memtable.pop(realIdx)) # remove from memtable and pass to client
        return consumed
        
    def _consumeLastPiece(self, idx, restSize):
        lastIdx = self._roundRobinIndex(idx)
        lastEntry = self.memtable[lastIdx]
        if restSize == lastEntry.length: #entire piece must be consumed
            lastConsumption = lastEntry
            del self.memtable[lastIdx]
        else:
            lastConsumption = self.memtable[lastIdx].consume(restSize) # consume rest from last part
        return lastConsumption
        
    def _roundRobinConsumptionStrategy(self, numberOfBytes):
        if self.index > len(self.memtable):
            self.index = 0 # reset round robin index
        pieces = self._getEnoughMemoryPieces(numberOfBytes)
        if pieces is None: # not enough memory available
            return None
            
        consumed = self._consumeEntirePieces(pieces - 1)
        
        totalMemory = sum([entry.length for entry in consumed]) # total memory in consumed
        rest = numberOfBytes - totalMemory # is never zero
        consumed.append(self._consumeLastPiece(pieces - 1, rest))
        
        self.index += pieces # increase round robin index
        return consumed

    """
    Returns a list of tuples containing (host, port, offset, length) of 
    the allocated numberOfBytes.
    Returns None if the request could not be fulfilled
    """
    def allocSpace(self, numberOfBytes):
        log.msg("allocSpace(%d)" % numberOfBytes)
        assert numberOfBytes > 0, "allocate request should always be larger than 1 byte"
        if len(self.memtable) == 0:
            return None
        result = self._roundRobinConsumptionStrategy(numberOfBytes)
        if result is None:
            return None
        allocatedSpaces = [entry.toTuple() for entry in result]
        log.msg("Alocated: %s" % allocatedSpaces)
        log.msg(repr(self))
        return allocatedSpaces
        
    def releaseSpace(self, host, port, offset, length):
        log.msg("releaseSpace(%s, %d, %d, %d)" % (host, port, offset, length))
        self.memtable.append(FreeListEntry(host, port, offset, length))
        log.msg(repr(self))

    """
    If a storage server goes down or is replaced, the freelist
    must be notified about this in order to update the
    host/port combination.
    Note: this operation might take a bit longer with the
    current operation since there is no index the host/port
    combination.
    """
    def moveHost(self, fromHost, fromPort, toHost, toPort):
        log.msg("moveHost(%s, %d, %s, %d)" % (fromHost, fromPort, toHost, toPort))
        for entry in self.memtable:
            if entry.host == fromHost and entry.port == fromPort:
                entry.host = toHost
                entry.port = toPort
        log.msg(repr(self))
                
    def __repr__(self):
        return "Freelist:\t%s" % (self.memtable)
        
        
"""
For simple testing only...
"""
if __name__ == '__main__':
    import sys
    log.startLogging(sys.stdout)
    log.msg('Performing simple tests...')
    
    f = FreeList()
    
    hostA = ("A", 8080, 0, 10) # 100mb
    hostB = ("B", 8080, 0, 10) # 100mb
    
    f.releaseSpace(*hostA)
    f.releaseSpace(*hostB)
    
    def testAlloc(size):
        log.msg('-' * 50)
        log.msg(repr(f))
        log.msg("Allocate:\t%d bytes" % size)
        log.msg("Result:\t%s" % f.allocSpace(size))
        log.msg(repr(f))
        log.msg('-' * 50)

    testAlloc(3)
    testAlloc(12)
    testAlloc(8)
    testAlloc(4)
    f.moveHost('A', 8080, "R", 9999)
    log.msg(repr(f))
    
    
    