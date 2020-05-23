from threading import Thread, currentThread
from time import time, sleep

from freelistclient import SimpleFreelistTestClient
from dictionarymanager import DictionaryAdminClient

from storagemanager import checkAllStorageServers
from storagemanager import stop as storageStop
from storagemanager import addServer as addStorageServer
from storagemanager import setDictionaryConnection, setFreelistConnection
from storagemanager import startNewGroup as startNewStorageGroup

from dictionarymanager import checkAllDictionaryServers
from dictionarymanager import stop as dictionaryStop
from dictionarymanager import connectServer as addDictionaryServer
from dictionarymanager import startNewReplicaGroup


HEARTBEAT_SECONDS = 10 # low for testing
HEARTBEAT_THREAD = None


def heartBeatJob():
    global HEARTBEAT_THREAD
    while HEARTBEAT_THREAD == currentThread():
        start = time()
        
        checkAllStorageServers()
        checkAllDictionaryServers()
        
        # check again in heartbeat time seconds (minus the time the job took)
        sleep_secs = max(HEARTBEAT_SECONDS - (int(time() - start)), 0)
        #print 'sleep %d seconds' % sleep_secs
        sleep(sleep_secs)
    print 'heartbeat thread stopped'
    
    
def stop():
    # stop heartbeat
    global HEARTBEAT_THREAD
    HEARTBEAT_THREAD = None
    
    storageStop()
    dictionaryStop()
    
    
def start():
    global HEARTBEAT_THREAD
    assert HEARTBEAT_THREAD is None
    HEARTBEAT_THREAD = Thread(target=heartBeatJob, name='HeartBeatJob')
    HEARTBEAT_THREAD.start()

def testSetup():
    global HEARTBEAT_THREAD
    assert HEARTBEAT_THREAD is not None
    
    # ----------------------------- CONFIG -----------------------------
    # dictionary server addresses (host, clientPort, adminPort):
    master = 'localhost', 8000, 8001
    slave1 = 'localhost', 8002, 8003
    slave2 = 'localhost', 8004, 8005
    
    # freelist address:
    freelist = 'localhost', 8888 # NOTE: also set in dictionary/dictionaryserver.py:24
    
    # storage addresses (host, clientPort, adminPort):
    storageServers = [
        ('localhost', 8080, 8081),
        ('localhost', 8082, 8083),
        ('localhost', 8084, 8085),
        ('localhost', 8086, 8087)
    ]
    # -------------------------- END CONFIG -----------------------------
    
    # setup everything
    addDictionaryServer(*master)
    addDictionaryServer(*slave1)
    addDictionaryServer(*slave2)
    
    startNewReplicaGroup()
    
    setFreelistConnection(SimpleFreelistTestClient(*freelist))
    setDictionaryConnection(DictionaryAdminClient(master[0], master[2]))
    
    for storageAddress in storageServers:
        addStorageServer(*storageAddress)
    
    startNewStorageGroup()
    
    
        