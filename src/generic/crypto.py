from hashlib import sha1
import time

from generic.communication_pb2 import HashedStorageHeader

PRIVATE_HASH_KEY = "BLABLABLA"
HASH_EXPIRE_SECONDS = 30

def signAndTimestampHashedStorageHeader(signedHeader):
    signedHeader.header.requestTimestamp = int(time.time())
    signedHeader.hashAlgorithm = HashedStorageHeader.SHA1
    sha1hash = sha1(signedHeader.header.SerializeToString() + PRIVATE_HASH_KEY)
    signedHeader.hash = sha1hash.digest()
    
"""
Returns True if a signed header is valid
otherwise a error message string.
"""
def validateHashedStorageHeader(signedHeader):
    if signedHeader.hashAlgorithm != HashedStorageHeader.SHA1:
        return "Unsupported hash, only SHA1 is supported"
    sha1hash = sha1(signedHeader.header.SerializeToString() + PRIVATE_HASH_KEY)
    if signedHeader.hash != sha1hash.digest():
        return "Incorrect hash"
    timediff = int(time.time()) - signedHeader.header.requestTimestamp
    if timediff > HASH_EXPIRE_SECONDS:
        return "Hash key expired %d seconds" % timediff
    return True
    