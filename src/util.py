"""
helper functions
"""
def xorBytes(a, b):
    assert len(a) == len(b)
    return ''.join(chr(ord(x) ^ ord(y)) for (x,y) in zip(a, b))
    
def byteValue(bytes):
    return [ord(byte) for byte in bytes]

def truncateString(data, length=50):    
    return (data[:length] + '..') if len(data) > length else data