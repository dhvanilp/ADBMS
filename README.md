
# ADBMS 

## Run the project

### Create a virtulenv for python2 and install requirements in requirements.txt
```
virtualenv venv -p python2
source venv/bin/activate
pip install -r requirements.txt
```
You need to activate the virtual environment before running each server.

### Generate the key pair in src/sslcert
```
cd src/sslcert
openssl req -newkey rsa:2048 -new -nodes -x509 -days 3650 -keyout key.pem -out cert.pem
```



### Storage servers:
```
python storageserver.py -p 8080 -a 8081 -d server1.bin
python storageserver.py -p 8082 -a 8083 -d server2.bin
python storageserver.py -p 8084 -a 8085 -d server3.bin
python storageserver.py -p 8086 -a 8087 -d server4.bin
python storageserver.py -p 8088 -a 8089 -d server5.bin
```

### Freelist:
```
python freelistserver.py -p 8888
```

### Dictionary (master, 3 slaves):
```
python dictionaryserver.py -p 8000 -a 8001 -w 9990
python dictionaryserver.py -p 8002 -a 8003 -w 9991
python dictionaryserver.py -p 8004 -a 8005 -w 9992
python dictionaryserver.py -p 8006 -a 8007 -w 9993
```

### Manager:
```
python managerserver.py
```

### Commandline client:
```
python
from client import *
key = store("DATA")
print retrieve(key)
```

### Webclient (runs always on 7777):
```
python webclient.py
```