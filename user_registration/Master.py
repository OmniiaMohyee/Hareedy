import zmq
import time
import json
import threading
from InterfaceDB import InterfaceDB



# initialize needed values from json file
with open('config.json') as config_file:  
        data = json.load(config_file)
        pairPorts=data["master_slave"]["pair_ports"]
        readPort = data["master_slave"]["repreq_ports"][0]
        masterIP=data["master_slave"]["address"][0]
        writePort = data["master_slave"]["write_port"]
        dbName=data["database_info"]["db_name"][0]
        dbPass=data["database_info"]["db_pass"]

#create dbInterace
dbInt= InterfaceDB("root",dbPass,"localhost",dbName)


# Thread function that listens for Client read (login) requests
def readListener(context,readPort):
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:%s" % readPort)
    while True:
        readParam = socket.recv().decode("utf-8")
        # readParam=readParam.decode("utf-8")
        un_r, pas_r = readParam.split()
        print("Received read request: ", un_r,pas_r)
        status_r=dbInt.authUser(un_r,pas_r)
        socket.send_string(str(status_r))


print("my port is",readPort)

#initialize sockets 
context = zmq.Context()
pairSocket1 = context.socket(zmq.PAIR)
pairSocket1.bind("tcp://*:%s" %  pairPorts[0])
pairSocket2 = context.socket(zmq.PAIR)
pairSocket2.bind("tcp://*:%s" %  pairPorts[1])
writeSocket = context.socket(zmq.REP)
writeSocket.bind("tcp://*:%s" % writePort)


#start readlistenenr thread
listener_r = threading.Thread(target=readListener, args=(context,readPort))
listener_r.start()

#Wait for slaves to connect
mess=pairSocket1.recv()
print(mess)
mess=pairSocket2.recv()
print(mess)

while True:
    print("waiting recv")
    writeParam = writeSocket.recv().decode("utf-8")
    un_w,pas_w,em_w=writeParam.split()
    print("Received write request: ", un_w,pas_w,em_w)
    #should evaluate request here and respone should be sent to client
    #according to response, command is to be replicated to alll slaves
    status_w=dbInt.addUser(un_w,em_w,pas_w)
    writeSocket.send_string(str(status_w))
    if status_w==1: #successful op!
        print("Replicating to slaves!")
        pairSocket1.send_string(writeParam)
        pairSocket2.send_string(writeParam)

 
 
listener_r.join()