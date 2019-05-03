import sys
import zmq
import time
import threading
import json
from InterfaceDB import InterfaceDB


with open('../config.json') as config_file: 
        data = json.load(config_file)
        pairPort=data["master_slave"]["pair_ports"][int(sys.argv[1])-1]
        servPort = data["master_slave"]["repreq_ports"][int(sys.argv[1])]
        masterIP=data["master_slave"]["address"][0]
        dbName=data["database_info"]["db_name"][int(sys.argv[1])]
        dbPass=data["database_info"]["db_pass"]

        
#create dbInterace
dbInt= InterfaceDB("root",dbPass,"localhost",dbName)


def serverListener(context,pairPort):
    socket = context.socket(zmq.PAIR)
    socket.connect ("tcp://%s:%s" % (masterIP, pairPort))
    socket.send_string("i amm hereeee master!")
    while True:
        replicateParam = socket.recv().decode("utf-8")
        un_w,pas_w,em_w=replicateParam.split()
        status_w=dbInt.addUser(un_w,em_w,pas_w)
        if status_w==1:
                print("Successful ADD")
        else:
                print("god help us all.")




print("my port is",servPort)
context = zmq.Context()
repSocket = context.socket(zmq.REP)
repSocket.bind("tcp://*:%s" % servPort)



listener_s = threading.Thread(target=serverListener, args=(context,pairPort,))
listener_s.start()

print("I started a thread")

while True:
        readParam = repSocket.recv().decode("utf-8")
        un_r, pas_r = readParam.split() 
        print("Received read request: ", un_r,pas_r)
        status_r=dbInt.authUser(un_r,pas_r)
        repSocket.send_string(str(status_r))


listener_s.join()
