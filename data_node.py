import mysql.connector as mysql
import zmq
import sys
import time
import random
from multiprocessing import Process
import json
import os
import signal


# Publishes ALIVE messages to the port specified by the port number port every 1 second.
# Each message is preceded by the id of the sending node.
def send_alive_messages(node_id, address, ports):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    port = ""
    for sample_port in ports:
        try:
            socket.bind("tcp://%s:%s" % (address, sample_port))
            print("Sending ALIVE messages to %s:%s" % (address, sample_port))
            port = sample_port
            break
        except zmq.ZMQError:
            print("Could not connect to %s:%s.. Trying another port-address.." % (address, sample_port))
    if port == "":
        print("Error: there are no free ports. Exiting..")
        os.killpg(os.getpgid(os.getpid()), signal.SIGTERM)  # This is done to kill the parent process as well.
        sys.exit()
    while True:
        message_str = str("%d %s" % (node_id, "ALIVE"))
        # print("Node %d: Sending %s to %s:%s" % (node_id, message_str, address, port))
        socket.send_string(message_str)
        time.sleep(1)

def recieve_duplicate(recieve_deplicate_port):
    print ("Recieving file...")
    # local_port = data["data_nodes"]["management_ports"][my_id*num_ports+2]
    context = zmq.Context() 
    socket = context.socket(zmq.PAIR)
    socket.bind("tcp://*:%s" % recieve_deplicate_port)
    msg2 = socket.recv()
    print(msg2)

def send_duplicate(msg,dst_addr,dst_port):
    context = zmq.Context() 
    socket = context.socket(zmq.PAIR)
    socket.connect("tcp://%s:%s" % (str(dst_addr),str(dst_port)))
    socket.send_string("here's the file.. bel hana we shefa") #To be done: send files not just string
    print("file was sent successfully! yaaa")

def replicate(rec_from_master_port,recieve_deplicate_port,my_id,data,num_ports): #wait or replicate msg from master tracker
    print ("Data Node in 'replicate' func, listening on port %s" %rec_from_master_port)
    context = zmq.Context() 
    socket1 = context.socket(zmq.PAIR)
    socket1.bind("tcp://*:%s" % rec_from_master_port)
    reciever = Process( target=recieve_duplicate, args=(recieve_deplicate_port,), daemon=True)
    reciever.start()
    while True:
        msg = socket1.recv_string()
        print("yes.. i recieved a message from master tracker")
        SorR, f_name, node_addr, node_port = msg.split()
        SorR = str(SorR) 
        # socket2 = context.socket(zmq.PAIR)
        # open two threads here
        if (SorR == "recieve"):#use 3rd port for each data node
            m = "I should reccieve from %s : %s" %(node_addr,node_port)
            # reciever = Process( target=recieve_duplicate, args=(m,recieve_deplicate_port,), daemon=True)
            # reciever.start()
        else:
            m = "I should send to node on %s:%s" %(node_addr,node_port)
            sender = Process( target=send_duplicate, args=(m,node_addr,node_port), daemon=True)
            sender.start()
    return

if __name__ == "__main__":
    data_node_id = int(sys.argv[1])  # Should probably check the argument is given first.
    # data_node_id = random.randrange(1, 1000)
    with open('config.json') as config_file:  # Should probably check this exists first.
        data = json.load(config_file)
        master_addr = data["master_trackers"]["address"]
        ports = data["master_trackers"]["ports"]
        num_ports = data["num_data_node_ports"]
        rec_from_master_port = data["data_nodes"]["management_ports"][data_node_id*num_ports]#1st port for sending
        # send_deplicate_port = data["data_nodes"]["management_ports"][data_node_id*num_ports + 1]#2nd port for sending
        recieve_deplicate_port = data["data_nodes"]["management_ports"][data_node_id*num_ports + 2]#3rd port for listening on
    alive_sender = Process(target=send_alive_messages, args=(data_node_id, master_addr, ports,), daemon=True)
    alive_sender.start()

    replicate_reciever = Process(target=replicate, args=(rec_from_master_port,recieve_deplicate_port,data_node_id,data,num_ports))
    replicate_reciever.start()
    while True:
        # print("Data node should do other stuff here.")
        time.sleep(2)
