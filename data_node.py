import mysql.connector as mysql
import zmq
import sys
import time
import random
from multiprocessing import Process
import json
import os
import signal
import threading
import socket

local_host = '127.0.0.1'
context = zmq.Context()
# Publishes ALIVE messages to the port specified by the port number port every 1 second.
# Each message is preceded by the id of the sending node.
def send_alive_messages(node_id, address, ports):
    # context = zmq.Context() 
    global context
    socket = context.socket(zmq.PUB)
    port = ""
    while(port == ""):
        for sample_port in ports:
            try:
                socket.connect("tcp://%s:%s" % (address, sample_port))
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

######===================== For Replication Part ================ #######
def add_file(file_name,client_id,data_node_id,master_addr):
    file_log_port = 20010
    ip = '127.0.0.1'
    s = socket.socket()
    s.connect((master_addr,file_log_port))
    print("going to add file! data node id is"+str(data_node_id))
    nid_cid_fname = str(data_node_id )+ '#'+str(client_id)+'#'+file_name
    s.send(bytes(nid_cid_fname,'utf-8'))
    status = s.recv(1024)
    print("file added into DB!")    

def recieve_duplicate(recieve_deplicate_port,f_name,client_id,my_id,master_addr):

    print ("Recieving file...")
    # context = zmq.Context() 
    global context
    try:
        socket = context.socket(zmq.PAIR)
        print(recieve_deplicate_port)
        socket.bind("tcp://*:%s" % recieve_deplicate_port)
        recieved_file = socket.recv().decode("utf-8")
        print("recieved file data")
        add_file(f_name,client_id,my_id,master_addr)
        f = open(f_name,'wb')#+"rep_node"+str(my_id)+".txt"
        f.write(recieved_file)
        socket.close()
    except zmq.ZMQError:
        print("Error in rec dup")
    

def send_duplicate(f_name,dst_addr,dst_port):
    print ("Sending file...")
    # context = zmq.Context() 
    global context
    socket = context.socket(zmq.PAIR)
    print(dst_addr,dst_port)
    socket.connect("tcp://%s:%s" % (str(dst_addr),str(dst_port)))
    with open(f_name,'rb') as f:
        send_file = f.read(1024)
        socket.send_string(send_file)
        while send_file != "":
            send_file = f.read(1024)
            socket.send_string(send_file)
    print("file was sent successfully! yaaa")


def replicate(rec_from_master_port,recieve_deplicate_port,my_id,num_ports,master_addr): #wait or replicate msg from master tracker
    print ("Data Node in 'replicate' func, listening on port %s" %rec_from_master_port)
    # context = zmq.Context() 
    global context
    socket1 = context.socket(zmq.PAIR)
    socket1.bind("tcp://*:%s" %rec_from_master_port)   
    while True:
        print("Waiting to recieve",rec_from_master_port)
        msg = socket1.recv().decode("utf-8")
        print("yes.. i recieved a message from master tracker")
        SorR, f_name, client_id, node_addr, node_port  = msg.split()
        SorR = str(SorR) 
        if (SorR == "recieve"):#use 3rd port for each data node
            recieve_port = recieve_deplicate_port + node_port
            reciever = Process( target=recieve_duplicate, args=(recieve_port,f_name,client_id,my_id,master_addr), daemon=True)
            reciever.start()
        else:
            sender = Process( target=send_duplicate, args=(f_name,node_addr,str(int(node_port)+my_id)), daemon=True)
            sender.start()

######===================== For Client Upload/Download Part ================ #######
def add_file_client(file_name,client_id,data_node_id,sock,master_addr):
    file_log_port = 20010
    ip = '127.0.0.1'
    s = socket.socket()
    s.connect((master_addr,file_log_port))
    print("going to add file! data node id is"+str(data_node_id))
    nid_cid_fname = str(data_node_id )+ '#'+str(client_id)+'#'+file_name
    s.send(bytes(nid_cid_fname,'utf-8'))
    status = s.recv(1024)
    sock.send(status)


def download(name,sock):
    file_name,client_id = sock.recv(1024).decode('utf-8').split('#')
    client_id = int(client_id)
    print(file_name)
    print(os.path.isfile(file_name))
    if os.path.isfile(file_name):
        sock.send(bytes("EXISTS" + str(os.path.getsize(file_name)),'utf-8'))    
        user_response = sock.recv(1024).decode('utf-8')
        if user_response[:2] == 'OK':
            with open(file_name,'rb') as f:
                bytesToSend = f.read(1024)
                sock.send(bytesToSend)
                while bytesToSend != "":
                    bytesToSend = f.read(1024)
                    sock.send(bytesToSend)
    else:
        sock.send(bytes("ERR",'utf-8'))
    sock.close()

def upload(name,sock,data_node_id,master_addr):
    name_size = sock.recv(2048).decode('utf-8')
    file_name, file_size,client_id = name_size.split('#')
    print(file_size)
    print(client_id)
    file_size = int(file_size)
    client_id = int(client_id)
    add_file_client(file_name,client_id,data_node_id,sock,master_addr)
    f = open(file_name,'wb')
    data = sock.recv(1024)  
    totalrecv = len(data)
    f.write(data)
    tmp = 0
    while totalrecv < file_size:
        data = sock.recv(1024)
        totalrecv += len(data)
        f.write(data)
        percent = int((totalrecv/float(file_size))*100)
        if percent != tmp:
            print(str(percent)+"% Done.")
            tmp = percent
    print("Upload is Complete.")
    

def client_upload(ip,node_to_client_up_port,data_node_id,master_addr):
    s = socket.socket()
    s.bind((ip,node_to_client_up_port))
    s.listen(5)
    print('data node upload socket started!')
    while True :
        c,addr = s.accept()        
        print("Client connected to upload socket with ip :<"+str(addr)+">")
        t = threading.Thread(target = upload , args = ("uploadThread",c,data_node_id,master_addr))
        t.start()
    s.close()

def client_download(ip,node_to_client_down_port):
    s = socket.socket()
    s.bind((ip,node_to_client_down_port))
    s.listen(5)
    print('data node download socket started!')
    while True :
        c,addr = s.accept()
        print("Client connected to download socket with ip :<"+str(addr)+">")
        t = threading.Thread(target = download , args = ("downloadThread",c))
        t.start()
    s.close()


if __name__ == "__main__":
    data_node_id = int(sys.argv[1])  # Should probably check the argument is given first.
    # data_node_id = random.randrange(1, 1000)
    with open('config.json') as config_file:  # Should probably check this exists first.
        data = json.load(config_file)
        master_addr = data["master_trackers"]["address"]
        ports = data["master_trackers"]["ports"]
        num_ports = data["num_data_node_ports"]
        rec_from_master_port = data["data_nodes"]["management_ports"][data_node_id*num_ports]#1st port for sending
        recieve_deplicate_port = data["data_nodes"]["management_ports"][data_node_id*num_ports + 2]#3rd port for listening on
        node_to_client_up_port = int(data["data_nodes"]["client_ports"][data_node_id*3])
        node_to_client_down_port1 = int(data["data_nodes"]["client_ports"][data_node_id*3+1])
        node_to_client_down_port2 = int(data["data_nodes"]["client_ports"][data_node_id*3+2])
    
    alive_sender = Process(target=send_alive_messages, args=(data_node_id, master_addr, ports), daemon=True)
    alive_sender.start()

    client_upload_server = Process(target = client_upload, args=(local_host,node_to_client_up_port,data_node_id,master_addr))
    client_upload_server.start()
    client_downlaod_server1 = Process(target = client_download, args=(local_host,node_to_client_down_port1))
    client_downlaod_server1.start()
    client_downlaod_server2 = Process(target = client_download, args=(local_host,node_to_client_down_port2))
    client_downlaod_server2.start()
    
    replicate_reciever = Process(target=replicate, args=(rec_from_master_port,recieve_deplicate_port,data_node_id,num_ports,master_addr))
    replicate_reciever.start()
    while True:
        # print("Data node should do other stuff here.")
        time.sleep(2)
