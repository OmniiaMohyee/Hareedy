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

data_node_id = 0

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
        print("Node %d: Sending %s to %s:%s" % (node_id, message_str, address, port))
        socket.send_string(message_str)
        time.sleep(1)

######===================== For Replication Part ================ #######
def send_to_node(msg,sock):
    return 

def recv_from_node(sock):
    return

def recieve_duplicate1(recieve_deplicate_port):
    print ("Recieving Replica...")
    # local_port = data["data_nodes"]["management_ports"][my_id*num_ports+2]
    context = zmq.Context() 
    socket = context.socket(zmq.PAIR)
    socket.bind("tcp://*:%s" % recieve_deplicate_port)
    msg2 = socket.recv()
    print(msg2)

def send_duplicate1(msg,dst_addr,dst_port):
    context = zmq.Context() 
    socket = context.socket(zmq.PAIR)
    socket.connect("tcp://%s:%s" % (str(dst_addr),str(dst_port)))
    #socket.send_string("here's the file.. bel hana we shefa") #To be done: send files not just string

    print("file was sent successfully! yaaa")

## we need to know the file name here.
# def recieve_duplicate(recieve_deplicate_port):
#     print ("Recieving Replicate...")
#     s = socket.socket()
#     s.bind((local_host,int(recieve_deplicate_port)))
#     s.listen(5)
#     while True:
#         c,addr = s.accept()
#         t = threading.Thread(target = recv_from_node , args = (c))
#         t.start()
#     s.close()

# def send_duplicate(msg,dst_addr,dst_port):
#     s = socket.socket()
#     s.connect((local_host,int(dst_port)))
#     t = threading.Thread(target = send_to_node , args = (msg,c))
#     t.start()
    
def recieve_duplicate(recieve_deplicate_port,f_name,client_id,my_id):
    print ("Recieving file...")
    # local_port = data["data_nodes"]["management_ports"][my_id*num_ports+2]
    context = zmq.Context() 
    socket = context.socket(zmq.PAIR)
    socket.bind("tcp://*:%s" % recieve_deplicate_port)
    msg2 = socket.recv()
    msg2.split()
    add_file(f_name,client_id,my_id)
    # print(msg2)

def send_duplicate(msg,dst_addr,dst_port):
    print ("Sending file...")
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
    
    while True:
        msg = socket1.recv_string()
        print("yes.. i recieved a message from master tracker")
        SorR, f_name, client_id, node_addr, node_port  = msg.split()
        SorR = str(SorR) 
        # socket2 = context.socket(zmq.PAIR)
        # open two threads here
        if (SorR == "recieve"):#use 3rd port for each data node
            m = "I should reccieve from %s : %s" %(node_addr,node_port)
            reciever = Process( target=recieve_duplicate, args=(recieve_deplicate_port,f_name,client_id,my_id), daemon=True)
            reciever.start()
        else:
            m = "I should send to node on %s:%s" %(node_addr,node_port)
            sender = Process( target=send_duplicate, args=(m,node_addr,node_port), daemon=True)
            sender.start()
    return
#should data node know master tracker's 

######===================== For Client Upload/Download Part ================ #######
def add_file(file_name,client_id,data_node_id,sock):
    # data_node_id = 0
    file_log_port = 20010
    ip = '127.0.0.1'
    s = socket.socket()
    s.connect((ip,file_log_port))

    print("going to add file! data node id is"+str(data_node_id))

    nid_cid_fname = str(data_node_id )+ '#'+str(client_id)+'#'+file_name
    s.send(bytes(nid_cid_fname,'utf-8'))
    status = s.recv(1024)
    sock.send(status)



def check_file(file_name,client_id):

    db = mysql.connect(host="localhost", user="root", passwd="12345678", database="data_nodes")
    cursor = db.cursor()
    cursor.execute("SELECT exists(select * from file_table where user_id = "+str(client_id)+" and file_name = '"+file_name+"' );")
    return (cursor.fetchall()[0][0] != 0)


def download(name,sock):
    client_id = int(sock.recv(1024).decode('utf-8'))
    file_name = sock.recv(1024).decode('utf-8')
    print(file_name)
    print(os.path.isfile(file_name))

    if os.path.isfile(file_name) and check_file(file_name,client_id):
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

def upload(name,sock,data_node_id):
    name_size = sock.recv(2048).decode('utf-8')
    file_name, file_size,client_id = name_size.split('#')
    print(file_size)
    print(client_id)
    file_size = int(file_size)
    client_id = int(client_id)

    add_file(file_name,client_id,data_node_id,sock)


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

    add_file(file_name,client_id,data_node_id,sock)
    

def client_upload(ip,node_to_client_up_port,data_node_id):
    s = socket.socket()
    s.bind((ip,node_to_client_up_port))

    s.listen(5)
    print('data node upload socket started!')
    while True :
        c,addr = s.accept()
        
        print("Client connected to upload socket with ip :<"+str(addr)+">")
        t = threading.Thread(target = upload , args = ("uploadThread",c,data_node_id))
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
        # send_deplicate_port = data["data_nodes"]["management_ports"][data_node_id*num_ports + 1]#2nd port for sending
        recieve_deplicate_port = data["data_nodes"]["management_ports"][data_node_id*num_ports + 2]#3rd port for listening on
        node_to_client_up_port = int(data["data_nodes"]["client_ports"][data_node_id*3])
        node_to_client_down_port1 = int(data["data_nodes"]["client_ports"][data_node_id*3+1])
        node_to_client_down_port2 = int(data["data_nodes"]["client_ports"][data_node_id*3+2])


    alive_sender = Process(target=send_alive_messages, args=(data_node_id, master_addr, ports,), daemon=True)
    alive_sender.start()

    client_upload_server = Process(target = client_upload, args=(local_host,node_to_client_up_port,data_node_id))   #node_to_client_up_port needs to be read from JSON!
    client_upload_server.start()
    client_downlaod_server1 = Process(target = client_download, args=(local_host,node_to_client_down_port1))
    client_downlaod_server1.start()
    client_downlaod_server2 = Process(target = client_download, args=(local_host,node_to_client_down_port2))
    client_downlaod_server2.start()
    
    replicate_reciever = Process(target=replicate, args=(rec_from_master_port,recieve_deplicate_port,data_node_id,data,num_ports))
    replicate_reciever.start()

    while True:
        # print("Data node should do other stuff here.")
        time.sleep(2)
