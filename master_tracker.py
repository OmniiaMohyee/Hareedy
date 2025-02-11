import mysql.connector as mysql
import zmq
import sys
import time
import random
import json
from multiprocessing import Process
import socket
import threading

data_node_sock = []
context = zmq.Context()

# send_rep=[][]

def init_data_nodes_database():
    db = mysql.connect(
        host="localhost",
        user="root",
        passwd="hydragang"
    )
    cursor = db.cursor()
    cursor.execute("CREATE DATABASE data_nodes")


def init_data_nodes_tables():
    db = mysql.connect(
        host="localhost",
        user="root",
        passwd="hydragang",
        database="data_nodes"
    )
    cursor = db.cursor()
    cursor.execute("CREATE TABLE `node_table` (`node_number` INT, `is_node_alive` BOOLEAN, `last_modified` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")
    cursor.execute("INSERT INTO node_table (node_number) VALUES (0),(1),(2)")
    cursor.execute("CREATE TABLE `file_table` (`user_id` INT NOT NULL, `file_name` VARCHAR(255) NOT NULL, `node_number` INT, `file_path` VARCHAR(255), `is_node_alive` BOOLEAN, `last_modified` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")


def init_system():
    init_data_nodes_database()
    init_data_nodes_tables()


# This function subscribes to the data node's alive messages.
def listen_to_alive_messages(address, port):
    global context
    # context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.bind("tcp://%s:%s" % (address, port))
    socket.setsockopt_string(zmq.SUBSCRIBE, '')
    print("Listening to ALIVE messages on %s:%s.." % (address, port))
    db = mysql.connect(host="localhost", user="root", passwd="hydragang", database="data_nodes")
    cursor = db.cursor()
    while True:
        try:
            received_message = socket.recv_string(flags=zmq.NOBLOCK)
            node_id, message = received_message.split()
            node_id = int(node_id)
            # print("Tracker: received %s on %s:%s" % (received_message, address, port))
            cursor.execute("UPDATE node_table SET is_node_alive = TRUE WHERE node_number=%d" % node_id)
            cursor.execute("UPDATE file_table SET is_node_alive = TRUE WHERE node_number=%d" % node_id)
            db.commit()
            # print(cursor.rowcount)
        except zmq.Again as e:
            cursor.execute("UPDATE node_table SET is_node_alive = FALSE WHERE last_modified < NOW() - INTERVAL 1 MINUTE")
            cursor.execute("UPDATE file_table SET is_node_alive = FALSE WHERE last_modified < NOW() - INTERVAL 1 MINUTE")
            db.commit()
            # print(cursor.rowcount)

######===================== For Replication Part ================ #######

def getInstanceCount(f_name):
    db = mysql.connect(host="localhost", user="root", passwd="hydragang", database="data_nodes")
    cursor = db.cursor()
    cursor.execute('SELECT count(*) FROM file_table WHERE file_name=%s', [f_name])
    count =cursor.fetchall()
    count = int(count[0][0])
    return count


def getSourceMachine(f_name):  
    db = mysql.connect(host="localhost", user="root", passwd="hydragang", database="data_nodes")
    cursor = db.cursor()
    cursor.execute("SELECT node_number  FROM file_table WHERE file_name=%s AND is_node_alive = TRUE", [f_name])
    source =cursor.fetchall()
    if(cursor.rowcount == 0):
        source = -1
        print("Couldn't find source for file: ",f_name)
    else:
        source = int(source[0][0])
        print("SRC Node for file %s is %s" % (f_name,source) )
    return source


def selectMachineToCopyTo(f_name,offset):
    db = mysql.connect(host="localhost", user="root", passwd="hydragang", database="data_nodes")
    cursor = db.cursor()
    cursor.execute("SELECT node_number FROM node_table WHERE (node_number NOT IN(SELECT b.node_number FROM node_table a LEFT JOIN file_table b ON a.node_number = b.node_number WHERE file_name=%s)) AND is_node_alive = TRUE", [f_name])
    selected =cursor.fetchall()
    print("selescted as dstination",selected)
    if (not selected or (len(selected)==1 and offset ==1) ):
        print("no more available data nodes to replicate at")
        return
    else:
        selected_id = selected[0+offset][0]
    with open('config.json') as config_file:
        data = json.load(config_file)
        num_ports = data["num_data_node_ports"]
        node_addr = data["data_nodes"]["address"][selected_id]  
        node_port = data["data_nodes"]["management_ports"][(selected_id)*num_ports+2]# dst recieve on 3rd port
    return selected_id, node_addr, node_port


def NotifyMachineDataTransfer(src, dst, f_name,client_id):
    global context
    if (dst != None):
        dst_id, dst_node_addr,dst_node_port = dst
        with open('config.json') as config_file:
            data = json.load(config_file)
            num_ports = data["num_data_node_ports"]
            src_node_addr = data["data_nodes"]["address"][src]
            src_node_port_p1 = data["data_nodes"]["management_ports"][src*num_ports]
            src_node_port_p2 = data["data_nodes"]["management_ports"][src*num_ports]
            dst_node_port_p2 = data["data_nodes"]["management_ports"][(dst_id)*num_ports]
        # context = zmq.Context()
        # context2 = zmq.Context()
        print("send msg to src on port %s and addr %s" %(src_node_port_p2,src_node_addr))
        socket = context.socket(zmq.PAIR)
        socket.connect("tcp://%s:%s" % (src_node_addr, src_node_port_p2))#master connect with them on first port
        socket.send_string("send %s %s %s %s" % (f_name, client_id, dst_node_addr, dst_node_port)) #3rd port
        
        print("send msg to dst on port %s and addr %s" %(dst_node_port_p2,dst_node_addr))
        socket2 = context.socket(zmq.PAIR)
        socket2.connect("tcp://%s:%s" % (dst_node_addr, dst_node_port_p2))
        socket2.send_string("recieve %s %s %s %s" % (f_name, client_id, src_node_addr, src_node_port_p1))
        socket.close() 
        socket2.close() 
    return


def replicate():
    # master connect with 1st port of each data node
    # data node send on 2nd port
    # and recieve on 3rd port
    db = mysql.connect(host="localhost", user="root", passwd="hydragang", database="data_nodes")
    cursor = db.cursor()
    while True:
        i =0
        try:
            cursor.execute("SELECT file_name,user_id FROM file_table WHERE file_name IN (SELECT distinct file_name FROM file_table) group by file_name")
            files =cursor.fetchall()
            # print("num of files: ",cursor.rowcount)
            for file in files: #for each distinct file instances:    
                count = getInstanceCount(file[0]) 
                print("count of existance = ", count)
                if (count < 3):
                    # print ("file number ",i, " with name: ", file[0]," count = ",count)
                    src = getSourceMachine(file[0])
                    if(src == -1):
                        break
                    dst1 = selectMachineToCopyTo(file[0],0)
                    dst2 = selectMachineToCopyTo(file[0],1)
                    # if not_operating1:
                    NotifyMachineDataTransfer(src, dst1, file[0],file[1])
                    # if not_operating1:    
                    NotifyMachineDataTransfer(src, dst2, file[0],file[1])
                    time.sleep(1)
                    i = i+1
        except mysql.Error as err:
            print("Error in replication function: {}".format(err))
            cursor.close()
        time.sleep(2)

######===================== For Client Upload/Download Part ================ #######

def wait_clients(port,data_node_sock):
    s = socket.socket()
    ip = '127.0.0.1'
    s.bind((ip,9600))
    s.listen(5)   
    while True :
        c,addr = s.accept()       
        print("A Client with IP:<"+str(addr)+"> has a request.")
        t = threading.Thread(target = get_client_request , args = ("saveThread",c,data_node_sock))
        t.start()
    s.close()
 

def check_file(file_name,client_id):
    db = mysql.connect(host="localhost", user="root", passwd="hydragang", database="data_nodes")
    cursor = db.cursor()
    cursor.execute("SELECT exists(select * from file_table where user_id = "+str(client_id)+" and file_name = '"+file_name+"' );")
    return (cursor.fetchall()[0][0] != 0)

def get_client_request(name,sock,data_node_sock):
    print("got here! \n")
    request_type = sock.recv(1024).decode('utf-8')
    print("request_type is " + str(request_type))
    print(data_node_sock)
    print(len(data_node_sock))
    db = mysql.connect(host="localhost", user="root", passwd="hydragang", database="data_nodes")
    cursor = db.cursor()
    cursor.execute("SELECT distinct node_number FROM file_table WHERE is_node_alive = TRUE;")
    nodes =cursor.fetchall()
    if request_type == 'U':
        i = random.randrange(0, 3, 6)
        i = 3
        sock.send(bytes(str(data_node_sock[i]),'utf-8'))
    else:
        file_name,client_id= sock.recv(2048).decode('utf-8').split('#')
        client_id = int(client_id)
        print(file_name)
        if check_file(file_name,client_id):
            sock_to_send = ""
            for node in nodes:
                sock_to_send += str(data_node_sock[node[0]*3+1])+'#'+str(data_node_sock[node[0]*3+2])+'#'
            sock_to_send = sock_to_send[0:len(sock_to_send)-1]
            print(sock_to_send)
            print("sended: EXISTS, Choose a port")
            sock.send(bytes("EXISTS, Choose a port",'utf-8'))
            sock.send(bytes(sock_to_send,'utf-8'))
        else:
            sock.send(bytes("File doesn't exist",'utf-8'))
            print("sended:File doesn't exist")

def add_file(s):
    data_node_id,client_id,file_name = s.recv(2048).decode('utf-8').split('#')
    print("adding file from master tracker "+data_node_id)
    db = mysql.connect(host="localhost", user="root", passwd="hydragang", database="data_nodes")
    cursor = db.cursor()
    cursor.execute("INSERT INTO file_table (user_id,node_number,file_name,file_path) VALUES ("+client_id+","+data_node_id+",'"+file_name+"','"+file_name+"');")
    db.commit()
    cursor.close()
    print("file added into DB!")
    s.send(bytes("SUCCESS!",'utf-8'))

def file_logger(file_log_port):
    s = socket.socket()
    ip = '127.0.0.1'
    s.bind((ip,file_log_port))
    s.listen(5)
    while True:
        c,addr = s.accept()
        t = threading.Thread(target = add_file,args =(c,))
        t.start()
    s.close()

#def ayhaga():
if __name__ == "__main__":
    client_port = 20000
    file_log_port = 20010
    with open('config.json') as config_file:  # Should probably check this exists first.
        data = json.load(config_file)
        master_addr = data["master_trackers"]["address"]
        ports = data["master_trackers"]["ports"]
        data_node_sock = data["data_nodes"]["client_ports"]
    for i in range(6):
        data_node_sock[i] = int(data_node_sock[i])
    db = mysql.connect(host="localhost", user="root", passwd="hydragang", database="data_nodes")
    cursor = db.cursor()
    active_listener0 = Process(
        target=listen_to_alive_messages, args=(master_addr, ports[0],), daemon=True)
    active_listener1 = Process(
        target=listen_to_alive_messages, args=(master_addr, ports[1],), daemon=True)
    active_listener2 = Process(
        target=listen_to_alive_messages, args=(master_addr, ports[2],), daemon=True)
    client_listener = Process( 
        target=wait_clients,args=(client_port,data_node_sock,),daemon=True)
    replicator = Process(
        target = replicate, args = (), daemon=True)
    file_log = Process(
        target = file_logger, args=(file_log_port,), daemon = True)
    active_listener0.start()
    active_listener1.start()
    active_listener2.start()
    client_listener.start()
    replicator.start()
    file_log.start()
    while True:
        # print("Master tracker rollin' yon way.")
        time.sleep(0.5)

#init_system()
