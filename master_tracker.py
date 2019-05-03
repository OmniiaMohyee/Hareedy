import mysql.connector as mysql
import zmq
import sys
import time
import random
import json
from multiprocessing import Process


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
    cursor.execute("CREATE TABLE `file_table` (`user_id` INT NOT NULL, `file_name` VARCHAR(255) NOT NULL, `node_number` INT, `file_path` VARCHAR(255), `is_node_alive` BOOLEAN, ``` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")


def init_system():
    init_data_nodes_database()
    init_data_nodes_tables()


# This function subscribes to the data node's alive messages.
def listen_to_alive_messages(address, port):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect("tcp://%s:%s" % (address, port))
    socket.setsockopt_string(zmq.SUBSCRIBE, '')
    print("Listening to ALIVE messages on %s:%s.." % (address, port))
    db = mysql.connect(host="localhost", user="root", passwd="hydragang", database="data_nodes")
    cursor = db.cursor()
    while True:
        try:
            received_message = socket.recv_string(flags=zmq.NOBLOCK)
            node_id, message = received_message.split()
            node_id = int(node_id)
            print("Tracker: received %s on %s:%s" % (received_message, address, port))
            cursor.execute("UPDATE file_table SET is_node_alive = TRUE WHERE node_number=%d" % node_id)
            db.commit()
            # print(cursor.rowcount)
        except zmq.Again as e:
            cursor.execute("UPDATE file_table SET is_node_alive = FALSE WHERE ` < NOW() - INTERVAL 1 MINUTE")
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
    cursor.execute("SELECT node_number  FROM file_table WHERE last_modified= (SELECT MIN(`last_modified`) FROM file_table WHERE file_name=%s)", [f_name])
    source =cursor.fetchall()
    source = int(source[0][0])
    return source

def selectMachineToCopyTo(f_name,offset):
    db = mysql.connect(host="localhost", user="root", passwd="hydragang", database="data_nodes")
    cursor = db.cursor()
    cursor.execute("SELECT node_number FROM file_table WHERE (node_number NOT IN(SELECT node_number FROM file_table WHERE file_name=%s)) AND is_node_alive = TRUE", [f_name])
    selected =cursor.fetchall()
    if (not selected or (len(selected)==1 and offset ==1) ):
        print("no more available data nodes to replicate at")
        return
    else :
        selected = selected[0+offset][0]
    with open('config.json') as config_file:
        data = json.load(config_file)
        num_ports = data["num_data_node_ports"]
        selected_id = selected-1 #just bcz it's not zero_indexed in database--may be changed later
        node_addr = data["data_nodes"]["address"][selected_id]  
        node_port = data["data_nodes"]["management_ports"][(selected_id)*num_ports+2]# dst recieve on 3rd port
    return selected_id, node_addr, node_port

def NotifyMachineDataTransfer(src, dst, f_name):
    if (dst != None):
        dst_id, dst_node_addr,dst_node_port = dst
        with open('config.json') as config_file:
            data = json.load(config_file)
            num_ports = data["num_data_node_ports"]
            src_node_addr = data["data_nodes"]["address"][src-1]
            src_node_port_p1 = data["data_nodes"]["management_ports"][(src-1)*num_ports]
            src_node_port_p2 = data["data_nodes"]["management_ports"][(src-1)*num_ports]
            dst_node_port_p2 = data["data_nodes"]["management_ports"][(dst_id)*num_ports]

        context = zmq.Context()
        context2 = zmq.Context()

        print("send msg to src on port %s" %src_node_port_p2)
        socket = context.socket(zmq.PAIR)
        socket.connect("tcp://%s:%s" % (src_node_addr, src_node_port_p2))#master connect with them on first port
        socket.send_string("send %s %s %s" % (f_name, dst_node_addr, dst_node_port)) #3rd port

        print("send msg to dst on port %s" %dst_node_port_p2)
        socket2 = context2.socket(zmq.PAIR)
        socket2.connect("tcp://%s:%s" % (dst_node_addr, dst_node_port_p2))
        socket2.send_string("recieve %s %s %s" % (f_name, src_node_addr, src_node_port_p1))
        # time.sleep(0.5)
        
    return

def replicate():
    # master connect with 1st port of each data node
    # data node send on 2nd port
    # and recieve on 3rd port
    db = mysql.connect(host="localhost", user="root", passwd="hydragang", database="data_nodes")
    cursor = db.cursor()
    try:
        cursor.execute("SELECT a.file_name, a.node_number FROM file_table a WHERE file_name IN (SELECT distinct file_name FROM file_table b) group by file_name")
        files =cursor.fetchall()
        for file in files: #for each distinct file instances:    
            count = getInstanceCount(file[0]) 
            if (count < 3):
                print (file[0])
                src = getSourceMachine(file[0])
                dst1 = selectMachineToCopyTo(file[0],0)
                dst2 = selectMachineToCopyTo(file[0],1)
                NotifyMachineDataTransfer(src, dst1, file[0])
                NotifyMachineDataTransfer(src, dst2, file[0])
                # time.sleep(0.5)
    except mysql.Error as err:
        print("Something went wrong: {}".format(err))
        cursor.close()

if __name__ == "__main__":
    with open('config.json') as config_file:  # Should probably check this exists first.
        data = json.load(config_file)
        master_addr = data["master_trackers"]["address"]
        ports = data["master_trackers"]["ports"]
    port = ports[int(sys.argv[1])]  # Should probably check the argument is given first.
    active_listener = Process(
        target=listen_to_alive_messages, args=(master_addr, port,), daemon=True)
    active_listener.start()
    replicator = Process(
        target = replicate, args = (), daemon=True)
    replicator.start()
    while True:
        # print("Master tracker rollin' yon way.")
        time.sleep(0.5)

# listen_to_alive_messages(9232)