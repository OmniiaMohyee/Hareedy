import mysql.connector as mysql
import zmq
import sys
import time
import random
import json
from multiprocessing import Process

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
        node_port = data["data_nodes"]["ports"][(selected_id)*num_ports]
    return selected_id, node_addr, node_port

def NotifyMachineDataTransfer(src, dst, f_name):
    if (dst != None):
        dst_id, dst_node_addr,dst_node_port = dst
        with open('config.json') as config_file:
            data = json.load(config_file)
            num_ports = data["num_data_node_ports"]
            src_node_addr = data["data_nodes"]["address"][src-1]
            src_node_port_p1 = data["data_nodes"]["ports"][(src-1)*num_ports]
            src_node_port_p2 = data["data_nodes"]["ports"][(src-1)*num_ports + 1]
            dst_node_port_p2 = data["data_nodes"]["ports"][(dst_id)*num_ports + 1]

        context = zmq.Context()
        context2 = zmq.Context()

        print("send msg to src on port %s" %src_node_port_p2)
        socket = context.socket(zmq.PAIR)
        socket.connect("tcp://%s:%s" % (src_node_addr, src_node_port_p2))
        socket.send_string("send %s %s %s" % (f_name, dst_node_addr, dst_node_port))

        print("send msg to dst on port %s" %dst_node_port_p2)
        socket2 = context2.socket(zmq.PAIR)
        socket2.connect("tcp://%s:%s" % (dst_node_addr, dst_node_port_p2))
        socket2.send_string("recieve %s %s %s" % (f_name, src_node_addr, src_node_port_p1))
        time.sleep(0.5)
    return

if __name__ == "__main__":
    db = mysql.connect(host="localhost", user="root", passwd="hydragang", database="data_nodes")
    cursor = db.cursor()
    try:
        cursor.execute("SELECT a.file_name, a.node_number FROM file_table a WHERE file_name IN (SELECT distinct file_name FROM file_table b) group by file_name")
        files =cursor.fetchall()
        for file in files: #for each distinct file instances:    
            count = getInstanceCount(files[0][0]) 
            if (count < 3):
                print (files[0][0])
                src = getSourceMachine(files[0][0])
                dst1 = selectMachineToCopyTo(files[0][0],0)
                dst2 = selectMachineToCopyTo(files[0][0],1)
                NotifyMachineDataTransfer(src, dst1, files[0][0])
                NotifyMachineDataTransfer(src, dst2, files[0][0])
    except mysql.Error as err:
        print("Something went wrong: {}".format(err))
        cursor.close()
    print("END")