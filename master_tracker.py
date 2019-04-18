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
        user="test",
        passwd="test"
    )
    cursor = db.cursor()
    cursor.execute("CREATE DATABASE data_nodes")


def init_data_nodes_tables():
    db = mysql.connect(
        host="localhost",
        user="test",
        passwd="test",
        database="data_nodes"
    )

    cursor = db.cursor()
    cursor.execute("CREATE TABLE `file_table` (`user_id` INT NOT NULL, `file_name` VARCHAR(255) NOT NULL, `node_number` INT, `file_path` VARCHAR(255), `is_node_alive` BOOLEAN, `last_modified` TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")


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
    db = mysql.connect(host="localhost", user="test", passwd="test", database="data_nodes")
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
            cursor.execute("UPDATE file_table SET is_node_alive = FALSE WHERE last_modified < NOW() - INTERVAL 1 MINUTE")
            db.commit()
            # print(cursor.rowcount)


if __name__ == "__main__":
    with open('config.json') as config_file:  # Should probably check this exists first.
        data = json.load(config_file)
        master_addr = data["master_trackers"]["address"]
        ports = data["master_trackers"]["ports"]
    port = ports[int(sys.argv[1])]  # Should probably check the argument is given first.
    active_listener = Process(
        target=listen_to_alive_messages, args=(master_addr, port,), daemon=True)
    active_listener.start()
    while True:
        print("Master tracker rollin' yon way.")
        time.sleep(0.5)


# listen_to_alive_messages(9232)
