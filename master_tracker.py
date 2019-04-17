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
    cursor.execute("CREATE TABLE `file_table` (`user_id` INT NOT NULL, `file_name` INT NOT NULL, `node_number` INT, `file_path` VARCHAR(255), `is_node_alive` BOOLEAN)")


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
    while True:
        received_message = socket.recv_string()
        node_id, message = received_message.split()
        node_id = int(node_id)
        print("Tracker: received %s on %s:%s" %
              (received_message, address, port))


if __name__ == "__main__":
    with open('config.json') as config_file:
        data = json.load(config_file)
        master_addr = data["master_trackers"]["address"]
        ports = data["master_trackers"]["ports"]
    port = ports[int(sys.argv[1])]
    active_listener = Process(
        target=listen_to_alive_messages, args=(master_addr, port,), daemon=True)
    active_listener.start()
    while True:
        print("Doing the work of tards")
        time.sleep(0.5)


# listen_to_alive_messages(9232)
