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
        print("Node %d: Sending %s to %s:%s" % (node_id, message_str, address, port))
        socket.send_string(message_str)
        time.sleep(1)


if __name__ == "__main__":
    client_id = int(sys.argv[1])  # Should probably check the argument is given first.
    # client_id = random.randrange(1, 1000)
    with open('config.json') as config_file:  # Should probably check this exists first.
        data = json.load(config_file)
        master_addr = data["master_trackers"]["address"]
        ports = data["master_trackers"]["ports"]
    alive_sender = Process(target=send_alive_messages, args=(client_id, master_addr, ports,), daemon=True)
    alive_sender.start()
    while True:
        print("Data node should do other stuff here.")
        time.sleep(2)
