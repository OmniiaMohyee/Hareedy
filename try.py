import zmq
import random
import sys
import time

port = "5556"
context = zmq.Context()
socket = context.socket(zmq.PAIR)
socket.bind("tcp://*:%s" % port)

while True:
    socket.send_string("Server message to client3")
    msg = socket.recv()
    print (msg)
    time.sleep(1)


# import zmq
# import random
# import sys
# import time

# port = "5556"
# context = zmq.Context()
# socket = context.socket(zmq.PAIR)
# socket.connect("tcp://192.168.43.148:%s" % port)

# while True:
#     msg = socket.recv()
#     print (msg)
#     socket.send_string("client message to server1")
#     socket.send_string("client message to server2")
#     time.sleep(1)

    