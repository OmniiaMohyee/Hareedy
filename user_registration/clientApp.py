import zmq
import sys
import os
import json
import random
from mysql.connector import errorcode

# initialize needed values from json file
with open('../config.json') as config_file:  
        data = json.load(config_file)
        readPorts = data["master_slave"]["repreq_ports"]
        writePort=data["master_slave"]["write_port"]
        readIPs=data["master_slave"]["address"]
        writeIP=data["master_slave"]["address"][0]


#randomize starting readServer!
starting=random.randint(0,2) #chooses a num between 0 to 2
readPorts[0],readPorts[starting]=readPorts[starting],readPorts[0]
readIPs[0],readIPs[starting]=readIPs[starting],readIPs[0]

#initialize readsockets
timeout=400
context = zmq.Context()
readSocket = context.socket(zmq.REQ)
readSocket.setsockopt( zmq.RCVTIMEO, timeout) 
for i in range(3):
    readSocket.connect ("tcp://%s:%s" % (readIPs[i],readPorts[i]))

#initialize writesockets
writeSocket = context.socket(zmq.REQ)
writeSocket.setsockopt( zmq.RCVTIMEO, 5000)
writeSocket.connect ("tcp://%s:%s" % (writeIP,writePort))


#function that restarts sockets and rearranges connections in case one fails
def restart_socket():
    global readSocket, context,serv
    readSocket.close()
    readSocket = context.socket( zmq.REQ )
    readSocket.setsockopt( zmq.RCVTIMEO, timeout) 
    tempip=readIPs.copy()
    tempport=readPorts.copy()
    for i in range(3):
        readIPs[i]=tempip[(serv+i+1)%3]
        readPorts[i]=tempport[(serv+i+1)%3]
        readSocket.connect ("tcp://%s:%s" % (readIPs[i],readPorts[i]))
    serv=0

def inputData(field):
    data=input("Please enter "+field+":")
    while True:
        if(data.find(' ')!=-1):
            data=input(field+" can't have spaces, try again!: ")
        else:
            return data

def tryAgain( mess ):
    op=input("Would you like to "+mess+ "?? (y/n) ")
    while True:
        if op=='y' or op=='Y':
            return True
        elif op=='n' or op=='N':
            return False
        else:
            op=input("y or n! :")
    

        

serv=0   
def userLogin():
    global serv
    # while True:
    #take and verify user credintials
    username=inputData("Username")
    password=inputData("password")
    conTrials=0
    #authenticate user credintials
    while True:

        while True:
            try:
                readSocket.send_string (username + " "+ password) #  Get the reply.
                status = readSocket.recv().decode("utf-8")
                print(status)
                status=int(status)
                serv+=1
                serv=serv%3
                break
            except zmq.Again:
                if conTrials>2: # tried conncting with allll servers but no user!
                    print("Cannot connect right now! try again later.")
                    return False
                restart_socket()
                conTrials+=1
                continue

        if status== 1:
            print("Successful Login! ")
            return True
        elif status== 2:
            print("Wrong UserName!")
            if tryAgain("try again"):
                username=inputData("Username")
                continue
            else:
                return False
        elif status==3:
            print("Wrong Password!")
            if tryAgain("try again"):
                password=inputData("password")
                continue
            else:
                return False
        else:
            print("Oooppss! Seems like something is wrong with out DB! Try again later")
            return False

def userSignup():
    username=inputData("Username")
    email= inputData("Email")
    password=inputData("Password")
    #authenticate user credintials
    
    while True:
        try:
            writeSocket.send_string (username + " "+ password+ " "+ email) #  Get the reply.
            status = writeSocket.recv().decode("utf-8")
            print(status)
            status=int(status)
        except zmq.Again:
            print("It seems server is down :(, can't sign you up now. Try again later")
            return False

        if status== 1:
            print("Successful Signup! ")
            return True
        elif status== 2:
            print("Username or email already exists!")
            if tryAgain("try again"):
                username=inputData("Username")
                email= inputData("Email")
                continue
            else:
                return False
        else:
            print("Oooppss! Seems like something is wrong with out DB! Try again later")
            return False



def chooseOp( mess ):
    print("     1 - Login")
    print("     2 - Signup")
    print("     3 - Quit")
    return input(mess+" ( 1 or 2 or 3)")

            






#should probably find a way to check that all servers are down



notConnected=True

print("Welcome my fellow Human! ^_^")
print("How Would you like to proceed?")
option = chooseOp("Please enter a number!")
connected=False
while True:
    if option=='1':
        if userLogin():
            print("Done login")
            connected=True
            break
        elif tryAgain("Choose another op"):
            option=chooseOp("Please enter a number!")
            continue
        else:
            option='3'
            continue
    elif option=='2':
        if userSignup():
            print("Done signup")
            connected=True
            break
        elif tryAgain("Choose another op"):
            option=chooseOp("Please enter a number!")
            continue
        else:
            option='3'
            continue
    elif option=='3':
        print("bye then!")
        connected=False
        break
    else:
      option= chooseOp("Please enter a valid option number!")
 


while True:
    continue
readSocket.close()

     

   