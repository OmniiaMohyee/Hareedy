#Client

import socket 
import os
import sys
import zmq
import json
import random
# from mysql.connector import errorcode

# initialize needed values from json file
with open('config.json') as config_file:  
	data = json.load(config_file)
	readPorts = data["master_slave"]["repreq_ports"]
	writePort=data["master_slave"]["write_port"]
	readIPs=data["master_slave"]["address"]
	writeIP=data["master_slave"]["address"][0]
	#client and mastertracker comm
	host = data["master_trackers"]["address"]
	port=data["master_trackers"]["clientports"]


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
        if(data.find(' ')!=-1 or data==""):
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
	global username,serv
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
	global username
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

            








def download(s,client_id,file_name):
	if file_name != 'q':
		fname_cid = file_name+'#'+str(client_id)
		s.send(bytes(str(fname_cid),'utf-8'))
		data = s.recv(1024).decode('utf-8')
		print(data[:6])
		if data[:6] == 'EXISTS':
			file_size = float(data[6:])
			print("File Exists " + str(file_size)+" bytes, download? [Y/N]")
			message = input()
			if message == 'Y':
				s.send(bytes('OK','utf-8'))
				f = open("new_"+file_name,'wb')
				data = s.recv(1024)
				totalrecv = len(data)
				f.write(data)
				tmp = 0
				while totalrecv < file_size:
					data = s.recv(1024)
					totalrecv += len(data)
					f.write(data)
					percent = int((totalrecv/float(file_size))*100)
					if percent != tmp:
						print(str(percent)+"% Done.")
						tmp = percent
				print("ٍSUCCESS!")
		else:
			print("File does not exist!")
	s.close()
	return

def upload(s,client_id):
	print("Enter the file name")
	file_name = input()
	total_send = 0
	if file_name != 'q':
		if not(os.path.isfile(file_name)):
			print("Error! : invalid File Name")
		else:	
			name_size_cid = file_name + '#'+str(os.path.getsize(file_name))+'#'+str(client_id)
			s.send(bytes(name_size_cid,'utf-8'))

			print("file name is "+ file_name)
			with open(file_name,'rb') as f:
				bytesToSend = f.read(1024)
				s.send(bytesToSend)
				while bytesToSend != "" and total_send <= os.path.getsize(file_name)  :
					bytesToSend = f.read(1024)
					total_send += 1024
					s.send(bytesToSend)
			f.close()
	status = s.recv(1024).decode('utf-8')
	print(status)
	s.close()
	return 


def main(client_id):
	global host,port
	print("hello, you're user with id" +str(client_id))
	print("Hello, for file upload enter U and for file downlaod enter D")	
	choice = input()
	# host = '127.0.0.1'
	# port = 20000
	# data_node_port = 0 
	s = socket.socket()
	s.connect((host,port))
	if choice == "U":
		print("going to upload")
		s.send(bytes("U",'utf-8'))
		data_node_port = int(s.recv(1024).decode('utf-8'))
		data_s = socket.socket()
		data_s.connect((host,data_node_port))
		upload(data_s,client_id)
	elif choice == "D":
		print("going to download")
		s.send(bytes("D",'utf-8'))
		print("Enter File Name: ")
		file_name = input()
		fname_cid = file_name+'#'+str(client_id)
		s.send(bytes(str(fname_cid),'utf-8'))
		msg = s.recv(1024).decode('utf-8')
		print(msg)
		if msg[:6] == 'EXISTS':		
			data_node_ports = s.recv(3072).decode('utf-8').split('#')
			print(data_node_ports)
			print("choose a port to download from")
			i = int(input())
		while i>len(data_node_ports) or i<1:
			print("enter a valid port number")
			i = int(input())
		data_s = socket.socket()
		data_s.connect((host,int(data_node_ports[i-1])))
		download(data_s,client_id,file_name)
	return 


if __name__ == '__main__':
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
	
	readSocket.close()

	if connected:	
		client_id = username
		main(client_id)