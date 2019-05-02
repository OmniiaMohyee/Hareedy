#Server

import socket
import threading
import os

def retrieve_file(name,sock):

	file_name = sock.recv(1024).decode('utf-8')
	print(file_name)
	print(os.path.isfile(file_name))
	if os.path.isfile(file_name):
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



def main():
	host = '127.0.0.1'
	port1 = 5000

	s = socket.socket()
	s.bind((host,port1))


	
	s.listen(1)
	print('Server Started :)')




	
	while True :
		c,addr = s.accept()
		
		print("Client connected ip :<"+str(addr)+">")

		


		#if mode == "D":
		t = threading.Thread(target = retrieve_file , args = ("retrThread",c))
		t.start()

	s.close()

if __name__ == '__main__':
	main()