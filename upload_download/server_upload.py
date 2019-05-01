import socket
import os
import threading

def save_file(name,sock):

	name_size = sock.recv(2048).decode('utf-8')
	file_name, file_size = name_size.split('#')
	file_size = int(file_size)

	f = open("new_new_"+file_name,'wb')
	data = sock.recv(1024)	
	totalrecv = len(data)
	f.write(data)
	tmp = 0
	while totalrecv < file_size:
		data = sock.recv(1024)
		totalrecv += len(data)
		f.write(data)
		percent = int((totalrecv/float(file_size))*100)
		if percent != tmp:
			print(str(percent)+"% Done.")
			tmp = percent
		print("Upload is Complete.")

def main():
	host = '127.0.0.1'
	port = 5000

	s = socket.socket()
	s.bind((host,port))


	
	s.listen(5)
	print('Server Started :)')




	
	while True :
		c,addr = s.accept()
		
		print("Client connected ip :<"+str(addr)+">")

		t = threading.Thread(target = save_file , args = ("saveThread",c))

		t.start()

	s.close()

if __name__ == '__main__':
	main()