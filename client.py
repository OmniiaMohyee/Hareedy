#Client

import socket 
import os
import sys



def download(s,client_id):

	print("Enter the file name")
	file_name = input()

	if file_name != 'q':
		s.send(bytes(str(client_id),'utf-8'))
		s.send(bytes(file_name,'utf-8'))
		print(file_name)
		data = s.recv(1024).decode('utf-8')
		print(data[:6])
		if data[:6] == 'EXISTS':
			file_size = float(data[6:])
			print("File Exists " + str(file_size)+" bytes, download? [Y/N]")
			message = input()

			if message == 'Y':
				s.send(bytes('OK','utf-8'))
				f = open(file_name,'wb')
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

				print("Download is complete")
		else:
			print("File does not exist!")
	s.close()
	return

def upload(s,client_id):

	print("Enter the file name")
	file_name = input()


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
				while bytesToSend != "":
					bytesToSend = f.read(1024)
					s.send(bytesToSend)

	s.close()
	return 


def main(client_id):

	print("hello, you're user with id" +str(client_id))

	print("Hello, for file upload enter U and for file downlaod enter D")
	

	
	choice = input()
	host = '127.0.0.1'
	port = 20000
	data_node_port = 0 
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
		data_node_port = int(s.recv(1024).decode('utf-8'))


		data_s = socket.socket()
		data_s.connect((host,data_node_port))
		download(data_s,client_id)
	return 
	

if __name__ == '__main__':
	
	client_id = int(sys.argv[1])
	main(client_id)


				