#Client

import socket 
import os

def download(s):

	print("Enter the file name")
	file_name = input()

	if file_name != 'q':
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

				print("Download is complete")

		else:
			print("File does not exist!")
	s.close()

def upload(s):

	print("Enter the file name")
	file_name = input()

	if file_name != 'q':
		if not(os.path.isfile(file_name)):
			print("Error! : invalid File Name")
		else:
			name_size = file_name + '#'+str(os.path.getsize(file_name))
			s.send(bytes(name_size,'utf-8'))

			with open(file_name,'rb') as f:
				bytesToSend = f.read(1024)
				s.send(bytesToSend)
				while bytesToSend != "":
					bytesToSend = f.read(1024)
					s.send(bytesToSend)

	s.close()


def main():

	print("Hello, for file upload enter U and for file downlaod enter D")
	

	
	choice = input()
	if choice == "U":
		print("going to upload")
		#s.send(bytes("U",'utf-8'))
		host = '127.0.0.2'
		port = 5000

		s = socket.socket()
		s.connect((host,port))
		upload(s)
	elif choice == "D":
		print("going to download")
		#s.send(bytes("D",'utf-8'))
		host = '127.0.0.1'
		port = 5000
		s = socket.socket()
		s.connect((host,port))
		download(s)

	

if __name__ == '__main__':
	main()

				