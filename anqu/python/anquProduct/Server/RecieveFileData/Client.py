#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : lgy
# dateTime : 2016-07-20

# import RecieveFile
# import sys
# import socket

# class listener():
# 	def __init__(self,Host='127.0.0.1',Port=5555):
# 		try:
# 			self.mysocket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
# 			self.mysocket.bind((Host,Port))
# 		except socket.error,msg:
# 			print 'Bind failed!\nError Code :'+str(msg[0])+ " Message" + msg[1]
# 			sys.exit()
# 		# finally:
# 			# print 'Socket Bind'

# 	def startServer(self):
# 		try:
# 			self.mysocket.listen(10)
# 			print 'Server start ... ...'
# 		except socket.error ,msg:
# 			print 'Server start Failed !Error Code : '+str(msg[0]) + 'Message' + msg[1]
# 			sys.exit()
# 		whileconn = self.mysocket.accept()
# 		print 'Connected with ' + addr[0] + ':' + str(addr)

# def main():
# 	listener_now = listener()
# 	listener_now.startServer()

# if __name__ == '__main__':
# 	main()

from socket import *	
import time

# host = '127.0.0.1'
host = '192.168.238.128'
port = 9998
bufsize = 1024
addr = (host,port)
client = socket(AF_INET,SOCK_STREAM)
client.connect(addr)
while True:
    # data = raw_input()
    data = '/home/mysql1/anqu/analysisResault/ClassWord/'
    if not data or data=='exit':
        break
    client.send('%s' % data)
    data = client.recv(bufsize)
    if not data:
        break
    print data.strip()+'\n'
    time.sleep(5)
client.close()


