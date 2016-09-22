#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : lgy
# dateTime : 2016-07-20

import sys
from RecieveFile import RecieveFile as rf
import SocketServer  
from SocketServer import StreamRequestHandler as SRH  
from time import ctime  

# host = '127.0.0.1' 
host = '192.168.238.128' 
port = 9998  
addr = (host,port)
user = 'mysql1'
passwd = 'mysql'  
      
class Servers(SRH):  
	def handle(self):  
		print 'got connection from ',self.client_address  
		self.wfile.write('connection %s:%s at %s succeed!' % (host,port,ctime()))  
		while True:  
			data = self.request.recv(1024)  
			if not data:   
				break 
			readFile = rf()
			readFile.login(Host=self.client_address[0])
			print self.client_address,data,'\n'
			readFile.getFilestoLocal(data,'/home/mysql1/anqu/analysisResault/TestFile/')
			readFile.close()
			self.request.send(data)
			# break
print 'server is running....'
server = SocketServer.ThreadingTCPServer(addr,Servers)
server.serve_forever()


