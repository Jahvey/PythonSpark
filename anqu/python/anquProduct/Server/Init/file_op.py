#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : lgy
# dateTime : 2016-07-29

import sys
sys.path.append('/home/mysql1/anqu/python/anquProduct/Server')
reload(sys)
sys.setdefaultencoding('utf8')
import config


class file_op():
	def __init__(self):
		self.caFile = config.categoryFilePath
		self.searchFile = config.searchapp_cn
		# add your data file path in the follow:

	def get_fileData(self,file):
		c_data = []
		with open(file,'r') as f:
			line = f.readline()
			while line != None and len(line) > 0:
				c_data.append(line[0:len(line)-2].split(' '))
				line = f.readline()
		return c_data

	def get_fileDatasharp(self,file):
		c_data = []
		with open(file,'r') as f:
			line = f.readline()
			while line != None and len(line) > 0:
				c_data.append(line[0:len(line)-2].split('###'))
				line = f.readline()
		return c_data

	def get_searchSourcedata(self):
		c_data = []
		with open(self.searchFile,'r') as f:
			line = f.readline()
			while line != None and len(line) > 0:
				data = line[0:len(line)-4].split('###')
				yield data
				c_data.append(data)
				line = f.readline() 
		# return c_data

def main():
	f_op = file_op()
	# # data = f_op.get_searchSourcedata()
	# for data in f_op.get_category():
	# 	print data[0]

if __name__ == '__main__':
	main()
