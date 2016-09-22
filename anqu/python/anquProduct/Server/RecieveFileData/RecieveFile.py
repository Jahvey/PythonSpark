#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : lgy
# dateTime : 2016-07-20

import sys
sys.path.append('/home/mysql1/anqu/python/code/Tools')
sys.path.append('/home/mysql1/anqu/python/insertDataHql')
reload(sys)
sys.setdefaultencoding('utf8') 

import ftplib
import os
from wordLanguedge import WordLanguedge as wl
from selectWord import selectWord as SW
from HqlSpark import HqlSpark as hs

class RecieveFile():
	def __init__(self):
		# self.Link_Host = '127.0.0.1'
		# self.Link_Port = 2020
		# self.filePath = '/home/mysql1/anqu/analysisResault/ClassWord'
		self.rhs = hs()
		self.chi = wl()
		# pass

	#login in 
	def login(self,Host='127.0.0.1',Port=21,user='mysql1',passwd='mysql'):
		self.conn = ftplib.FTP()
		self.conn.set_debuglevel(0)
		self.conn.connect(Host,Port)
		self.conn.login(user,passwd)
		self.conn.set_pasv(False)
		print self.conn.getwelcome()+'\nWelcome connect ftp!'

	# judge filepath is file or not 
	def _is_ftp_file(self,ftp_path):
		try:
			if ftp_path in self.conn.nlst(os.path.dirname(ftp_path)):
				return True
			else:
				return False
		except ftplib.error_perm,e:
			return False
	#file 
	def _ftp_list(self, line):
		list = line.split(' ')
		if self.ftp_dir_name==list[-1] and list[0].startswith('d'):
			self._is_dir = True
	#judge remote filepath id dir or not
	def _is_ftp_dir(self,ftp_path):
		ftp_path = ftp_path.rstrip('/')
		ftp_parent_path = os.path.dirname(ftp_path)
		self.ftp_dir_name = os.path.basename(ftp_path)
		self._is_dir = False
		if ftp_path == '.' or ftp_path== './' or ftp_path=='':
			self._is_dir = True
		else:
			try:
				self.conn.retrlines('LIST %s' %ftp_parent_path,self._ftp_list)
			except ftplib.error_perm,e:
				return self._is_dir   
			return self._is_dir

	#get remote file which remote filepath contained 
	def  file_list(self,ftp_path):
		if self._is_ftp_dir(ftp_path):
			file_list = self.conn.nlst(ftp_path)
			file_names = []
			for file in file_list:
				file_names.append(file.split('/')[-1])
			return list(set(file_names))
		return []

	#download remote files to local
	def getFilestoLocal(self,RemoteFilePath,LocalFilePath='/home/mysql1/anqu/analysisResault/TestFile/'):
		remotefile = RemoteFilePath
		if os.path.isdir(LocalFilePath) == False:
			os.makedirs(LocalFilePath)
		print 'start ... ...'
		if self._is_ftp_dir(remotefile):#os.path.basename(remotefile))
			file_list = self.file_list(remotefile)
			for file in file_list:
				if 'searchapp' in file:
					# print file[10:12]
					print file
					self.deal_FileData_SearchApp(file,remotefile,file[10:12])

	def deal_Word(self,word,file_type):
		if len(word) < 1 or len(word) > 12:
			return False
		if self.chi.which_languedge(word.decode('utf8'),file_type) == False:
			return False
		if self.chi.is_punctuation(word.decode('utf8')):
			return False
		if self.dic_w.has_key(word):
			return False
		self.dic_w.setdefault(word)
		return True

	# def buildRecord(self,wordR):
	# 	# for word in wordR:
	# 	# word = []
	# 	wordR[2] = wordR[2].split(',')
	# 	wordR[4] = wordR[4].split(',')
	# 	return 

	# deal data and insert into dataBase
	def deal_FileData_SearchApp(self,file,remotefile,d_type='cn'):
		min_s = ''
		i = 0
		selectword = SW()
		word_Data = []
		state_f = False
		count = 0

		self.dic_w = {}

		for data in self.getFileBlock(file,remotefile):
			data_list = []
			if '^^^' not in data:
				min_s += data
				if '^^^' not in min_s:
					continue
				else:
					data = min_s
					min_s = ''
			data_list = data.split('^^^')
			data_list[0] = min_s + data_list[0]
			word_list = []
			
			# print len(data_list)
			for i in xrange(len(data_list)-1):
				word_data = data_list[i].split('###')
				###
				if self.deal_Word(word_data[0].decode('utf8'),d_type):
					word_data[2] = word_data[2].split(',')
					word_data[4] = word_data[4].split(',')
					word_list.append(word_data)
			min_s = data_list[len(data_list)-1]
			# print len(word_list)
			self.rhs.insertDataFromStruct(word_list,d_type,state_f)
			self.rhs.refreshTable('search_'+d_type)
			state_f = True

	# download remote file to local
	def getFiletoLocal(self,filename,RemoteFilePath,LocalFilePath='/home/mysql1/anqu/analysisResault/TestFile/'):
		if self._is_ftp_file(RemoteFilePath+filename):
			self.conn.cwd(RemoteFilePath)
			fsize = self.conn.size(filename)
			if fsize == 0:
				return 
			# check local file isn't exists and get the local file size
			lsize = 0L
			if os.path.exists(LocalFilePath+filename):
				lsize = os.stat(LocalFilePath+filename).st_size

			if lsize >= fsize:
				print 'local file is bigger or equal remote file'
				return 
			blockSize = 1024*1024   #size 128M
			print blockSize
			cmpsize = lsize
			self.conn.voidcmd('TYPE I')
			rfile = self.conn.transfercmd('RETR '+filename,lsize)
			f_write = open(LocalFilePath+filename,'ab')
			while True:
				data = rfile.recv(blockSize)
				print sys.getsizeof(data)
				if not data:
					break
				f_write.write(data)
				cmpsize += len(data)
				print 'download process:%.2f%%'%(float(cmpsize)/fsize*100)
			f_write.close()
			self.conn.voidcmd('NOOP')
			self.conn.voidresp()
			rfile.close() 

	#get file block by size cinfig
	def getFileBlock(self,filename,RemoteFilePath):
		if self._is_ftp_file(RemoteFilePath+filename):
			self.conn.cwd(RemoteFilePath)
			fsize = self.conn.size(filename)
			if fsize == 0:
				return
			# check local file isn't exists and get the local file size
			lsize = 0
			cmpsize = lsize
			blockSize = 1024*1024*40   #*1024   #size 1M  40M
			MaxBlockSize = 64*blockSize
			self.conn.voidcmd('TYPE I')
			rfile = self.conn.transfercmd('RETR '+filename,lsize)
			data_buff = u''
			while True:
				data = rfile.recv(blockSize).decode('utf-8', 'ignore')
				if sys.getsizeof(data_buff) <= MaxBlockSize and sys.getsizeof(data_buff+data) > MaxBlockSize:
					print sys.getsizeof(data_buff)/blockSize
					yield data_buff
					data_buff = u''
				if not data:
					if len(data_buff) > 0:
						yield data_buff
					break
				cmpsize += len(data)
				data_buff += data
				# print 'read process:%.2f%%'%(float(cmpsize)/fsize*100)
			self.conn.voidcmd('NOOP')
			self.conn.voidresp()
			rfile.close()

	#get file context
	def getFileContext(self,file,RemoteFilePath,d_type):  #d_type mark file type chose method 
		#add code deal file 
		for data in self.getFileBlock(file,RemoteFilePath):
			pass
	#close connect 
	def close(self):
		self.conn.close()


def main():
	rf = RecieveFile()
	# print_code()
	rf.login()
	RemoteFilePath = '/home/mysql1/anqu/analysisResault/TestInputFile/'
	LocalFilePath = '/home/mysql1/anqu/analysisResault/TestOutPutFile/'
	# print 'fa'
	rf.getFilestoLocal(RemoteFilePath,LocalFilePath)
	rf.close()

if __name__ == '__main__':
	main()
