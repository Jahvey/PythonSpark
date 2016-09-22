#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : lgy
# dateTime : 2016-07-29


import sys
sys.path.append('/home/mysql1/anqu/python/anquProduct/Server')
sys.path.append('/home/mysql1/anqu/python/anquProduct/Server/RecieveFileData')
reload(sys)
sys.setdefaultencoding('utf8')
import config
from file_op import file_op
# from data_deal import data_deal as data_d
from HqlSpark import HqlSpark

#init run envirenment main database
class InitClass():
	def __init__(self):
		# conf = SparkConf().setAppName("init")
		# self.sc = SparkContext(conf=conf)
		self.myHql = HqlSpark()
		# pass

	def init_category(self):
		fp = file_op()
		category = fp.get_fileData(config.categoryFilePath)
		self.myHql.insertDataFromStruct(category,'category','',False)
 	
 	#init searchapp table only chinese
	def init_searchapp(self):
		# print config.searchapp_cn
		with open(config.searchapp_cn,'r') as fp:
			line = fp.readline()
			lines = []
			p = len(config.searchapp_cn)-15
			tableName = 'searchapp_'
			d_type = config.searchapp_cn[p:p+2]
			print tableName,d_type
			state = False  # False rewrite search table 
			while  line != None and len(line):
				word = line[0:-4].split("###")
				word[2] = word[2].split(',')
				word[4] = word[4].split(',')
				lines.append(word)
				# print line
				if len(lines) > 0 and len(lines) >= 10000:
					self.myHql.insertDataFromStruct(lines,tableName,d_type,state)
					state = True
					lines = []
					# break
				line = fp.readline()
			if len(lines) > 0:
				self.myHql.insertDataFromStruct(lines,tableName,d_type,state)				

	#init hintword table
	def init_HintWord(self):
		self.myHql.createTable("create table if not exists hintword (word string,priority string,type string,hintWord string ,time string) ",'hintword')
		hintword = file_op().get_fileDatasharp(config.hintword_file)
		self.myHql.insertDataFromStruct(hintword,'hintword','hint',False)

def main():
	init_c = InitClass()
	# init_c.init_category()
	# init_c.init_HintWord()
	init_c.init_searchapp()

if __name__ == '__main__':
	main()


