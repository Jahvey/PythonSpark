#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : lgy
# dateTime : 2016-08-02

import sys
sys.path.append('/home/mysql1/anqu/python/anquProduct/Server')
reload(sys)
sys.setdefaultencoding('utf8')
import config
import cPickle as pickle
# from pyspark import Broadcast
# from file_op import file_op

class selectWord():
	def __init__(self):
		pass
		# self.myHql = HqlSpark()
		# self.conf = SparkConf().setAppName('selectWord')
		# self.sc = SparkContext(conf=self.conf)
		# self.
	#数据对象写入文件存储
	def writeObj(self,obj,file):
		filepath = config.fileObjectPath+'/'
		f = open(filepath+file, 'w')
		pickle.dump(obj, f)
		f.close()

	#读取文件中的数据对象
	def readObj(self,file):
		filepath = config.fileObjectPath+'/'
		f = open(filepath+file,'r')
		return pickle.load(f)
		

def main():
	sw = selectWord()
	print config.fileObjectPath

if __name__ == '__main__':
	main()
