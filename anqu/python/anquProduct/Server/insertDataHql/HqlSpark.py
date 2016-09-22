#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : byz
# dateTime : 2016-07-25

import sys
sys.path.append('/home/mysql1/anqu/python/code/Tools')
sys.path.append('/home/mysql1/anqu/python/anquProduct/Server')
reload(sys)
sys.setdefaultencoding('utf8')
import config 


from pyspark import SparkConf,SparkContext
from pyspark.sql import HiveContext
from pyspark.mllib.clustering import KMeans,KMeansModel
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,LongType
from pyspark.sql import functions
# from functions import *
from pyspark.sql import Row
import all_shame as als
import numpy
from numpy import *
import json
import time


# result = {
# 	ClusterNum = 20    #总共包含多少聚类
# 	AllCluster:[
# 	Cluster:{
# 		cluster_Id:1,
# 		ClassWords:[{word:XXX,priority:number},....],
# 		KeyWords:[{word:XXX,priority:number},....]
# 	}]
# }


class HqlSpark():
	# init Spark sql 
	def __init__(self,language = 'cn'):
		self.conf = SparkConf().setAppName("HqlSparkAPP").setSparkHome("/home/spark").set("spark.driver.allowMultipleContexts","true")
		self.sc = SparkContext(conf=self.conf)
		self.sql = HiveContext(self.sc)
		self.sql.sql('use anserchapp')
		self.curlan = language
		self.curDataWord = None
		# self.loadWord_state = False

	#get current language word
	def getAllWord(self,language='cn'):
		if self.curlan == language and self.curDataWord:
			return
		self.curlan = language
		sql_s = 'select * from searchapp_%s limit 100'%self.curlan
		self.curDataWord = self.sql.sql(sql_s)

	def refreshTable(self,tableName):
		self.sql.refreshTable(tableName)

	#create table 
	def createTable(self,sql_sentence,tableName):
		# table_name = sql_sentence.split(' ')[2]
		self.sql.sql(sql_sentence)
		self.sql.refreshTable(tableName)
		print 'create table success'

	#insert data into table
	def  insertData(self,sql_sentence):
		self.sql.sql(sql_sentence)

	# insert data into table from data_struct 
	def insertDataFromStruct(self,data,tableName = 'searchapp_',d_type = 'cn',state=False):   #data tuple or list list   data,
		# rdd = self.sc.parallelize(data)
		if d_type == '':
			in_data = self.sql.createDataFrame(data,als._categry_shame)
		elif d_type == 'hint':
			in_data = self.sql.createDataFrame(data,als.hintWord_shame)
			d_type = ''
		else :
			in_data = self.sql.createDataFrame(data,als.searchApp_shame)
		# final_data = in_data
		if state:
			in_data.saveAsTable(tableName=tableName+d_type,Source='metastore_db',mode='append')#   append  overwrite
		else:
			in_data.saveAsTable(tableName=tableName+d_type,Source='metastore_db',mode='overwrite')

	# delete table 
	def deleteDataFromTable(self,table='searchapp_',d_type='ch'):
		sql_sentence = 'delete from '+table+d_type
		self.sql.dropTempTable(table+d_type)
		self.sql.refreshTable(table+d_type)

	def showTales(self):
		table_list = []
		tables = self.sql.sql('show tables').collect()
		for table in  tables:
			table_list.append(table['tableName'])
		return table_list

	def getData(self,sql_hive):
		datas = self.sql.sql(sql_hive).collect()
		return datas

	#according input words find hintword from table hintword
	def selectHintWord(self,base_wordFr):
		hintWord = self.sql.sql('select word,hintWord from hintword')
		word = hintWord.join(base_wordFr,hintWord.hintWord == base_wordFr.word,'outer').select(hintWord.word).distinct()
		word_news = self.curDataWord.join(word,word.word == self.curDataWord.word,'outer').select(self.curDataWord.word,'priority','searchApp','searchCount','genre').distinct()
		word_news = word_news.dropna(how='any')
		return word_news
		
	#according to appId find word from searchapp
	def selectAppIdWord(self,appIds):
		result = None
		for appId in appIds:
			if result == None:
				result = self.curDataWord.filter(functions.array_contains(self.curDataWord.searchapp,appId)).select('word','priority','searchApp','searchCount','genre').distinct()
			res = self.curDataWord.filter(functions.array_contains(self.curDataWord.searchapp,appId)).select('word','priority','searchApp','searchCount','genre').distinct()
			result = result.unionAll(res)
			word = result.select('word')
			result = result.dropna(how='any')
		return  result,word

	#according to genre id find word from searchApp
	def selectGenreWord(self,genreIds):
		result = None
		for gId in genreIds:
			if result == None:
				result = self.curDataWord.filter(functions.array_contains(self.curDataWord.genre,gId)).select('word','priority','searchApp','searchCount','genre').distinct()
			res = self.curDataWord.filter(functions.array_contains(self.curDataWord.genre,gId)).select('word','priority','searchApp','searchCount','genre').distinct() 			
			result = result.unionAll(res)
		return result
	# get all word for analysis
	def getAnalysisWords(self,appIds,genreIds):
		if appIds == None or len(appIds) <= 0:
			return None
		appWord,word = self.selectAppIdWord(appIds)
		genreWord = None
		thinkWord = None
		if genreIds != None and len(genreIds) > 0:
			genreWord = self.selectGenreWord(genreIds)
		if word and word.count() > 0:
			thinkWord = self.selectHintWord(word)

		if appWord and genreWord and thinkWord:
			appWord = appWord.unionAll(genreWord)
			appWord = appWord.unionAll(thinkWord)
			return appWord.distinct()
			# return appWord.unionAll(genreWord).unionAll(thinkWord).distinct()
		elif appWord and genreWord:
			return appWord.unionAll(genreWord).distinct()
		elif appWord and thinkWord:
			appWord = appWord.unionAll(thinkWord)
			return appWord.distinct()
		elif genreWord and thinkWord:
			genreWord = genreWord.unionAll(thinkWord)
			return genreWord.distinct()
		elif appWord:
			return appWord.distinct()
		elif genreWord:
			return genreWord.distinct()
		else:
			return thinkWord.distinct()

	#build Matrix
	def buildMatrix(self,words):
		class_all = self.sql.sql("select genreID from category order by genreID desc")
		c_genres = class_all.collect()
		genres = {}
		i = 0
		for c in c_genres:
			genres.setdefault(c.genreID,i)
			i += 1

		datas = words.select('genre').collect()
		mlength = len(c_genres)
		nlength = len(datas)
		Matrix = numpy.zeros((nlength,mlength))
		num = 0
		print len(Matrix)
		for data in datas:
			for ge in data.genre:
				Matrix[num][genres.get(ge)] = 1
			num += 1
		return Matrix

	#get Input data 
	def getInPut(self,appIds,genreIds):
		words = self.getAnalysisWords(appIds,genreIds)
		return self.buildMatrix(words)

	# k_means analysis 
	def spark_means(self,Matrix,Kcluster = 2,MaxIterations = 10,runs = 10):
		cluster_data = self.sc.parallelize(Matrix)
		trains = KMeans().train(cluster_data,Kcluster,MaxIterations,runs)
		results = trains.predict(cluster_data).collect()
		return results

	#combine word 
	def combine_data(self,words=None,result=None):
		len_re = len(result)
		len_w = words.count()
		if len_re != len_w:
			print 'word num :',len_w,' is not equal result num:',len_re
		if len_re < len_w:
			words = self.sql.createDataFrame(words.take(len_re))

		else:
			result = result[0:len_w]
			print words.count(),len(result)
		result = map(lambda x : str(x) , result)
		cluster_re = self.sc.parallelize(result,1)
		# print cluster_re.collect(),words.map(list).count()
		re = words.map(list).repartition(1).zip(cluster_re).map(lambda p: Row(word=p[0][0], priority=int(p[0][1]),\
			searchcount=int(p[0][3]),cluster=p[1]))
		cluster_sha = self.sql.createDataFrame(re)
		# cluster_sha.show()
		return cluster_sha

	# select Class Word
	def selectWord(self,cluster_sha,top_K=2):
		df = cluster_sha
		select_par = df.groupBy('cluster').agg({'searchcount':'avg','priority':'avg'}).collect()
		ClusterNum = len(select_par)
		clusterWord = []
		for line in select_par:
			cluster_df = df.filter(df.cluster==line[0]).select('word','priority','searchcount')
			ClassWord = cluster_df.filter(cluster_df.searchcount>line[1]).select('word','priority')
			ClassWord = ClassWord.filter(ClassWord.priority>=line[2]).select('*').limit(top_K)

			KeyWords = cluster_df.filter(cluster_df.searchcount<line[1]).select('word','priority')
			KeyWords = KeyWords.filter(KeyWords.priority >= line[2]).select("*").limit(top_K)
			cluster = {
				'cluster_id':line[0],
				'classWord':ClassWord.toJSON().collect(),
				'keyWord':KeyWords.toJSON().collect()
			}
			clusterWord.append(cluster)
		result = {
			'ClusterNum':ClusterNum,
			'AllCluster':clusterWord
		}
		return result


def main():
	hqlS = HqlSpark()
	# # # # pass
	hqlS.getAllWord()
	# # start = time.time()
	# # # # searcAppIds = [['1102138730'], ['1031897589'], ['1120562180'], ['972356413'], ['1112060888'], ['1119225952'], ['1080608190']]
	# searcAppIds = ['610391947',]
	# genreIds = ['6005','7014']
	# result,word = hqlS.getAnalysisWords(searcAppIds,genreIds)
	# # # result.show()
	# Matrix = hqlS.buildMatrix(result)
	# c_result = hqlS.spark_means(Matrix)

	# print
	words = hqlS.curDataWord
	result = [1,2,1,1,2,1,2,2,1,3,3,3,1,2,1,1,2,1,2,2,1,3,3,3,1,2,1,1,2,1,2,2,1,3,3,3,1,2,1,1,2,1,2,2,1,3,3,3,1,2,1,1,2,1,2,2,1,3,3,3,1,2,1,1,2,1,2,2,1,3,3,3,1,2,1,1,2,1,2,2,1,3,3,3,1,2,1,1,2,1,2,2,1,3,3,3]
	cluster_re = hqlS.combine_data(words,result)
	print hqlS.selectWord(cluster_re,top_K=2)

	# print Matrix[0:5]
	# print type(Matrix),len(Matrix[0])
	# result.show()	
	# end = time.time()
	# print end - start
	pass
	
if __name__ == '__main__':
	main()
