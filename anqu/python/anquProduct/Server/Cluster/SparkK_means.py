#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : byz
# dateTime : 2016-07-28
import sys
sys.path.append('/home/mysql1/anqu/python/anquProduct/Server')
reload(sys)
sys.setdefaultencoding('utf8')
import config

from pyspark import SparkConf,SparkContext
from pyspark.mllib.clustering import KMeans,KMeansModel
from numpy import array
from math import sqrt
from selectWord import selectWord as SW
import time


class KMeansCluster():
	def __init__(self,Kcluster = 20,MaxIterations = 10,runs = 10):
		self.k = Kcluster
		self.iteration = MaxIterations
		self.runs = runs
		self.conf = SparkConf().setAppName("Cluster_k_means")
		self.sc = SparkContext(conf=self.conf)

	def K_means(self,data):
		cluster_data = self.sc.parallelize(data)
		trains = KMeans().train(cluster_data,self.k,self.iteration,self.runs)
		results = trains.predict(cluster_data).collect()
		return results

def main():

	# kmc = KMeansCluster()
	sw = SW()
	data = sw.readObj("Matrix.txt")
	print type(data)
	# for da in data:
		# print da ,type(da)
		# break
	results = KMeansCluster.K_means(data)
	# for re in results:
		# print re
	print results[0:20]
	# time().sleep(20)
	pass


if __name__ == '__main__':
	main()


