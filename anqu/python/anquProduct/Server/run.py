#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : byz
# dateTime : 2016-08-03

import sys
sys.path.append("/home/mysql1/anqu/python/anquProduct/Server")
reload(sys)
import config

sys.setdefaultencoding('utf8') 

from pyspark import SparkConf as sconf
from HqlSpark import HqlSpark
from NetLink import NetLink
import json
from d9t.json import parser
from selectWord import selectWord as SW
from SparkK_means import KMeansCluster as skmc


def runAnalysis(Appids,languadge='cn'):
	conf = sconf().set("spark.driver.allowMultipleContexts", "true")
	mysql = HqlSpark(languadge)
	nt = NetLink()
	sw = SW()
	mysql.getAllWord()
	all_AppIds,genre_Ids = nt.getCompleteIds(Appids)
	All_word = mysql.getAnalysisWords(all_AppIds,genre_Ids)
	result = sw.readObj("kMeansresult.txt")
	print result,len(result),All_word.count()


def main():
	runAnalysis(Appids=['1111484111','1012852987','1083354212','1097663377'])

if __name__ == '__main__':
	main()
