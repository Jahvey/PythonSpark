#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : lgy
# dateTime : 2016-08-03

import sys
sys.path.append("/home/mysql1/anqu/python/anquProduct/Server")
# sys.path.append("/home/mysql1/anqu/python/code/Tools")
reload(sys)
import config
sys.setdefaultencoding('utf8') 

import urllib2
import json
from d9t.json import parser

class NetLink():
	def __init__(self):
		self.urlHead = 'http://my.asostar.com/api/index.php?app=app&appid='
		self.urlMi = '&area=143465-19&device='
		self.urlTail = '&callback=_jsonpip6kyd5k5cornyf9us4zsq0k9'

	def getUrlData(self,Id = '620112416',dev_ty = 1):
		Url = self.urlHead+Id+self.urlMi+str(dev_ty)+self.urlTail
		print Url
		data = urllib2.urlopen(Url).read()
		start = data.find('(')
		data = data[start+1:len(data)-1]
		jD = parser.JsDomParser(data).parse()
		return jD

	def getCompleteIds(self,Ids = ['1111484111','1012852987','1083354212','1097663377'],dev_ty = 1):
		complete_Ids = []
		genreIds = []
		for Id in Ids:
			Jsoncontext = self.getUrlData(Id,dev_ty)
			if Jsoncontext == None:
				continue
			complete_Ids.extend(Jsoncontext['customersAlsoBoughtApps'])
			complete_Ids.append(Id)
			if Jsoncontext['result'] == None:
				continue
			if Jsoncontext['result']['genres'] == None:
				continue
			genre_r = self.getCompleteGenreID(Jsoncontext['result']['genres'])
			genreIds.extend(genre_r)
		complete_Ids = list(set(complete_Ids))
		genreIds = list(set(genreIds))
		return complete_Ids,genreIds

	#get complete product genreID
	def getCompleteGenreID(self,genres):
		genreIds = []
		if genres == None or len(genres) <= 0:
			return genreIds
		for genre in genres:
			line = genre['url']
			length = len(line)
			genreIds.append(line[length-4:length])
		return genreIds


def main():
	nl = NetLink()
	#nl.getUrlData()
	#complete_Ids,genreIds = nl.getCompleteIds()
	#print len(complete_Ids),len(genreIds)
	#print complete_Ids,genreIds

if __name__ == '__main__':
	main()

