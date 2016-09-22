#!flask/bin/python
#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : byz
# dateTime : 2016-08-05
import sys
sys.path.append('/home/mysql1/anqu/python/anquProduct/Server')
reload(sys)
sys.setdefaultencoding('utf8')
import config
from HqlSpark import HqlSpark as HS
from NetLink import NetLink
import json
from d9t.json import parser
from selectWord import selectWord as SW
from SparkK_means import KMeansCluster as skmc
from flask import Flask, jsonify, request, abort
# from flask.ext.restful import Resource, Api
import json

app = Flask(__name__)
# api = Api(app)
mysql = HS()


def Analysis(Appids,languadge='cn',ClusterNum=10,WordNum=50):
	nt = NetLink()
	sw = SW()
	mysql.getAllWord(languadge)
	all_AppIds,genre_Ids = nt.getCompleteIds(Appids)
	All_word = mysql.getAnalysisWords(all_AppIds,genre_Ids)
	Matrix = mysql.buildMatrix(All_word)
	result = mysql.spark_means(Matrix,ClusterNum)
	# print All_word,result
	data = mysql.combine_data(All_word,result)
	return mysql.selectWord(data,WordNum)

def SendMessage(EmailAddress):
	print 'email send to %s'%EmailAddress
	pass


def runAnalysis(pare=None):
	# task_j = json.dumps(pare)
	ta = json.loads(pare)
	# re = ta.keys()[0]
	appIds = []
	print "loads suceful"
	kk = len(ta['appIds'])
	for x in xrange(0,kk):
		appId = ta['appIds'][x].get("appid")
		appIds.append(appId)
	#
	language = ta["language"]
	EmailAddress = ta["EmailAddress"]
	ClusterNum = ta["ClusterNum"]
	WordNum = ta["WordNum"]
	#
	print "dict suceful"
	result = Analysis(appIds,language,ClusterNum,WordNum)
	print "Analysis suceful"
	print result
	#
	EmailMessage = ta["EmailMessage"]
	ShowOnWebPage = ta["EmailMessage"]
	if EmailMessage:
		EmailAddress = ta['EmailMessage']
	if ShowOnWebPage:
		return {"answer":result}
	else:
		return {"answer":''}


# @app.route('/analysis_g/<int:pare>', methods=['GET'])
# def task(pare):
#	   return jsonify({'an':str(pare+1)})
# <string:pare>

@app.route('/ana', methods=['GET', 'POST'])
def call_analysis():
	print "Hello, World!"
	# if not request.json:(NO)
	# 	abort(400)
	#
	# # data = request.get_json['appIds'] (NO)
	# # print "data", data 
	# data1 = request.data
	# print "data1", data1 
	data = request.get_data()
	print "data2", data #, len(data2)
	#
	j_data =  json.loads(data)
	print "j_data", j_data['language'] 
	
	# task_j = json.dumps(data2)
	# resultDict = json.load(task_j)
	print "j_data", j_data # resultDict['language'] 
	# return jsonify(task_j)
	# data = request.form
	return jsonify({'task': runAnalysis(data)}), 201


# vim /home/mysql1/anqu/python/anquProduct/restApi/flask/RestApiServer.py
# rm /home/mysql1/anqu/python/anquProduct/restApi/flask/RestApiServer.py

# curl -i -H "Content-Type: application/json" -X POST -d '{"appIds": [ {"appid": "1076877374"}, {"appid": "1108288808"} ], "language": "cn", "EmailAddress": "", "ClusterNum": 2, "WordNum": 2, "EmailMessage": true, "ShowOnWebPage": true}' http://182.254.247.157:5000/ana
# curl -i -H "content-type: application/json" -X POST -d '{\"appIds\": [{\"appid\": \"1076877374\"}, {\"appid\": \"1108288808\"}], \"language\": \"cn\", \"EmailAddress\": \"\", \"ClusterNum\": 2, \"WordNum\": 2, \"EmailMessage\": True, \"ShowOnWebPage\": True}' http://182.254.247.157:5000/ana
# curl -i -H "Content-Type: application/json" -X POST -d '{"appIds":[{"appid":"1076877374"},{"appid":"1108288808"}],"language":"cn","EmailAddress":"","ClusterNum":2,"WordNum":2,"EmailMessage":True,"ShowOnWebPage":True}'  http://182.254.247.157:5000/ana

# curl -d '{"appIds":[{"appid":"1076877374"},{"appid":"1108288808"}],"language":"cn","EmailAddress":"","ClusterNum":2,"WordNum":2,"EmailMessage":True,"ShowOnWebPage":True}' -H "Content-Type: application/json" http://182.254.247.157:5000/ana
# curl -d '{"appIds":"1108288808","language":"cn","EmailAddress":"","ClusterNum":2,"WordNum":2,"EmailMessage":True,"ShowOnWebPage":True}' -H "Content-Type: application/json" http://182.254.247.157:5000/ana




@app.route('/')
def index():
	print "Hello, World!"
	# print runAnalysis(['1131455194','1012852987'])
	# return jsonify({'task': runAnalysis(['1131455194','1012852987'])}), 201
	aa = Analysis(["1111484111","1012852987"])
	# aa = list(Analysis(["1111484111","1012852987"]))
	print "This is analysis result:\n",aa
	return jsonify({'task': aa}), 201


if __name__ == '__main__':
	app.run(host='0.0.0.0',port=5000)
	#app.run(debug=True)


