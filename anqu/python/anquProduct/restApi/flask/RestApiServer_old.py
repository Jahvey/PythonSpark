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
#!flask/bin/python
#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : lgy
# dateTime : 2016-08-05
import sys
sys.path.append('/home/mysql1/anqu/python/anquProduct/Server')
reload(sys)
sys.setdefaultencoding('utf8')
import config
from HqlSpark import HqlSpark as HS
#!flask/bin/python
from NetLink import NetLink
import json
from d9t.json import parser
from selectWord import selectWord as SW
from SparkK_means import KMeansCluster as skmc
from flask import Flask, jsonify
from flask import request
from flask import abort
import json

app = Flask(__name__)
mysql = HS()

def Analysis(Appids,languadge='cn',ClusterNum=20,WordNum=50):
	nt = NetLink()
	sw = SW()
	mysql.getAllWord(languadge)
	all_AppIds,genre_Ids = nt.getCompleteIds(Appids)
	All_word = mysql.getAnalysisWords(all_AppIds,genre_Ids)
	Matrix = mysql.buildMatrix(All_word)
	result = mysql.spark_means(Matrix,ClusterNum)
	data = mysql.combine_data(All_word,result)
	return mysql.selectWord(data,WordNum)

def SendMessage(EmailAddress):
	print 'email send to %s'%EmailAddress
	pass


def runAnalysis(pare=None):
	task_j = json.dumps(pare)
	ta = json.loads(task_j)
	re = ta.keys()[0]
	pa_dic = json.loads(re)
	lan = pa_dic['language']
	
	clusterNum = pa_dic['ClusterNum']
	WordNum = pa_dic['WordNum']
	Appids = []
	lines = pa_dic['appIds']
	print lines
	for line in lines:
		Appids.append(line['appid'])
	result = Analysis(Appids,lan,clusterNum,WordNum)
	ShowOnWebPage = pa_dic['ShowOnWebPage']
	EmailMessage = pa_dic['EmailMessage']
	if EmailMessage:
		EmailAddress = pa_dic['EmailMessage']
	if ShowOnWebPage:
		return {"answer":result}
	else:
		return {"answer":''}
		
@app.route('/analysis_g/<int:pare>', methods=['GET'])
def task(pare):
	return jsonify({'an':str(pare+1)})

@app.route('/analysis_p', methods=['POST'])
def call_analysis():
	#args ,form (values)
    if request.json:
        abort(400)
    data = request.form
    return jsonify({'task': runAnalysis(data)}), 201

if __name__ == '__main__':
	app.run(host='0.0.0.0',port=5000)
