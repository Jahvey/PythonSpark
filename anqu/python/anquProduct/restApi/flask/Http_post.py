#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : byz
# dateTime : 2016-08-06


#!flask/bin/python
import urllib
import urllib2
import json
# from flask import Flask,jsonify
    
def http_post():
	url='http://182.254.247.157182.254.247.157182.254.247.157182.254.247.157182.254.247.157182.254.247.157182.254.247.157182.254.247.157182.254.247.157182.254.247.157182.254.247.157182.254.247.157182.254.247.157182.254.247.157182.254.247.157182.254.247.157182.254.247.157182.254.247.157:50500/analysis_p'
	# values ={'user':'Smith','passwd':'123456'}
	pare = {
				# 'task':{
					'language':'cn',
					'appIds':[
						{"appid":'1076877374'},
		    			{"appid":'1108288808'}],
		    		'ClusterNum':2,
		    		'WordNum':2,
		    		'ShowOnWebPage':True,
		    		'EmailMessage':True,
		    		'EmailAddress':''
    			# }
    		}
   	# print pare['appIds'][1]
    # print pare
	jdata = json.dumps(pare)				# 对数据进行JSON格式化编码
	# print jdata
	print jdata
	req = urllib2.Request(url, jdata)       # 生成页面请求的完整数据
	print req
	# response = urllib2.urlopen(req)       # 发送页面请求
	# return response.read()                    # 获取服务器返回的页面信息
	response = urllib2.urlopen(req).read()
	response = json.loads(response)
	print response
	test = response["task"]
	return test


resp = http_post()
print resp
