#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : byz
# dateTime : 2016-08-01


#，软件系统全局变量 ------ 变量声明

#

Host_IP = '127.0.0.1'    #config to you database host address
database_en = 'mysql_anqu_en'  # english  
database_chi = 'mysql_anqu_chi' #chinese
database_japan = 'mysql_anqu_japan' #japanese
# add your database depend on your need
# database_XXX  
dataBase = database_chi
dataBase_port = 3306    #config to your database port 
dataBase_user = 'root'  #config to your database user
dataBase_passwd = 'root'#config to your database user's password

#xiang guan de biao 
# hintWord = 'hintword'   
hintWord = 'searchHint'


#系统运行各模块包路径
# spark run file path config 
runfile = '/home/mysql1/anqu/python/anquProduct/Server'
RecieveFile = runfile+'/RecieveileData'
insertFile = runfile + '/insertDataHql'
InitFile = runfile + 'Init'
ClusterFile = runfile + '/Cluster'
sparkRunFile1 = '/home/mysql1/spark/python'
sparkRunFile2 = '/home/mysql1/spark/python/pyspark'
sparkRunFile3 = '/home/mysql1/spark/python/lib/py4j'
ToolsFile = runfile + '/Tools'

#data file path for init HiveSql database 
dataFilePath = '/home/mysql1/anqu/analysisResault/TestInputFile/'
categoryFilePath = dataFilePath + '_category.txt'
searchapp_cn = dataFilePath + 'searchapp_cn_20160801.txt'
hintword_file = dataFilePath+'hintWord.txt'


fileResaultPath = '/home/mysql1/anqu/analysisResault/'  #config the path what you want
# fileClassResaultPath = fileResaultPath + 'ClassWord'
# fileKeyWordResaultPath = fileResaultPath + 'KeyWord'
fileObjectPath = fileResaultPath + "Object"
# fileRootPath = '/home/mysql1/anqu/python/code/'        #config the path being the point your code was contained ! 
# fileToolsPath = fileRootPath + "Tools"
# fileWordsPath = fileRootPath+'Word'
# fileAnalysisPath = fileRootPath+'analysis'
# fileDataPath = fileRootPath + 'data_deal'
# fileClusterPath = fileRootPath + 'Cluster'
# fileWordAnalysisPath = fileRootPath + 'wordAnalysis'

#系统运行，以来包路径配置

import sys
sys.path.append(runfile)
sys.path.append(RecieveFile)
sys.path.append(insertFile)
sys.path.append(sparkRunFile1)
sys.path.append(sparkRunFile2)
sys.path.append(sparkRunFile3)
sys.path.append(ToolsFile)
sys.path.append(ClusterFile)
reload(sys)
sys.setdefaultencoding('utf8')

