#encodig=utf-8
# _*_ coding:utf-8 _*_
# Writer : lgy
# dateTime : 2016-06-16

import re

class WordLanguedge():
	def __init__(self):
		self.initFuncation()
		# pass

	#判断是都包含中文
	def is_chinese(self,c_str):
		zhPattern = re.compile(u'[\u4e00-\u9fa5]+')
		#一个小应用，判断一段文本中是否包含简体中：
		# contents=u'一个小应用，判断一段文本中是否包含简体中：'
		match = zhPattern.search(c_str)
		# print match
		if match:
			# print ''
			an = re.compile(u'[^\u4e00-\u9fa5,^\w]+')
			ma = an.search(c_str)
			if ma:
				# print ma.group(0)
				return False
			else:
				return True
		else:
			return False

	def is_english(self,word):
		zhPattern = re.compile(u'[\s]+')
		# an = re.compile('')
		#一个小应用，判断一段文本中是否包含japan：
		# contents=u'一个小应用，判断一段文本中是否包含japan'
		match = zhPattern.search(c_str)
		if match:
			an = re.compile(u'[^\s,^\d]+')
			ma = an.search(c_str)
			if ma:
				# print ma.group(0)
				return False
		else:
			return False
		return True

	#是否包含单引号
	def is_contains(self,c_str):
		an = re.compile(u'\'')
		match = an.search(c_str)
		if match:
			return True
		else:
			return False

	# judge word punctuation contaned
	def is_punctuation(self,c_str):
		an = re.compile(u'[,.:;?=%!*&()^\-+#$@~`。，、‘；】【、’|]') #   ,.:;?=-!%*&()^
		match = an.search(c_str)
		if match :
			return True
		else:
			return False
	#judge word is japanese
	def is_japanese(self,c_str):
		zhPattern = re.compile(u'[\u3040-\u30FF,\u4e00-\u9fa5,\s]+')
		# an = re.compile('')
		#一个小应用，判断一段文本中是否包含japan：
		# contents=u'一个小应用，判断一段文本中是否包含japan'
		match = zhPattern.search(c_str)
		if match:
			an = re.compile(u'[^\u3040-\u30FF,^\u4e00-\u9fa5,^\s,\d]+')
			ma = an.search(c_str)
			if ma:
				# print ma.group(0)
				return False
		else:
			return False
		return True
	#judge word is Russian
	def is_Russian(self,c_str):
		zhPattern = re.compile(u'[\u0410-\u044f,\s]+')
		# an = re.compile('')
		#一个小应用，判断一段文本中是否包含japan：
		# contents=u'一个小应用，判断一段文本中是否包含japan'
		match = zhPattern.search(c_str)
		if match:
			an = re.compile(u'[^\u0410-\u044f,^\s,^\d]+')
			ma = an.search(c_str)
			if ma:
				# print ma.group(0)
				return False
		else:
			return False
		return True
	#judge word is french
	def is_French(self,c_str):
		zhPattern = re.compile(u'[\u0400-\u1279,\s]+')
		# an = re.compile('')
		#一个小应用，判断一段文本中是否包含japan：
		# contents=u'一个小应用，判断一段文本中是否包含japan'
		match = zhPattern.search(c_str)
		if match:
			an = re.compile(u'[^\u0400-\u1279,^\s,^\u0000-\u007F,^\d]+')
			ma = an.search(c_str)
			if ma:
				# print ma.group(0)
				return False
		else:
			return False
		return True

	def is_Arabic(self,word):
		zhPattern = re.compile(u'[\u0600-\u06FF,\u0750-\u077F,\s]+')
		# an = re.compile('')
		#一个小应用，判断一段文本中是否包含japan：
		# contents=u'一个小应用，判断一段文本中是否包含japan'
		match = zhPattern.search(c_str)
		if match:
			an = re.compile(u'[^\u0600-\u06FF,^\u0750-\u077F,^\s,^\d]+')
			ma = an.search(c_str)
			if ma:
				# print ma.group(0)
				return False
		else:
			return False
		return True

	#init func 
	def initFuncation(self):
		print 'init funcation which languedge'
		self.functions = {
		'cn':self.is_chinese,
		'us':self.is_english,
		'en':self.is_english,
		'fr':self.is_French,
		'ge':self.is_Germen,
		'ru':self.is_Russian,
		'ta':self.is_chinese_traditional,
		'jp':self.is_japanese
		'sa':self.is_Arabic
		}


	#judge a word is Traditional chinese (TaiWan)
	def is_chinese_traditional(self,c_str):
		return False
	#fun point 
	def which_languedge(self,c_str,ltype='ru'):
		return self.functions[ltype](c_str)
	#judge word is germen
	def is_Germen():
		return False

	#judge word id 

def print_code():
	for i in xrange(1040,1279):
		print u'%c %04x'%(i,i),
		if (i-2) % 10 == 0 and i != 0:
			print ''

def main():
	chin = WordLanguedge()
	chin.initFuncation()
	print_code()
	str_japan = u'txt免费全本21小说'
	print str_japan
	print chin.which_languedge(str_japan,'cn')

if __name__ == '__main__':
	main()