#-*- coding: utf-8 -*-
import pyfpgrowth
import time
import MakeFile
from DomeggookModel import CategoryModel
from FPGrowthView import RecommendItemListView
import ConfigController
from multiprocessing import Pool
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import sys
class FPGrowthController():
	def __init__(self,executeMode):
		self.__config = ConfigController.Config()
		dbModule = CategoryModel(self.__config)
		view = RecommendItemListView(self.__config)
		self.executeMode = executeMode
		self.__dbModule = dbModule
		self.__view = view
	def doProcessWork(self,data):
		patterns = pyfpgrowth.find_frequent_pattenrs(data,int(self.__config['association']['support']))
		return patterns
	def fpAlgorithm(self):
		#text file 인지 db 인지 구별 ex) python main.py text   or   python main.py db
		if self.executeMode == 'text':
			count,transactions = MakeFile.makeTestFile()
		elif self.executeMode == 'db':
			count,transactions = self.__dbModule.MakeTransactions(self.__config['limit']['start_date'],self.__config['limit']['end_date'])

		print("transcations Count: {}".format(count))
		# 모듈 시간 계산을 위해
		t1 = time.time()
		# FP GROWTH 모듈 FITTING
		#지지도 설정(첫번째 파라미터는 트랜잭션, 두번째 파라미터는 지지도)
		patterns = pyfpgrowth.find_frequent_patterns(transactions,int(self.__config['association']['support']))
		#신뢰도 0~1 설정
		rules = pyfpgrowth.generate_association_rules(patterns, float(self.__config['association']['confidence']))
		t2 = time.time()
		
		runtime = str(t2 -t1)
		#모듈 실행시간, 추천 데이터 (트랜잭션 양은 아님),추천데이터 
		return runtime,len(rules),rules.items()

	def insertRecommendTable(self):
		#모듈 리턴값
		runtime,dataLength,result = self.fpAlgorithm()
		print("SUPPORT : {}".format(self.__config['association']['support']))	
		#개별 추천 데이터 갯수
			
		idx = 0
		print("runtime :{}".format(runtime))
		#fpAlgorithm에서 나오는 데이터가 dictionary로 되어있습니다.
		#키 밸류 형식으로 튜플 형식으로 나오는데 개별적으로 짤라야대서 하나씩 삽입을위한것
		for key,value in result:
			for keys in key:
				for values in value[0]:
					if value[1] != 1:
						self.__dbModule.insertRecommendTable(keys,values,value[1])
						idx+=1
		#데이터 삽입 갯수
		print("All Count: {}".format(idx))
		#모듈 런타임
		print("Module RunTime:{}".format(runtime))
		return idx,runtime

	def showRecommendList(self):
		return self.__view.showRecommendList()
	def closeConnection(self):
		self.__dbModule.exit()
