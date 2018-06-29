# -*- coding: utf-8 -*-
from pyspark.ml.fpm import FPGrowth
#from pyspark.mllib.fpm import FPGrowth
from pyspark import SparkContext, SparkConf,StorageLevel
from pyspark.sql import functions as F
from pyspark.sql.functions import col,size,split
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType,BooleanType,StructField,StructType,StringType,DoubleType
from pyspark.sql.session import SparkSession
import time,threading
import sys
from threading import Thread
import pandas as pd

def fp_growth(conf):
	
	print("분석을 실시합니다.")
	st_time = time.time()

	#설정
	amount = int(conf['limit']['amount'])
	end_section = int(conf['limit']['end_section'])
	start_section = int(conf['limit']['start_section'])
	start_date = conf['limit']['start_date']
	end_date = conf['limit']['end_date']
	output = int(conf['result']['output'])
	result_table_name = conf['database']['result_table_name']
	count_table_name = conf['database']['count_table_name']
	db_name = conf['database']['database_name']
	db_id = conf['database']['database_id']
	db_pw = conf['database']['database_password']
	db_addr = conf['database']['database_addr']

	support = conf['association']['support']
	confidence = conf['association']['confidence']
	support = 0.0005
	confidence = 0.1
	
#	url = "jdbc:mysql://"+db_addr+":3306/"+db_name+"?autoReconnect=true&&useSSL=true&characterEncoding=utf8&useServerPrepStmts=false&rewriteBatchedStatements=true"
	url = "jdbc:mysql://"+db_addr+":3306/"+db_name+"?autoReconnect=true&&useSSL=true&characterEncoding=utf8"

	appName = "fp_growth"
	master = "local[*]"

	#date query 설정
	st_date = str((-1)*time.mktime(time.strptime(str(start_date),"%Y-%m-%d")))	
	ed_date = str((-1)*time.mktime(time.strptime(str(end_date),"%Y-%m-%d")))

	print("start_date: " + start_date + ", timestamp: " +  st_date)
	print("end_date: " + end_date + ", timestamp: " + ed_date)

	t1 = time.time()
	print("init: "+str(t1-st_time))
	
	#spark객체 생성
	sparkConf = SparkConf()
	sparkConf.setAppName(appName)
	sparkConf.setMaster(master)
#	sparkConf.set('spark.executor.memory', '8g')
#	sparkConf.set('spark.executor.cores', '2')
	
	print(sparkConf)
	
	sc = SparkContext.getOrCreate()
	t1_1 = time.time()
	print("sparkContext getOrCreate: " + str(t1_1 - t1))

	#spark = SQLContext(sc)
	#sqlcontext = SQLContext(sc)
	spark = SparkSession(sc)
	t1_2 = time.time()
	print("set sparkSession: " + str(t1_2 - t1_1))
	

	t2 = time.time()
	print("create spark session: "+str(t2 - t1))

	'''
	spark = SparkSession.builder \
		.master("local[*]") \
		.appName(appName) \
		.getOrCreate()

	'''	
	sql_query = "(SELECT orderUid AS id, GROUP_CONCAT( CONVERT( itemNo, CHAR ) ) AS items FROM ngm_order2018 WHERE date between " + ed_date  + " AND " + st_date + " GROUP BY orderUid) as tmp"

	print(sql_query)
	exit()
	sparkDB = spark.read.format("jdbc") \
		.option("url", url) \
		.option("driver", "com.mysql.jdbc.Driver") \
		.option("user", db_id) \
		.option("password", db_pw)
		
	t2_1 = time.time()
	print("database connecting: " + str(t2_1 - t2))

	df_trans = sparkDB \
		.option("dbtable", sql_query) \
		.load()

	#df_trans = df_trans.repartition(7)

	'''	
	#Load Transaction Data(txt)
	s1 = time.time()
	transFilePath = "/home/service/TTA/recommender/transactions.txt"
	trans = spark.read.text(transFilePath)
	transDF = trans.select(explode(split(col("value"), "\t")).alias("id"))
	result = transDF.groupBy("id").count()

	print(transDF)
	print(result)	
	
	s2 = time.time()	

	trans.createOrReplaceTempView("trans")
	trans.cache()
	s3 = time.time()

	trans.show()
	trans.printSchema()
	print(type(trans))
	print("test: " + str(s2 - s1))
	print("test2: " + str(s3 -  s2))
	'''

	#FILE READ TEST		
	'''
	file = open("/home/service/TTA/recommender/transactions.txt", "r")
	
	data = []
#	while(1):
	lines = file.readlines(100000)

	print(lines[3])	
	
#	if not lines:
#		break
	for line in lines:
		line = line.replace("\n", "")
		fields = line.split("\t")	
		data.append([fields[0], fields[1]])
		print(fields)
#		print(line)
	
	print(type(lines))
	print(lines[0])
	print(lines[1])
	print(lines[2])
	
	print(type(data))
	df = data.toDF()	
	
	print(type(df))
	print(df.show())

	exit()
	'''

#	df_trans = df_trans.repartition(40)
#	df_trans.cache()
#	df_trans.repartition(df_trans
	
	print("Number of Partitions: " + str(df_trans.rdd.getNumPartitions()))
#	df_trans.cache()

	t2_2 = time.time()	
	print("database loading: " + str(t2_2 - t2_1))

	df_trans = df_trans.withColumn("items", split(col("items"), ",\s*").cast("array<int>").alias("items")).where(size(col("items"))>1)
#	df_trans = df_trans.withColumn("items", split(col("items"), ",\s*").cast("array<int>").alias("items"))
	
#	df_trans.show()
	#로컬 스토리지 레벨을 메모리와 디스크랑
#	df_trans.persist()
	t3 = time.time()
	print("load ngm_order2018 transaction table: "+ str(t3-t2))


#	print("Count DataSet: " + str(df_trans.count()))

#	t3 = time.time()
#	Yrint("load ngm_order2018 transaction table: "+ str(t3-t2))

#	특정 요소(itemNo)를 포함하는 배열만 가져오기.. 

#	exit()
#	df_trans.cache()

	def testModule(convertType, df_trans):

		module_st = time.time()
		ttt1 = time.time()
		fpGrowth = FPGrowth(minSupport=float(support), minConfidence=float(confidence));
		ttt2 = time.time()
		print("FPGrowth: " + str(ttt2 - ttt1))
		
		model = fpGrowth.fit(df_trans)
		
		ttt3 = time.time()
		print("fpGrowth.fit: " + str(ttt3 - ttt2))

		df_confidence = model.associationRules #신뢰도
		df_confidence = df_confidence.where(size(col("antecedent"))<2)
		df_confidence.persist(StorageLevel.MEMORY_AND_DISK)
		ttt4 = time.time()
		print("model.associationRules: " + str(ttt4 - ttt3))

		df_conf_cnt = df_confidence.count()	
		module_ed1 = time.time() 
		print("module Runtime: " + str(module_ed1 - module_st))

		if(df_conf_cnt>0):

			df_confidence = df_confidence.withColumn("antecedent",convertType(df_confidence['antecedent']))
			df_confidence = df_confidence.withColumn("consequent",convertType(df_confidence['consequent']))

			print("저장을 실시합니다.")
			
			save_st = time.time()
			
			df_confidence.write \
				.mode("append") \
				.format("jdbc") \
				.option("url", url) \
				.option("driver", "com.mysql.jdbc.Driver") \
				.option("dbtable", result_table_name) \
				.option("user", db_id) \
				.option("password", db_pw) \
				.save()

			save_ed = time.time()
			#print("이 카테고리에 속한 아이템 종류는 ..."+str(cntItem))
			print("지지도,신뢰도를 만족하는 분석 결과 개수는 ..."+str(df_conf_cnt))
			df_confidence.show()

			print("Data Save: " + str(save_ed - save_st))
		
		print(model.freqItemsets.show())
		
#		time.sleep(100)
#		print(model.transform(splitData).show())

	convertType = udf(lambda x:x[0], IntegerType())
	testModule(convertType, df_trans)
	input('PYTHON VIEW')
	spark.stop()
#	df_trans.unpersist()

