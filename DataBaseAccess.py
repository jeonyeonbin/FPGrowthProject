import pymysql
db_name = 'Category'
db_id = 'root'
db_pwd = 'almondflake'
db_addr = 'localhost'
def DataBaseQuery():
	try:
		conn = pymysql.connect(host=db_addr, user=db_id, password=db_pwd, db=db_name, charset='utf8')
	except Exception as e:
		print(e)

	curs = conn.cursor()
	sql_query = "SELECT orderUid AS id, GROUP_CONCAT( CONVERT( itemNo, CHAR ) ) AS items FROM ngm_order2018 WHERE date between -1517410800.0 AND -1514732400.0 GROUP BY orderUid"
	try:
		curs.execute(sql_query)
		rows=curs.fetchall()
		itemLists = []
		for data in rows:
			addData = data[1].split(',')
			if len(addData) != 1:
				itemLists.append(addData)

		for myData in itemLists:
			print(myData)
		return itemLists

	except Exception as e:
		print(e)
