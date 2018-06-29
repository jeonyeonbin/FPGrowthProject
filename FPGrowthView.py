from DomeggookModel import CategoryModel 
class RecommendItemListView():
	def __init__(self,config):
		model = CategoryModel(config)
		self.__db = model
	
	def showRecommendList(self):
		recommendList = self.__db.recommendItemList()
		print('-----------------------------------------------------')
		print('|   antecedent   |    consequent  |    confidence    |')
		print('-----------------------------------------------------')
		count = 0
		for data in recommendList:
			count+=1	
			print('|%-13s  |%-13s |%-20s |'%(data[0],data[1],data[2]))
		print('------------------------------------------------------\n')
		return count	
			
