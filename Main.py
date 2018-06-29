import sys
from FPGrowthController import FPGrowthController
FPGrowth = FPGrowthController(sys.argv[1])
data,runtime  = FPGrowth.insertRecommendTable()
count = FPGrowth.showRecommendList()

FPGrowth.closeConnection()
print("Module Runtime :{}\n ALL Insert COUNT:{}\n".format(runtime,count))

