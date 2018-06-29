import conf_check

def Config():
	path = '/home/service/TTA/recommender/fpModule/'
	config = conf_check.Check(path)
	return config

