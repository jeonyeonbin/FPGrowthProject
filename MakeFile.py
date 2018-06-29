def makeTestFile():
    f = open('./transactions.txt','r')
    itemList =[]
    cnt =0
    while True:  
        line = f.readline()
        if not line : break
        cnt+=1
        itemList.append(line.split('\t')[1].replace('\n','').split(','))
    f.close()
    return cnt,itemList
