# Basic file operations

# function to open a file, read, splitlines and return a list
def readData(datapath)
	dataFile = open(dataPath)
	dataStr = dataFile.read()
	dataList = dataStr.splitLines()
	return dataList
