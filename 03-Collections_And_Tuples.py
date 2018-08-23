# understanding Collections

#-----------------------------------------------------------------------------------------------------------------------------
# code snippet to  Get all unique order statuses from orders data (loop through list, get order status and add it to the set
#-----------------------------------------------------------------------------------------------------------------------------
def readData(dataPath) :
    dataFile = open(dataPath)
    dataStr = dataFile.read()
    dataList = dataStr.splitlines()
    return dataList
	
orders = readData("/data/retail_db/orders/part-00000")	

order_statuses = set({})

for i in orders:
	order_statuses.add(i.split(",")[3]) 
	
for statuses in s:
    print(statuses)
  
#-----------------------------------------------------------------------------------------------------------------------------
# code snippet : function to get revenue for a given order_id (from order_items)
#-----------------------------------------------------------------------------------------------------------------------------
def readData(dataPath) :
    dataFile = open(dataPath)
    dataStr = dataFile.read()
    dataList = dataStr.splitlines()
    return dataList

orderItemsPath = '/data/retail_db/order_items/part-00000'
orderItems = readData(orderItemsPath)
orderItems[:10]  

def getOrderRevenue(orderId):
    revenue = 0.0
    for order_item in orderItems:
        if (int(order_item.split(",")[1]) == orderId):
            print('item value =',float(order_item.split(",")[4]))
            revenue += float(order_item.split(",")[4])
    return revenue 

order4revenue = getOrderRevenue(4)
print('total =',order4revenue)
  
  
#-----------------------------------------------------------------------------------------------------------------------------
# code snippet : 
#-----------------------------------------------------------------------------------------------------------------------------
