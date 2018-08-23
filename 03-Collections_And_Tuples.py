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
# get daily revenue for completed and closed orders --> we will have to join orders and order_items

# func1 : develop a function to get completed and closed orders
# func2 : extract order_id, order_date from completed and closed orders
# func3 : join order_items and orders; get order_item_subtotal and aggregate the
#-----------------------------------------------------------------------------------------------------------------------------
def getCompletedClosedOrders(orders):
    ordersFiltered = []
    for order in orders:
        if(order.split(',')[3] in ('COMPLETE','CLOSED')):
            ordersFiltered.append(order)
    return ordersFiltered

def getDictForOrders(orders):
    ordersDict = {}
    for order in orders:
        orderAttributes = order.split(',')
        ordersDict[int(orderAttributes[0])] = orderAttributes[1]
    return ordersDict

def getDailyRevenue(orderIdDateDict,orderItems):
    dailyRevenue = {}
    for orderitem in orderItems:
        # orderItemTuple = (orderID, totalRevenue)
        orderItemTuple = (int(orderitem.split(',')[1]),float(orderitem.split(',')[4]))
        if orderIdDateDict.get(orderItemTuple[0]):
            orderDate = orderIdDateDict[orderItemTuple[0]]
            if dailyRevenue.get(orderDate):
               dailyRevenue[orderDate] += orderItemTuple[1]          
            else :
               dailyRevenue[orderDate] = orderItemTuple[1]
    return dailyRevenue

# snippet to call the functions
ordersPath = '/data/retail_db/orders/part-00000'
orderItemsPath = '/data/retail_db/order_items/part-00000'

orders = readData(ordersPath)
orderItems = readData(orderItemsPath)

ordersFiltered = getCompletedClosedOrders(orders)
orderIdDateDict = getDictForOrders(ordersFiltered)
dailyRevenue = getDailyRevenue(orderIdDateDict,orderItems)
len(dailyRevenue)

# print result slice
for i in list(dailyRevenue.items())[5:10]:
    print(i)

