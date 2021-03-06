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



#-----------------------------------------------------------------------------------------------------------------------------
# Exercise 1 
# code snippet : 
# get count of statuses from orders
#-----------------------------------------------------------------------------------------------------------------------------

def readData(dataPath) :
    dataFile = open(dataPath)
    dataStr = dataFile.read()
    dataList = dataStr.splitlines()
    return dataList

def getStatusCountFromOrders(p_orders):
    orderStatusCountDict = {}
    for order in p_orders:
        splitCols = order.split(',')
        if orderStatusCountDict.get(splitCols[3]):
            orderStatusCountDict[splitCols[3]] += 1
        else:
            orderStatusCountDict[splitCols[3]] = 1
    return orderStatusCountDict


# script to be executed to call the defined functions
ordersPath = '/data/retail_db/orders/part-00000'
orders = readData(ordersPath)
orderStatusCount = getStatusCountFromOrders(orders)
print(orderStatusCount)   


#-----------------------------------------------------------------------------------------------------------------------------
# Exercise 2 
# code snippet : get count of orders by month
#-----------------------------------------------------------------------------------------------------------------------------

def readData(dataPath) :
    dataFile = open(dataPath)
    dataStr = dataFile.read()
    dataList = dataStr.splitlines()
    return dataList

def getOrdersCountByMonth(p_orders):
    print('started')
    monthDict = {'01':'Jan','02':'Feb','03':'Mar','04':'Apr','05':'May','06':'Jun','07':'Jul','08':'Aug','09':'Sep','10':'Oct','11':'Nov','12':'Dec'}
    orderCountByMonthDict = {}
    returnDict = {}
    for order in p_orders:
        monthCol = order.split(',')[1].split(' ')[0].split('-')[1]
        if orderCountByMonthDict.get(monthCol):
            orderCountByMonthDict[monthCol] += 1
        else:
            orderCountByMonthDict[monthCol] = 1
    
    # changing keys of the dictionary to month-names using regular for loop 
    for (k,v) in orderCountByMonthDict.items():
        if  (returnDict.get(monthDict.get(k)) == None):
            returnDict[monthDict[k]] = v
     
    # changing keys of the dictionary to month-names using dictionary comprehension    
    # returnDict = dict((monthDict[key], value) for (key, value) in orderCountByMonthDict.items())   
        
    return returnDict

# script to be executed to call the defined functions
ordersPath = '/data/retail_db/orders/part-00000'
orders = readData(ordersPath)
ordersCountByMonth = getOrdersCountByMonth(orders)
print(ordersCountByMonth)


#-----------------------------------------------------------------------------------------------------------------------------
# Exercise 3 
# code snippet : 
# Get those order items where order item subtotal is not equal to order item quantity multiplied by order item product price
#-----------------------------------------------------------------------------------------------------------------------------

def readData(dataPath) :
    dataFile = open(dataPath)
    dataStr = dataFile.read()
    dataList = dataStr.splitlines()
    return dataList

# columns in order_items table --> 
# order_item_id,order_item_order_id,product_id,quantity,sub_total,price

def getMismatchSubtotalOrderItems(p_order_items):
    mismathSubtotalOrders = list()
    for orderItem in p_order_items:
        orderItemColumns = orderItem.split(',')
        orderComputedPrice = float(orderItemColumns[5])*int(orderItemColumns[3])
        orderItemSubtotal = float(orderItemColumns[4])
        if orderComputedPrice != orderItemSubtotal:
            mismathSubtotalOrders.append(orderItemColumns[1])
            
    return mismathSubtotalOrders

# script to be executed to call the defined functions
orderItemsPath = '/data/retail_db/order_items/part-00000'
orderItems = readData(orderItemsPath)
mismathSubtotalOrderItems = getMismatchSubtotalOrderItems(orderItems)
    
print(len(orderItems))  # 172198
print(len(mismathSubtotalOrderItems)) # 9763


#-----------------------------------------------------------------------------------------------------------------------------
# Exercise 4 
# code snippet : 
# Get all those order details from orders where there are no corresponding order items
#-----------------------------------------------------------------------------------------------------------------------------

''' my understanding of the problem : get all the orders(order-details) for which there is no reference in ORDER_ITEMS 

 --------------------------------------------------------------------------------------------------------- 
 sql logic - 
 # join order_items and orders to find those order_items whose order_id is not present in orders.

 select o.* from orders o
 where not exists ( select 1 from order_items oi
                    where o.order_id = oi.order_id)
                    
 ---------------------------------------------------------------------------------------------------------                   

 Python colleciton logic on distributed data - 
 # create a dictionary of {order_id: orderItems_record} type - orderItemsDict
 # create a dictionary of {order_id: orders_record} type - ordersDict
 # compare the keys of the two dictionaries and append to a list when there is a mismatch

'''
def readData(dataPath) :
    dataFile = open(dataPath)
    dataStr = dataFile.read()
    dataList = dataStr.splitlines()
    return dataList

def createDictForCollectionAndKey(collection,keyIndexPos):
    '''assumption : specified key at keyIndexPos is the unique key in the collection
                  : columns are comma seperated'''
    returnDict = {}
    for rec in collection:
        recCols = rec.split(',')
        recKey = recCols[keyIndexPos]
        returnDict[recKey] = rec
    return returnDict

def compareDictsForMismatch(p_dict1,p_dict2):
    '''format : functionName(child-Dict,parent-Dict)
       input : 2 dicts for comaprison
       returns : details of mismatch keys from p_dict2
       ** returns a list of records from p_dict2 for which there are no associated references in p_dict1'''
    returnList = list()
    for key in p_dict2:
        if key not in p_dict1:
            returnList.append(p_dict2[key])
    return returnList

# script to be executed to call the defined functions     
ordersPath = '/data/retail_db/orders/part-00000'
orders = readData(ordersPath)
orderItemsPath = '/data/retail_db/order_items/part-00000'
orderItems = readData(orderItemsPath)

ordersDict = createDictForCollectionAndKey(orders,0)
orderItemsDict = createDictForCollectionAndKey(orderItems,1)

mismatchList = compareDictsForMismatch(orderItemsDict,ordersDict)

# validation
len(mismatchList)  # 11452
len(orders) # 68883



#-----------------------------------------------------------------------------------------------------------------------------
# Exercise 5 
# code snippet : 
# Get all those products whose daily revenue is more than $1000 – we need order_date, product_id and product_revenue
#-----------------------------------------------------------------------------------------------------------------------------
'''
--------------------------------------------------------------------------------------------------------- 
sql logic - 

select o.order_date,oi.order_item_product_id,sum(oi.order_item_subtotal)
from orders o, order_items oi
where o.order_id = oi.order_item_order_id
group by o.order_date,oi.order_item_product_id
having sum(oi.order_item_subtotal) > 1000;

3298 rows in set
 ---------------------------------------------------------------------------------------------------------                   
 Python colleciton logic on distributed data - 

# create a dict from order_items of the form - { order_id : 'order_id, product_id, product_revenue'}
 create a dict with order_id : order_date from orders
 append order date into the first dict

'''

