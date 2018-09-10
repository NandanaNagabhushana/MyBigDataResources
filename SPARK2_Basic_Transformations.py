------------------------------------------- SPARK 2 -----------------------------------------------------
'''
export SPARK_MAJOR_VERSION=2

pyspark --master yarn --conf spark.ui.port=12901 --num-executors 2

'''
orderItems = sc.textFile()


l = list(range(1,100001))
type(l)
lrdd = sc.parallelize(l) #transformation - collection to an RDD
type(lrdd)

lrdd.count()  #action

lrdd.take(10) #action; 

# for a collection, indexing has to be used --> l[0:11]

lrdd.collect() # action --> performs serialization of an RDD (parallelized collection) into a python collection

# transformation --> filter : horizontal slicing of data , select the entise records based on a filter criteria ; similar to "select * from tab where filter-condition" 
 evenRdd = lrdd.filter(lambda n:n%2==0)
 evenRdd.count()

# action --> reduce using a lambda function 
 sumEven = evenRdd.reduce(lambda en1,en2:en1+en2)
 sumEven
 
# action --> reduce using add ; import from operator
from operator import add
 addEven = evenRdd.reduce(add)
 
# word-count program --> flatMap, map and reduceByKey 
# diff b/w reduce and reduceByKey = reduce will operate on the entire RDD, reduceByKey will operate on sets of data defined by the specified keys
# flatMap(row-level transformation) flattens the collection, along with transforming it to another format that is specified; with flatmap, the number if records in the output collection increases; *** the logic of the lambda function should always return a collection, so that it will be flattened out

# map(row-level transformation) transforms each row in the collection to another specified format. The number of o/p records is same as the number of i/p records

 l = ["hello how are you?","thank you","you are welcome","most welcome"]
 lrdd = sc.parallelize(l)
words = lrdd. flatMap(lambda s:s.split(" "))
wordsMap = words.map(lambda w:(w,1))  # returns tuples of the form (word,1)
wordsMap.take(11)
[('hello', 1), ('how', 1), ('are', 1), ('you?', 1), ('thank', 1), ('you', 1), ('you', 1), ('are', 1), ('welcome', 1), ('most', 1), ('welcome', 1)]

wordsCountByKey = wordsMap.reduceByKey(lambda x,y:x+y)
for i in wordsCountByKey.collect(): print(i)

...
('you?', 1)
('you', 2)
('thank', 1)
('most', 1)
('welcome', 2)
('how', 1)
('are', 2)
('hello', 1)


from operator import add
wordsCountAdd = wordsMap.reduceByKey(add)

for i in wordsCountAdd.collect(): print(i)

#----------------------------------------------------------------
orderItems = sc.textFile("/public/retail_db/order_items")
for i in orderItems.take(10): print(i)

# record format : order_item_id, order_id, product_id,quantity,product_revenue,product_price
# problem statement : get revenue by order-id
'''
step 1 : map the record into tuples of the format (order_id,product_revenue)
step 2 : reduceByKey on order_id, sum up the product_revenue
'''
orderRevenueMap = orderItems.map(lambda oi:(int(oi.split(',')[1]),float(oi.split(',')[4])))
orderRevenueMap.take(10)

[(1, 299.98), (2, 199.99), (2, 250.0), (2, 129.99), (4, 49.98), (4, 299.95), (4, 150.0), (4, 199.92), (5, 299.98), (5, 299.95)]

'''when groupByKey is used on the above mentioned dataset, the result is of the form [(key-1,[value-1,...,value-n]), ... ,(key-n,[value-1,...,value-n])]. This will only group the values pertaining to the key into an iterable and give out tuples. A combiner logic has to be later used to aggregate the iterable portion of the (k,val-set) tuple for each key

for eg. the data set 
[(1, 299.98), (2, 199.99), (2, 250.0), (2, 129.99), (4, 49.98), (4, 299.95), (4, 150.0), (4, 199.92), (5, 299.98), (5, 299.95)]  on applying groupByKey will result in 
[(1,[299.98])(2,[199.99,250.0,129.99]),(4,[49.98,299.95,150.0,199.92]),(5,[299.98,299.95])]

'''
 orGrpByKey = orderRevenueMap.groupByKey()
for i in orGrpByKey.take(10):print(i)

...
(2, <pyspark.resultiterable.ResultIterable object at 0x6c306fc70e50>)
(4, <pyspark.resultiterable.ResultIterable object at 0x6c306fc70590>)
(8, <pyspark.resultiterable.ResultIterable object at 0x6c306fc70d10>)
(10, <pyspark.resultiterable.ResultIterable object at 0x6c306fc704d0>)
(12, <pyspark.resultiterable.ResultIterable object at 0x6c306fc70950>)
(14, <pyspark.resultiterable.ResultIterable object at 0x6c306fd9cc10>)
(16, <pyspark.resultiterable.ResultIterable object at 0x6c306fd9cfd0>)
(18, <pyspark.resultiterable.ResultIterable object at 0x6c306fd9cc90>)
(20, <pyspark.resultiterable.ResultIterable object at 0x6c306fd9cf10>)
(24, <pyspark.resultiterable.ResultIterable object at 0x6c306fd9ca50>)

orderRevenue = orGrpByKey.map(lambda o :(o[0],sum(o[1])))
for i in orderRevenue.take(10):print(i) 
...
(2, 579.98)
(4, 699.85)
(8, 729.8399999999999)
(10, 651.9200000000001)
(12, 1299.8700000000001)
(14, 549.94)
(16, 419.93)
(18, 449.96000000000004)
(20, 879.8599999999999)
(24, 829.97)

'''
groupByKey is considered as the most primitive aggregate transformation, because it does not use a combiner logic.

for and aggregate operation like , it is better to use reduceByKey or aggregateByKey as these will use a combiner logic, and are much more efficient.

'''

revenueByOrderId = orderRevenueMap.reduceByKey(lambda x,y:x+y)

revenueByOrderId.count()

for i in revenueByOrderId.take(10):print(i)
...
(2, 579.98)
(4, 699.85)
(8, 729.8399999999999)
(10, 651.9200000000001)
(12, 1299.8700000000001)
(14, 549.94)
(16, 419.93)
(18, 449.96000000000004)
(20, 879.8599999999999)
(24, 829.97)


from operator import add
revenueByOrderId = orderRevenueMap.reduceByKey(add)

for i in revenueByOrderId.take(10):print(i)
...
(2, 579.98)
(4, 699.85)
(8, 729.8399999999999)
(10, 651.9200000000001)
(12, 1299.8700000000001)
(14, 549.94)
(16, 419.93)
(18, 449.96000000000004)
(20, 879.8599999999999)
(24, 829.97)
