# to use Spark 2 on Python 3, set the below
export PYSPARK_PYTHON=python3.6
export SPARK_MAJOR_VERSION=2

# set up the config in the below profile file
cat ~/.bash_profile

#launch pyspark
pyspark --master yarn --conf spark.ui.port=12901 --conf spark.dynamicAllocation.enabled=false

# API --> my_rdd.repartition(num_partitions)  --> will divide the data into buckets

#sc.setLogLevel("INFO")

# aggregateByKey should be used when the sequenceOperation and combinerOperation (combiner logic) are different

orderItems = sc.textFile("/public/retail_db/order_items")
orderItemsMap = orderItems.map(lambda oi: (int(oi.split(",")[1]), float(oi.split(",")[4])))

for i in orderItemsMap.take(10): print(i)
...
(1, 299.98)
(2, 199.99)
(2, 250.0)
(2, 129.99)
(4, 49.98)
(4, 299.95)
(4, 150.0)
(4, 199.92)
(5, 299.98)
(5, 299.95)

revenueAndCountByOrder = orderItemsMap.aggregateByKey(
                         (0.0,0), # intitializing the o/p; this enables us to declare the output type eg. (revenue,count)
						 lambda x,y:  #x is the initialized value for 1st iteration; for further iterations, x will hold the intermediate value of the result for the current executor
						              # y is the value from the paired RDD record coming in from the specified collection. eg. if 1st record coming in is (2,199.99), then y will hold 199.99    
									  (x[0]+y,x[1]+1), #result of seq-op lambda function ; format (key,(sum_revenue,sum_count)) for the values found(by the executor) for each key
						 lambda x,y:  # combiner-op : both x and y will hold the intermediate result tuples from each executor for a particular key
									  (x[0]+y[0],x[1]+y[1]) # the tuple-elements, that is (revenue,count) will be added elementwise, to give out 1 tuple per key
									  
									  
revenueAndCountByOrder = orderItemsMap.aggregateByKey(
                         (0.0,0),
						 lambda x,y: (x[0]+y,x[1]+1),
						 lambda x,y: (x[0]+y[0],x[1]+y[1])
													  )	
													  
for i in revenueAndCountByOrder.take(10): print(i)	
...
(2, (579.98, 3))
(4, (699.85, 4))
(8, (729.8399999999999, 4))
(10, (651.9200000000001, 5))
(12, (1299.8700000000001, 5))
(14, (549.94, 3))
(16, (419.93, 2))
(18, (449.96000000000004, 3))
(20, (879.8599999999999, 4))
(24, (829.97, 5))								  
