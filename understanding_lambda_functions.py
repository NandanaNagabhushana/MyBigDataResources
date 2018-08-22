def sumOfIntegers(lb,ub):
	''' using the arithmetic series formula'''
	sum = ((ub*(ub+1)/2) - (lb*(lb+1)/2))+1
	return sum
	
def sumOfIntegers(lb,ub):
	''' using a for loop : inefficient method'''
	sum = 0
	for i in range(lb,ub+1):
		sum += i
	return sum
	
# define a higher order function that takes in a function-type as an argument

def operationOnIntegers(lb,ub,f):
	''' f is of type - function'''
	retval = 0	
	for i in range(lb,ub+1):
		retval += f(i)
	return retval
	
# invoke : operationOnIntegers(5,10,lambda i: i)
# returns : 45	

def operationOnInts(lb,ub,f):
	''' f is of type - function'''
	retval = 0	
	for i in range(lb,ub+1):
		print(f(i))
		retval += f(i)
	return retval

# return sum of cubes	
# invoke : operationOnInts(5,10,lambda i: i**3)
# returns : 
'''
125
216
343
512
729
1000
2925
'''


# return sum of cubes	
# invoke : operationOnInts(5,10,lambda i: i if(i%2 == 0) else 0)
# returns : 45	
'''
0
6
0
8
0
10
24
'''

# when the functionality becomes complex, or spans multiple lines of code, a predefined/named function can be passed into the lambda function 

# getEvenCube is a simple logic that can be achieved inside the lambda one-liner; used here only for understanding
def getEvenCube(n):
	return n**3 if(n%2 == 0) else 0

# return sum of even-cubes	
# invoke : operationOnInts(5,10,lambda i: getEvenCube(i))
# returns : 
'''
0
216
0
512
0
1000
1728
'''

''' lambda function usage scenario:
map
filter
functools.reduce

functionality on the iterable can be passed into these functions, and can be applied on the collection
'''

l = list(range(1,101))

# get sum of even numbers

lEven = filter(lambda f: True if (f%2 == 0) else False, l)
# invoke : for i in lEven: print(i)
