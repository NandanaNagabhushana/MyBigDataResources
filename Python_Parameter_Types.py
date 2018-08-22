# default arguments
def min_max(numbers):
	smallest = largest = numbers[0]
	for item in numbers:
		if item > largest:
			largest = item
		elif item < smallest:
			smallest = item
	return smallest,largest

# keyword/named arguments	
def func(a,b=5,c=10):
	print('a is ',a,' ; b is ',b,' ; c is ',c)

# variable length arguments	
def total(initial=5,*numbers):
	count=initial
	for num in numbers:
		count+= num
	return count
	
# keyword-only : key = value pairs )	
def total(initial = 5, *numbers, **keywords):
	count = initial
	for num in numbers:
		count += num
	for key in keywords:
		count += keywords[key]
	return count
