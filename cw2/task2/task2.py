from pyspark import SparkContext
from operator import add

sc = SparkContext('local','pyspark')
def age_group(age):
	if age<10:
		return '0-10'
	elif age<20:
		return '10-20'
	elif age<30:
		return '20-30'
	elif age<40:
		return '30-40'
	elif age<50:
		return '40-50'
	elif age<60:
		return '50-60'
	elif age<70:
		return '60-70'
	elif age<80:
		return '70-80'
	else:
		return '80+'

def parse_with_age_group(data):
	userid,age,gender,occupation,zip = data.split('|')
	return userid,age_group(int(age)),gender,occupation,zip,int(age)

fs = sc.textFile("file:///home/cloudera/cw2/u.user")
# data of users in age group [40,50)
data_with_age_40_50 = fs.map(lambda x:parse_with_age_group(x)).filter(lambda x:x[1]=='40-50')
# occupation frequency for age group [40,50)
occup_fre_of_40_50 = data_with_age_40_50.map(lambda x:x[3]).countByValue()
occup_fre_of_40_50 = sc.parallelize(occup_fre_of_40_50.items())
''' 
order the occupation frequency by its key, i.e. the frequency in descending order, take the top 10 most frequent pairs
'''
ten_most_fre_of_40_50 = occup_fre_of_40_50.takeOrdered(10,key=lambda x:-x[1])
# get the top 10 most frequent occupations for age group [40,50)
ten_most_fre_of_40_50 = sc.parallelize(ten_most_fre_of_40_50).map(lambda x:x[0])

# data of users in age group [50,60)
data_with_age_50_60 = fs.map(lambda x:parse_with_age_group(x)).filter(lambda x:x[1]=='50-60')
# occupation frequency for age group [50,60)
occup_fre_of_50_60 = data_with_age_50_60.map(lambda x:x[3]).countByValue()
occup_fre_of_50_60 = sc.parallelize(occup_fre_of_50_60.items())
''' 
order the occupation frequency by its key, i.e. the frequency in descending order, take the top 10 most frequent pairs
'''
ten_most_fre_of_50_60 = occup_fre_of_50_60.takeOrdered(10,key=lambda x:-x[1])
# get the top 10 most frequent occupations for age group [50,60)
ten_most_fre_of_50_60 = sc.parallelize(ten_most_fre_of_50_60).map(lambda x:x[0])

''' 
get the intersection of the two RDDs, which is the occupations among top 10 most frequent occupations in both age groups
'''
result = ten_most_fre_of_40_50.intersection(ten_most_fre_of_50_60).collect()
for re in result:
	print(re)
