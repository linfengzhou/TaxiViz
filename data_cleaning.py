import sys

def reducer(last,current):
    dropoff = current.split(',')[1] +','+current.split(',')[4] +','+current.split(',')[5]
    pickup = current.split(',')[0] +','+current.split(',')[2] +','+current.split(',')[3]
    if last != []:
        return last + ',p,'+pickup.encode('ascii','ignore')+'|d,'+dropoff.encode('ascii','ignore')

def mapper(x):
    return x.split('|')[1:]


def toCSVLine(data):
	return ','.join(str(d) for d in data)


taxi_rdd = sc.textFile('../../gws/classes/bdma/cusp/groups/7/Data/trip_data_12.csv')
header = taxi_rdd.first()
taxi_data = taxi_rdd.filter(lambda line: line != header)
lines = taxi_data.map(lambda x: (x.split(',')[0]+'d'+x.split(',')[5].split(' ')[0],x.split(',')[5]+','
	+x.split(',')[6]+','+x.split(',')[10]+','+x.split(',')[11]+','+x.split(',')[12]+','+x.split(',')[13]))
g_lines = lines.groupByKey().mapValues(list).map(lambda (x,y):(x,sorted(y))).flatMapValues(lambda x:x).reduceByKey(lambda x,y: reducer(x,y)).mapValues(lambda x: mapper(x))
new_lines = g_lines.flatMapValues(lambda x:x).map(toCSVLine)
new_lines.saveAsTextFile('emptytaxi/m12')




g_lines = lines.groupByKey().collect()
s_lines = sorted([(x, sorted(y)) for (x, y) in g_lines])
sc_lines = sc.parallelize(s_lines)
fsc_lines = sc_lines.flatMapValues(lambda x:x).reduceByKey(lambda x,y: reducer(x,y)).mapValues(lambda x: mapper(x))
new_lines = fsc_lines.flatMapValues(lambda x:x).map(toCSVLine)


# lines_rbk = lines_gbk.reduceByKey(lambda x,y: reducer(x,y))
lines_rbk = lines.reduceByKey(lambda x,y: reducer(x,y)).mapValues(lambda x: mapper(x))
new_lines = lines_rbk.flatMapValues(lambda x:x).map(toCSVLine)

new_lines.saveAsTextFile('drop_pick/m2')


lines_rbk.flatMapValues(lambda x:x)


if __name__=='__main__':
	sc = pyspark.SparkContext()
	trips = sc.textFile(','.join(sys.argv[1:-1]))




# lines_gbk = lines.groupByKey()

# sh submit.sh group_trips.py taxiviz1/output output.txt 128

'''
for each records,
0 medallion
5 pickup date
6 dropoff date
11 pickup latitude
12 pickup longitude
13 dropoff latitude
14 dropoff longitude
'''

## our mission 

'''
first records 6+','+12+','+13
second records 5+','+10+','+11
'''



## reducer function 
