from pyspark import SparkContext
from pyspark.sql import HiveContext
from datetime import date, datetime
from datetime import timedelta
from geopy.distance import vincenty
from pyspark.sql import HiveContext

def citi_reader(splitIndex, iterator): 
    if splitIndex == 0:
        iterator.next()
    import csv
    reader = csv.reader(iterator)
    for row in reader:
        if (row[3][:10] == '2015-02-01') & (row[6] == 'Greenwich Ave & 8 Ave'):
            start = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S+%f")
            yield start, row[0]
            
def filter_records_dist(splitIndex, iterator):
    if splitIndex == 0:
        iterator.next()
    import csv
    reader = csv.reader(iterator)
    for row in reader:
        if (row[4] != 'NULL') & (row[5] != 'NULL'):
            if (vincenty((40.73901691,-74.00263761), (float(row[4]), float(row[5]))).miles) <= 0.25:
                stop = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S.%f")
                extend = stop + timedelta(seconds = 600)
                yield stop, extend

def main(sc):
    spark = HiveContext(sc)
    
    yellow = sc.textFile('/tmp/yellow.csv.gz').cache()
    citibike = sc.textFile('/tmp/citibike.csv').cache()

    cb = citibike.mapPartitionsWithIndex(citi_reader)
    yd = yellow.mapPartitionsWithIndex(filter_records_dist)

    cb_df = cb.toDF(['started', 'ride'])
    yd_df = yd.toDF(['dropped', 'extended'])

    joined_df = yd_df.join(cb_df).filter((yd_df.dropped < cb_df.started) & (yd_df.extended > cb_df.started))

    print joined_df.count()

if __name__ == "__main__":
    sc = SparkContext()
    main(sc)