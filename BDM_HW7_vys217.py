from pyspark import SparkContext as sc
from datetime import date, datetime
from datetime import timedelta
import pyproj

def f(splitIndex, iterator): 
    if splitIndex == 0:
        iterator.next()
    import csv
    reader = csv.reader(iterator)
    for row in reader:
        if (row[3][:10] == '2015-02-01') & (row[6] == 'Greenwich Ave & 8 Ave'):
            a = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S+%f")
            yield a, row[0]
            
def cal_dist(origin, destination):
    lat1, lon1 = origin
    lat2, lon2 = destination
    
    proj = pyproj.Proj(init='EPSG:2263', preserve_units = True)
    station = proj(lon1, lat1)
    station = map(lambda x: x/5280, station)
    
    ys = proj(lon2, lat2)
    ys = map(lambda x: x/5280, ys)
    
    return np.sqrt((ys[0] - station[0])**2 + (ys[1] - station[1])**2)

def filter_recs_dist(splitIndex, iterator):
    if splitIndex == 0:
        iterator.next()
    import csv
    reader = csv.reader(iterator)
    for row in reader:
        if (row[4] != 'NULL') & (row[5] != 'NULL'):
            if cal_dist((40.73901691,-74.00263761),(float(row[4]), float(row[5]))) <= 0.25:
                a = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S.%f")
                b = a + timedelta(seconds = 600)
                yield a,b
                
yellow = sc.textFile('/tmp/yellow.csv.gz')
citibike = sc.textFile('/tmp/citibike.csv')

cb = citibike.mapPartitionsWithIndex(f)
yd = yellow.mapPartitionsWithIndex(filter_recs_dist)

cb_df = cb.toDF(['started', 'ride'])
yd_df = yd.toDF(['dropped', 'extended'])

d = yd_df.join(cb_df).filter((yd_df.dropped < cb_df.started) & (yd_df.extended > cb_df.started))

print len(d.toPandas()['ride'].unique())