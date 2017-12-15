import json

from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, DateType, IntegerType
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import UserDefinedFunction
from pyspark.storagelevel import StorageLevel
from flask import Flask, jsonify
from utils import crossdomain
app = Flask(__name__)

conf = SparkConf() \
    .setMaster("spark://172.21.0.14:7077") \
    .setAppName("tv-scenes") \
    .set("spark.executor.memory", "1g") \
    .set("spark.ui.port", 4040)

sc = SparkContext(conf=conf)
sc.addPyFile("foo.py")
sqlContext = SQLContext(sc)


CSV_PATH = 'file:///home/ubuntu/DWDB/'


def read_csv_into_temptable(table_name):
    filename = "{}/{}.csv".format(CSV_PATH, table_name)
    df = sqlContext.read.csv(filename, header=True, inferSchema=True)
    df.registerTempTable(table_name)


# load data

read_csv_into_temptable('EventClientChannelTune')
read_csv_into_temptable('Channels')
read_csv_into_temptable('Purchase')

def wrap_result(generator):
    return jsonify({
        'result': list(generator)
    })

sql = lambda sql: sqlContext.sql(sql)

@app.route('/购买情况/<method>/<period>')
@crossdomain('*')
def 购买情况(method, period):

    lines = sql('select Created, Currency, Price from Purchase')
    lines = lines.withColumn('年',  
        UserDefinedFunction(lambda x: x.year, IntegerType())('Created'))
    lines = lines.withColumn('天', 
        UserDefinedFunction(lambda x: x.date(), DateType())('Created'))
    lines = lines.withColumn('季度', 
        UserDefinedFunction(lambda d: "{yr}Q{q}".format(yr=d.year, q=(d.month + 2) // 3), StringType())('Created'))

    lines.persist()

    取次数 = lambda 周期: lines.groupBy(周期).count().sort(周期).persist()
    取平均金额 = lambda 周期: lines.filter('Currency = "CN"').filter('Price > 0').groupBy(周期).avg('Price').sort(周期).persist()

    result = (取次数 if method == '次数' else 取平均金额)(period)
    return wrap_result([
        row[period].isoformat() if period == '天' else row[period], 
        row['count'] if method == '次数' else row['avg(Price)']
    ] for row in result.collect())



@app.route('/设备在线数量')
@crossdomain('*')
def 设备在线数量():
    result = sql('''
        select 
            date(OriginTime) as Date, 
            count(distinct DeviceId) as DeviceCount 
        from EventClientChannelTune
        group by Date
        order by Date
    ''')

    return wrap_result([
        row['Date'].isoformat(),
        row['DeviceCount']
    ] for row in result.collect())

@app.route('/收视率')
@crossdomain('*')
def 收视率():
    result = sql('''
        select 
            Year, Month, TotalDuration, ChannelName 
        from (
            select 
                year(OriginTime) as Year,
                month(OriginTime) as Month, 
                ChannelNumber, 
                sum(Duration) as TotalDuration 
            from 
                EventClientChannelTune 
            group by 
                Year, Month, ChannelNumber
        ) join Channels on Channels.ChannelNumber = EventClientChannelTune.ChannelNumber
    ''')
    return wrap_result([
        "{}-{}".format(row['Year'], row['Month']),
        row['ChannelName'],
        row['TotalDuration'] / 86400. / 1000.
    ] for row in result.collect())


@app.route('/用户在线天数')
@crossdomain('*')
def 用户在线天数():
    result = sql('''
        select 
            Year, 
            Month, 
            OnlineDays,
            count(OnlineDays) as OnlineDaysCount 
        from (
            select
                year(OriginTime) as Year,
                month(OriginTime) as Month,
                count(distinct date(OriginTime), DeviceId) as OnlineDays
            from 
                EventClientChannelTune
            group by 
                Year, Month, DeviceId
        ) 
        group by 
            Year, Month, OnlineDays
    ''')

    return wrap_result([
        "{}-{}".format(row['Year'], row['Month']),
        row['OnlineDays'],
        row['OnlineDaysCount']
    ] for row in result.collect())


@app.route('/换台次数')
@crossdomain('*')
def 换台次数():
    result = sql('''
        select 
            DeviceOnDate,
            count(DeviceOnDate) as DeviceOnDateCount 
        from (
            select 
                date(OriginTime) as Date,
                count(DeviceId) as DeviceOnDate
            from
                EventClientChannelTune
            group by 
                Date, DeviceId
        )
        group by
            DeviceOnDate
        order by
            DeviceOnDate
    ''')

    return wrap_result([
        row['DeviceOnDate'],
        row['DeviceOnDateCount']
    ] for row in result.collect())


if __name__ == '__main__':
    import sys
    if '--interactive' in sys.argv:
        from IPython import embed
        embed()
    else: # if '--httpserver' in sys.argv
        app.run(host='0.0.0.0', port=5000)