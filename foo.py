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

def read_from_csv(table_name):
    filename = "file:///home/ubuntu/DWDB/{}.csv".format(table_name)
    return sqlContext.read.csv(filename, header=True, inferSchema=True)


def wrap_result(generator):
    return jsonify({
        'result': list(generator)
    })


@app.route('/购买情况/<method>/<period>')
@crossdomain('*')
def 购买情况(method, period):

    lines = read_from_csv('Purchase')
    lines = lines.select('Created', 'Currency', 'Price')
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
    read_from_csv('EventClientChannelTune').registerTempTable('EventClientChannelTune')
    result = sqlContext.sql('''
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

if __name__ == '__main__':
    import sys
    if '--interactive' in sys.argv:
        from IPython import embed
        embed()
    else: # if '--httpserver' in sys.argv
        app.run(host='0.0.0.0', port=5000)