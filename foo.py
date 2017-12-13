from pyspark.sql import SQLContext
from pyspark.sql.types import StringType, DateType, IntegerType
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import UserDefinedFunction


conf = SparkConf() \
    .setMaster("spark://172.21.0.14:7077") \
    .setAppName("tv-scenes") \
    .set("spark.executor.memory", "1g") \
    .set("spark.ui.port", 4040)

sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

def read_from_csv(table_name):
    filename = "file:///home/ubuntu/DWDB/{}.csv".format(table_name)
    return sqlContext.read.csv(filename, header=True, inferSchema=True)



if True: # 场景2：购买情况

    lines = read_from_csv('Purchase')
    lines = lines.withColumn('年', 
        UserDefinedFunction(lambda x: x.year, IntegerType())('Created'))
    lines = lines.withColumn('天', 
        UserDefinedFunction(lambda x: x.year, DateType())('Created'))
    lines = lines.withColumn('季度', 
        UserDefinedFunction(lambda d: "{yr}Q{q}".format(yr=d.year, q=(d.month + 2) // 3), StringType())('Created'))


    取次数 = lambda p: lines.groupBy(p).count()
    取平均金额 = lambda p: lines.filter('Currency = "CN"').filter('Price > 0').groupBy(p).avg('Price')

    print(取次数('季度'))   # DataFrame[季度: string, count: bigint]
    print(取平均金额('年')) # DataFrame[年: int, avg(Price): double]