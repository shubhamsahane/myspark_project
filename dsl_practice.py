import re

from pyspark.sql import *
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import col,rank,when,dense_rank,lit,row_number,lower,initcap,upper
from pyspark.sql.types import StructType,StructField,StringType,IntegerType


spark = SparkSession.builder.master("local").appName("test").getOrCreate()



data='C:\\bigdata\\datasets\\employees_oracle.csv'
df=spark.read.format('csv').option('header','true').option("inferSchema",'true').load(data)
print(df.show(10))
df.printSchema()

# max salary by department

# df.groupBy('DEPARTMENT_ID').agg(max('salary')).sort('DEPARTMENT_ID').show()
# df.groupBy('DEPARTMENT_ID').agg(max('salary')).sort(col('DEPARTMENT_ID').desc()).show()

#second highest salary by windowing or analytical fun
# windowing =Window.partitionBy('DEPARTMENT_ID').orderBy('salary')
# df=df.withColumn('cnt',dense_rank().over(windowing))
# df.filter(df.cnt==2).show()

#duplicate record
# df=df.select('EMPLOYEE_ID').distinct()
# print(df.count())
# df.groupBy(['EMPLOYEE_ID']) \
#     .count() \
#     .where('count >= 1') \
#     .sort('count', ascending=False) \
#     .show()
#
# df.groupBy(['EMPLOYEE_ID']) \
#     .count() \
#     .filter('count >= 1') \
#     .show()

#drop duplicates
# df.dropDuplicates()

#monotonically increase column
# df1=df.withColumn('new_column',monotonically_increasing_id()+1)
# df2=df1.withColumn('DEPARTMENT_ID',lit('1'))
# df2.show()
# df1.sort(col('new_column').desc()).show(5)
# df1.orderBy(col('new_column').desc()).show(5)
#substring
# df.withColumn('new',substring('FIRST_NAME',1,4)).show()
# df.withColumn('new1',substring_index('PHONE_NUMBER','.',-1)).show()

#Description
# df.describe().show()


#aggregation
# df.groupBy('DEPARTMENT_ID').agg(F.min(df.SALARY)).show()
# df.agg(min(df.SALARY)).show()

#lit
# df.withColumn('new',lit('1')).show()

# join
# df.join(df1, df.col == df1.col1 ,"left").show()
# df.join(broadcast(df1), df.col == df.col1 )


#max salary
# df.agg(max('SALARY')).show()
# df.groupBy('DEPARTMENT_ID').agg(max('SALARY')).sort('DEPARTMENT_ID').show()
# df.groupBy('DEPARTMENT_ID').agg(max('SALARY')).sort(col('DEPARTMENT_ID').desc()).show()
# windowing=Window.orderBy(col('SALARY').desc())
# df.select('SALARY').distinct().sort(col('SALARY')).withColumn("row",row_number().over(windowing)) \
#   .filter(col("row") <= 2) \
#   .show()

#windowing fun
# windowing=Window.partitionBy('DEPARTMENT_ID').orderBy('SALARY')
# df=df.withColumn('cnt',dense_rank().over(windowing))
# df.filter(df.cnt <=3).show()

#duplicate
# df.groupBy(['EMPLOYEE_ID']) \
#     .count() \
#     .filter('count >= 1') \
#     .show()
#
# df.groupBy(['EMPLOYEE_ID']).count().filter('count >=1').show()


# df.dropduplicates()


#countdistinct
# df.agg(countDistinct(df.EMPLOYEE_ID)).show()


#count no. records in
from pyspark.sql.functions import spark_partition_id
# df.show()
# df1=df.repartition(4)
# print(df1.rdd.getNumPartitions())
#
# df2=df1.withColumn('partitionid',spark_partition_id()).groupBy('partitionid').count()
# print(df2.collect())

#
# df1=df.repartition(100,'DEPARTMENT_ID')
# print(df1.rdd.getNumPartitions())
# df2=df1.withColumn('new',spark_partition_id()).groupBy('new').count()
# print(df2.collect())


# df1=df.repartition(10,'DEPARTMENT_ID')
# print(df1.rdd.getNumPartitions())
# df2=df1.withColumn('new',spark_partition_id()).groupBy('new').count()
# print(df2.take(2))



# df.select('first_name','last_name').filter(col('salary') > 16000).show()

#accumulator
# accu=spark.sparkContext.accumulator(0)
# df.foreach(lambda x : accu.add(1))
# print(accu.value)

       # print(df.count())


 # -- INSTEAD of count ....how to count record using any other functions ?

# df1=df.filter(col('salary')>15000)
# print(df1.count())
#
# accu_filter=spark.sparkContext.accumulator(0)
# df1.foreach(lambda x : accu_filter.add(1))
#
# print(accu_filter.value)



#capital initial character of word
# df.select(lower(df.FIRST_NAME)).show()

# for i in df.columns:print(i)
# df1=df.toDF(*[i.lower() for i in df.columns])
# df1.toDF(*[initcap(i) for i in df1.columns]).show()

# df.toDF(*[i for i in df.columns]).show()

# df.withColumn('new',lower(col('FIRST_NAME'))).show()

###### concat and count_ws
# df.withColumn('new', concat_ws(' ',df.FIRST_NAME ,df.LAST_NAME)).show()
# df=df.withColumn('new', concat(df.FIRST_NAME ,df.LAST_NAME))
# df.select(lower(col('new'))).show()
# df.select('new',concat(lower(
# substring('new',1,1)),substring('new',2,12)).alias('sub')).show()

########
# df.toDF(*[regexp_replace(c,r"[^A-Z]","1" )for c in df.columns]).show()


#####
# df.select('*').filter(col('DEPARTMENT_ID').rlike(['0-9'])).show()


########
# df.withColumn('new',when(col('DEPARTMENT_ID')==90,'true').otherwise('false')).show()
# df.withColumn('new',expr("case when DEPARTMENT_ID=90 then 'true' else 'false' end ")).show()
# df.select(when(col('DEPARTMENT_ID')==90,'true').otherwise('false').alias('new')).show()


#######
# df.toDF(*[re.sub(r"['_']",' ',i)  for i in df.columns]).show()

# df.toDF(*[initcap(re.sub(r"['_']",' ',i)) for i in df.columns])

#########  Adding leading zero to column
# df=df.withColumn('new',concat(lit('000'),df.SALARY)).withColumn('new1',lit('shubham_sahane'))
# df=df.withColumn('split',split(col('new1'),'_').getItem(0))
# df.select(regexp_replace(col('new'),r'^[0]*','').alias('col')).show()

# df.select('PHONE_NUMBER', substring_index('new1','_') ).show()

# emptyrdd=spark.sparkContext.emptyRDD()
# emptyrdd= spark.sparkContext.parallelize([])
# schema=StructType([
#     StructField('firstname',StringType(),True),
#     StructField('Lastname',StringType(),True),
#     StructField('Ege',IntegerType(),False)
# ])
#
# emptydf=spark.createDataFrame(emptyrdd,schema)
# emptydf.show()


df2=df.withColumn('Replace',regexp_replace('FIRST_NAME','St','NN')).show()