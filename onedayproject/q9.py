from pyspark.sql import SparkSession

spark = SparkSession.builder.appName(
  'Read All CSV Files in Directory').getOrCreate()

file2 = spark.read.csv('/Users/somalayashwanthreddy/Downloads/mockproject/onedayproject/*.csv', sep=',',
          inferSchema=True, header=True)

# #q9
file2.createOrReplaceTempView("data")
# spark.sql("select CompanyName,MAX(High),MIN(Low) from data group by CompanyName  ").show(100,False)
#
# # #q8
# spark.sql("select CompanyName,AVG(Volume) as avgvolume from data group by CompanyName order by avgvolume DESC limit 1 ").show(100,False)
#
#
# #q7
# spark.sql("select CompanyName,AVG(Volume) as avgvolume from data group by CompanyName order by avgvolume ASC ").show(100,False)

#q6
spark.sql("select CompanyName,AVG(Open) from data group by CompanyName ").show(100,False)

query="select v.*    \
             from (select v.*,\
             row_number() over (partition by CompanyName order by Date asc) as seqnum_asc,\
             row_number() over (partition by CompanyName order by Date desc) as seqnum_desc\
             from data v) v\
             where seqnum_asc = 1 or seqnum_desc = 1 "



spark.sql(query).show(40,False)



#
# #q5
# spark.sql("select CompanyName,STDDEV(Open) from data group by CompanyName ").show(100,False)
#
#
# #q4
# spark.sql("select CompanyName from data group by CompanyName")
