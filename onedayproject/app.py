from flask import Flask,jsonify,json
from pyspark.sql import SparkSession


app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False



spark = SparkSession.builder.appName(
  'dummy').getOrCreate()

file2 = spark.read.csv('/Users/somalayashwanthreddy/Downloads/mockproject/onedayproject/*.csv', sep=',',
          inferSchema=True, header=True)

# #q9
file2.createOrReplaceTempView("data")

@app.route('/highest_and_lowest_prices_for_stock',methods=['GET'])
def highest_and_lowest_prices_for_stock():
    x=spark.sql("select CompanyName,MAX(High) as High,MIN(Low) as Low from data group by CompanyName  ")
    results1 = x.toJSON().map(lambda j: json.loads(j)).collect()


    return jsonify(results1,200)

@app.route('/higher_avg_volume',methods=['GET'])
def higher_avg_volume():
    x=spark.sql(
        "select CompanyName,AVG(Volume) as avgvolume from data group by CompanyName order by avgvolume DESC limit 1 ")
    print(type(x))
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/avg_volume_over_period',methods=['GET'])
def avg_volume_over_period():
    x=spark.sql("select AVG(Volume) as avgvolume from data  ")

    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/mean_meadian_stocks',methods=['GET'])
def mean_meadian_stocks():
    x=spark.sql("select CompanyName,AVG(Open) as mean from data group by CompanyName ")

    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    y=spark.sql(
        "select distinct * from (select CompanyName,PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY Open) OVER (PARTITION BY CompanyName) AS Median_UnitPrice from data)")
    results1 = y.toJSON().map(lambda j: json.loads(j)).collect()

    final={}
    final["mean"]=results
    final["median"]=results1

    return jsonify(final, 200)

@app.route('/std_stock_of_stock',methods=['GET'])
def std_stock():
    x=spark.sql("select CompanyName,STDDEV(Open) as STD_OpenPrice from data group by CompanyName ")
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/maxgained_stock_overperiod',methods=['GET'])
def maxstock_moved_fromdayone_to_lastday():
    x=spark.sql("select CompanyName,Open,High,(High-Open) as max_diff from (Select CompanyName, (Select Open from data limit 1) as Open, max(High) as High from data group by CompanyName)data order by max_diff desc limit 1")
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/maxgap_up_and_gap_down',methods=['GET'])
def  maxgap_up_and_gap_down():
    x=spark.sql("with added_previous_close as (select CompanyName,OPen,Date,Close,LAG(Close,1,35.724998) over(partition by CompanyName order by Date) as previous_close from data ASC) select CompanyName,ABS(previous_close-Open) as max_swing from added_previous_close order by max_swing DESC ")
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/most_traded_stock_inday',methods=['GET'])
def most_tradedstock_daywise():
    x=spark.sql("WITH added_dense_rank AS (SELECT Date,CompanyName,Volume , dense_rank() OVER ( partition by Date order by Volume desc ) as dense_rank FROM data) select Date,CompanyName,Volume FROM added_dense_rank where dense_rank=1")
    results = x.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(results, 200)

@app.route('/maximum_percentage_up_and_down',methods=['GET'])
def stock_moved_maximum_percentage_wise_both_directions():
    x=spark.sql("WITH added_dense_rank AS (SELECT Date,CompanyName,(High-Open)/Open as uppercentage, dense_rank() OVER ( partition by Date order by (High-Open)/Open desc ) as dense_rank FROM data) select Date,CompanyName,uppercentage FROM added_dense_rank where dense_rank=1")
    x1 = spark.sql(
        "WITH added_dense_rank AS (SELECT Date,CompanyName,(Open-Low)/Open as Down_Percentage, dense_rank() OVER ( partition by Date order by (Open-Low)/Open desc ) as dense_rank FROM data) select Date,CompanyName,Down_Percentage FROM added_dense_rank where dense_rank=1")


    x1=x1.withColumnRenamed("CompanyName", "maxdown_company")
    x=x.withColumnRenamed("CompanyName", "maxup_company")

    output=x.join(x1,['Date'],how='inner')
    output.show(20,False)
    final = output.toJSON().map(lambda j: json.loads(j)).collect()
    return jsonify(final, 200)








app.run(host='0.0.0.0',port=5008)










