import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.functions import month, rank, col, dayofmonth
from pyspark.sql.window import Window

pd.options.display.width = 0

class BatchLayer():

    def __init__(self):
        self.spark = SparkSession.builder \
            .master('local') \
            .appName('self.appName') \
            .getOrCreate()

    def read_data(self):
        self.df = self.spark.read.format('csv').option('header','true').\
                    load('/media/sid/0EFA13150EFA1315/NYCTaxiData/trip_data_*.csv')
        print(type(self.df.head()))

        top 10 drivers distance wise
        self.top_10_dist = self.df.groupby('medallion').agg(func.sum('trip_distance').alias('sum_trip_distance'))\
                        .orderBy('sum_trip_distance',ascending=False)
        print(self.top_10_dist.head(5))
        self.top_10_dist.coalesce(1).write.format("com.databricks.spark.csv").\
            option("header", "true").save("results/top_10_dist.csv")

        #top 10 drivers who spent most time driving
        self.top_10_time = self.df.groupby('medallion').agg(func.sum('trip_time_in_secs').alias('sum_trip_time'))\
                        .orderBy('sum_trip_time',ascending=False)
        print(self.top_10_time.head(5))
        self.top_10_time.coalesce(1).write.format("com.databricks.spark.csv"). \
            option("header", "true").save("results/top_10_time.csv")

        #top 10 drivers number of trips wise
        self.top_10_trips = self.df.groupby('medallion').agg(func.count('medallion').alias('count'))\
                            .orderBy('count',ascending=False)
        print(self.top_10_trips.head(5))
        self.top_10_trips.coalesce(1).write.format("com.databricks.spark.csv"). \
            option("header", "true").save("results/top_10_trips.csv")

        #total distance in a month
        self.month_dist = self.df.groupby(month('pickup_datetime').alias('month'))\
                         .agg(func.sum('trip_distance').alias('sum_trip_distance'))\
                         .orderBy('sum_trip_distance',ascending=False)
        print(self.month_dist.head(5))
        self.month_dist.coalesce(1).write.format("com.databricks.spark.csv"). \
            option("header", "true").save("results/month_dist.csv")

        #total time in a month
        self.month_time = self.df.groupby(month('pickup_datetime').alias('month')) \
            .agg(func.sum('trip_time_in_secs').alias('sum_trip_time')) \
            .orderBy('sum_trip_time', ascending=False)
        print(self.month_time.head(5))
        self.month_time.coalesce(1).write.format("com.databricks.spark.csv"). \
            option("header", "true").save("results/month_time.csv")

        #total trips in a month
        self.month_trips = self.df.groupby(month('pickup_datetime').alias('month')) \
            .agg(func.count('trip_time_in_secs').alias('count_trip_time')) \
            .orderBy('count_trip_time', ascending=False)
        print(self.month_trips.head(5))
        self.month_trips.coalesce(1).write.format("com.databricks.spark.csv"). \
            option("header", "true").save("results/month_trips.csv")

        #window function
        window = Window.partitionBy('medallion').orderBy(col("trip_distance").desc())
        self.dff = self.df.select('medallion','trip_distance').groupby('medallion', 'trip_distance')\
            .count().withColumn("rank",rank().over(window)).filter(col('rank') <= 5)
        print(self.dff.show(n=20))
        self.dff.coalesce(1).write.format("com.databricks.spark.csv"). \
            option("header", "true").save("results/window_function.csv")

        self.df_heat = self.df.groupby(dayofmonth('pickup_datetime').alias('day'),month('pickup_datetime').alias('month'))\
                        .agg(func.sum('trip_distance').alias('sum_trip_distance'),
                             func.sum('trip_time_in_secs').alias('sum_trip_time'))


        # print(self.df_heat.show(n=5))
        self.df_heat.coalesce(1).write.format('com.databricks.spark.csv').option('header','true')\
        .save('results/heat')

if __name__ == '__main__':
    batch_obj = BatchLayer()
    batch_obj.read_data()
