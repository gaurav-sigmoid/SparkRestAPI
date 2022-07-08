import findspark

findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


class Queries:
    s_df = spark.read.csv(r"csv_files", inferSchema=True, header=True)
    s_df = s_df.withColumn('Date', s_df['Date'].cast('date'))
    s_df = s_df.drop_duplicates()
    s_df.createTempView("stocks")



    def max_diff_stock_percent(self):
        try:
            query1 = "Select stock_table.company, stock_table.date, stock_table.max_diff_stock_percent from " \
                     "(Select date,company,((high-open)/open)*100 as max_diff_stock_percent, dense_rank() " \
                     "OVER ( partition by date order by ( high-open)/open desc ) as dense_rank FROM stocks)stock_table " \
                     "where stock_table.dense_rank=1"
            data = spark.sql(query1).collect()
            results = {}
            for row in data:
                results[row['date'].strftime('%Y-%m-%d')] = {'company': row['company'], 'max_diff_stock_percent': row['max_diff_stock_percent']}
            return results
        except Exception as e:
            print(e)

    def most_traded_stock_per_day(self):
        try:
            query2 = "Select stock_table.date, stock_table.company, stock_table.volume from (Select date, company, volume, dense_rank() " \
                     "over (partition by date order by volume desc) as dense_rank from stocks)stock_table where stock_table.dense_rank=1"
            data = spark.sql(query2).collect()
            results = {}
            for row in data:
                results[row['date'].strftime('%Y-%m-%d')] = {'company': row['company'], 'date': row['date'],
                                                             'volume': row['volume']}
            return results
        except Exception as e:
            print(e)

    def stock_with_most_gap(self):
        try:
            query3 = "Select stock_table.company,abs(stock_table.previous_close-stock_table.open) as max_gap from (Select company, open, date, close, lag(close,1,35.724998) over(partition by company order by date) as previous_close from stocks asc)stock_table order by max_gap desc limit 1"
            data = spark.sql(query3).collect()
            results = {}
            for row in data:
                results['company'] = row['company']
                results['max_gap'] = row['max_gap']
            return results
        except Exception as e:
            print(e)

    def stock_with_max_movement(self):
        try:
            query_4 = "Select stocks.company, stocks.open, stocks.high, (stocks.high - stocks.open) " \
                      "as max_diff from (Select company, (Select open from stocks limit 1) as open, max(high) as high " \
                      "from stocks group by company)stocks order by max_diff desc limit 1"
            data = spark.sql(query_4).collect()
            results = {}
            for row in data:
                results['company'] = row['company']
                results['open'] = row['open']
                results['high'] = row['high']
                results['max_diff'] = row['max_diff']
            return results
        except Exception as e:
            print(e)

    def standard_deviation_for_each_stock(self):
        try:
            query_5 = spark.sql(
                "Select company, stddev_samp(Volume) as Standard_Deviation from stocks group by company")
            data = query_5.collect()
            data = dict(data)
            results = []
            for key, val in data.items():
                results.append({'Company': key, 'Standard_Deviation': val})
            return results
        except Exception as e:
            print(e)

    def mean_median_for_each_stock(self):
        try:
            query6 = "Select company, avg(open) as mean, percentile_approx(open,0.5) as median from stocks group by company"
            data = spark.sql(query6).collect()
            results = []
            for row in data:
                results.append({'company': row['company'], 'mean': row['mean'], 'median': row['median']})
            return results
        except Exception as e:
            print(e)

    def average_volume_per_stock(self):
        try:
            query7 = """select Company, AVG(Volume) as Average_Volume from stocks group by Company order by Average_Volume desc """
            data = spark.sql(query7).collect()
            data = dict(data)
            results = []
            for key, val in data.items():
                results.append({'Company': key, 'Average_Volume': val})
            return results
        except Exception as e:
            print(e)

    def stock_with_highest_average_volume(self):
        try:
            query_8 = """
                select Company, AVG(Volume) as Average_Volume from stocks group by Company order by Average_Volume desc limit 1
            """
            data = spark.sql(query_8).collect()
            data = dict(data)
            results = []
            for key, val in data.items():
                results.append({'company': key, 'max_average_volume': val})
            return results
        except Exception as e:
            print(e)

    def highest_lowest_price_for_each_stock(self):
        try:
            query9 = """
                    select Company, MAX(high) as Highest_Price, MIN(low) as Lowest_Price from stocks group by Company
                """
            data = spark.sql(query9).collect()
            print(data)
            results = []
            for row in data:
                results.append(
                    {'company': row['Company'], 'highest_price': row['Highest_Price'], 'lowest_price': row['Lowest_Price']})
            return results
        except Exception as e:
            print(e)
