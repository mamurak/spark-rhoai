from datetime import datetime
import time
from os import getenv
from warnings import filterwarnings

filterwarnings('ignore')

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, desc, lag, lead, to_date, sum, trim, upper, when
)
from pyspark.sql.window import Window


s3_endpoint_url = getenv('AWS_S3_ENDPOINT')
s3_access_key_id = getenv('AWS_ACCESS_KEY_ID')
s3_secret_access_key = getenv('AWS_SECRET_ACCESS_KEY')
s3_bucket_name = getenv('AWS_S3_BUCKET')
print(f'S3 endpoint: {s3_endpoint_url}\n'
      f'S3 bucket: {s3_bucket_name}')

SparkSession.stop

spark = (
    SparkSession
    .builder
    .appName('Retail Analytics')
    .config('spark.hadoop.fs.s3a.access.key', s3_access_key_id)
    .config('spark.hadoop.fs.s3a.secret.key', s3_secret_access_key)
    .config('spark.hadoop.fs.s3a.endpoint', s3_endpoint_url)
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
    .getOrCreate()
)


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger('org').setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger('akka').setLevel(logger.Level.ERROR)


sc = SparkContext.getOrCreate();
quiet_logs(sc)


def clean_data(df):
    # remove missing values
    df = df.dropna()
    # remove duplicate data
    df = df.dropDuplicates()
    return df


def read_data(spark, file_format, file_path):
    s3_path = f's3a://{s3_bucket_name}/{file_path}'
    file_reader = spark.read.format(file_format)
    if file_format == 'csv':
        data = file_reader.load(s3_path, header=True)
    else:
        data = file_reader.load(s3_path)
    return data


start = time.time()

sales_df = read_data(spark, 'csv', 'sales.csv')
stock_df = read_data(spark, 'json', 'stock.json')
supplier_df = read_data(spark, 'json', 'supplier.json')
customer_df = read_data(spark, 'csv', 'customer.csv')
market_df = read_data(spark, 'csv', 'market.csv')
logistic_df = read_data(spark, 'csv', 'logistic.csv')

sales_df.show()
stock_df.show()
supplier_df.show()
customer_df.show()
market_df.show()
logistic_df.show()


# data cleaning
sales_df = clean_data(sales_df)
stock_df = clean_data(stock_df)
supplier_df = clean_data(supplier_df)
customer_df = clean_data(customer_df)
market_df = clean_data(market_df)
logistic_df = clean_data(logistic_df)

# convert date columns to date type
sales_df = sales_df.withColumn('date_of_sale', to_date(col('date_of_sale')))
stock_df = stock_df.withColumn('date_received', to_date(col('date_received')))
supplier_df = supplier_df.withColumn('date_ordered', to_date(col('date_ordered')))

# standardize case of string columns
sales_df = sales_df.withColumn('product_name', upper(col('product_name')))
stock_df = stock_df.withColumn('product_name', upper(col('product_name')))
stock_df = stock_df.withColumn('location', upper(col('location')))
supplier_df = supplier_df.withColumn('product_name', upper(col('product_name')))
customer_df = customer_df.withColumn('customer_name', upper(col('customer_name')))
market_df = market_df.withColumn('product_name', upper(col('product_name')))
logistic_df = logistic_df.withColumn('product_name', upper(col('product_name')))

# remove leading and trailing whitespaces
sales_df = sales_df.withColumn('product_name', trim(col('product_name')))
stock_df = stock_df.withColumn('location', trim(col('location')))
supplier_df = supplier_df.withColumn('product_name', trim(col('product_name')))
customer_df = customer_df.withColumn('customer_name', trim(col('customer_name')))
market_df = market_df.withColumn('product_name', trim(col('product_name')))
logistic_df = logistic_df.withColumn('product_name', trim(col('product_name')))

sales_df.show()

# check for invalid values
sales_df = sales_df.filter(col('product_name').isNotNull())
stock_df = stock_df.filter(col('location').isNotNull())
customer_df = customer_df.filter(col('gender').isin('male', 'female'))
market_df = market_df.filter(col('product_name').isNotNull())
logistic_df = logistic_df.filter(col('product_name').isNotNull())

# drop extra columns
market_df = market_df.drop('price')
supplier_df = supplier_df.drop('price')

# join all data
data_int = (
    sales_df.join(stock_df, 'product_name', 'leftouter')
            .join(supplier_df, 'product_name', 'leftouter')
            .join(market_df, 'product_name', 'leftouter')
            .join(logistic_df, 'product_name', 'leftouter')
            .join(customer_df, 'customer_id', 'leftouter')
)

data_int.show()

data_int.printSchema()

# write the cleaned data
s3_path = f's3a://{s3_bucket_name}/cleaned.parquet'
try:
    data_int.write.format('parquet').save(s3_path)
except Exception:
    print('write failed')

end = time.time()

print('Time taken for Data Cleaning: ', end - start)

# DO VARIOUS RETAIL DATA ANALYTICS

start = time.time()

# read cleaned data

data = read_data(spark, 'parquet', 'cleaned.parquet')

# Case when statement to create a new column to indicate whether the product is perishable or not:

data = data.withColumn(
    'perishable', when(col('shelf_life') <= 30, 'yes').otherwise('no')
)

# You can use the when() and otherwise() functions to create new columns based on certain conditions:

data = data.withColumn(
    'sales_status', when(col('quantity_sold') > 50, 'good').otherwise('bad')
)

# create a window to perform time series analysis
window = Window.partitionBy('product_name').orderBy('date_of_sale')

# calculate the rolling average of sales for each product
time_series_df = data.withColumn(
    'rolling_avg_sales', avg('quantity_sold').over(window)
)

# use window function for forecasting

forecast_df = (
    time_series_df
    .withColumn(
        'prev_sales', lag('rolling_avg_sales').over(window))
    .withColumn('next_sales', lead('rolling_avg_sales').over(window))
)

# Calculate the average price of a product, grouped by supplier
forecast_df.groupBy('sup_id').agg({'price': 'avg'}).show()

# Calculate the total quantity in stock and total sales by supplier
(
    forecast_df.groupBy('sup_id')
               .agg({'quantity_in_stock': 'sum', 'price': 'sum'})
               .show()
)

# Calculate the number of perishable v/s non-perishable product per location
forecast_df.groupBy('perishable').agg({'perishable': 'count'}).show()

# Calculate number of good v/s bad sales status per location
forecast_df.groupBy('sales_status').agg({'sales_status': 'count'}).show()

# Count the number of sales that contain a 10% off promotion
countt = (
    forecast_df.filter(forecast_df['contains_promotion'].contains('10% off'))
               .count()
)
print(countt)
# Perform some complex analysis on the DataFrame

# Calculate the total sales, quantity sold by product and location
total_sales_by_product_location = (
    forecast_df.groupBy('product_name', 'location')
               .agg(
                   sum('price').alias('total_price'),
                   sum('quantity_ordered').alias('total_quantity_sold'),
                   avg('quantity_sold').alias('avg_quantity_sold'))
               .sort(desc('total_price'))
)

# Group the data by product_name
grouped_df = forecast_df.groupBy('product_name')

# Sum the quantity_in_stock, quantity_ordered, quantity_sold, and (price * quantity_sold) for each group
aggregated_df = (
    grouped_df.agg(
        sum('quantity_in_stock').alias('total_quantity_in_stock'),
        avg('price').alias('average_price'),
        sum('quantity_ordered').alias('total_quantity_ordered'),
        sum('quantity_sold').alias('total_quantity_sold'),
        sum(col('price') * col('quantity_sold')).alias('total_sales'),
        sum('prev_sales').alias('total_prev_sales'),
        sum('next_sales').alias('total_next_sales'),
    ).sort(desc('total_sales'))
)

end = time.time()

print('Time taken for Data Analysis: ', end - start)

aggregated_df.show()

total_sales_by_product_location.show()

# WRITE THE AGGREGATES TO DISK
timestamp = datetime.now().strftime('%y%m%d%H%M')

aggregated_s3_path = f's3a://{s3_bucket_name}/aggregated-{timestamp}.parquet'
aggregated_df.write.format('parquet').save(aggregated_s3_path)

total_sales_s3_path = f's3a://{s3_bucket_name}/total_sales-{timestamp}.parquet'
total_sales_by_product_location.write.format('parquet').save(total_sales_s3_path)

spark.stop()