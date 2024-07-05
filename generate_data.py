from multiprocessing import cpu_count, Pool
from os import getenv
from random import choice, randint, uniform
from warnings import filterwarnings
filterwarnings('ignore')

from pandas import DataFrame
from pyspark import SparkContext
from pyspark.sql import SparkSession


SparkSession.stop

spark = (
    SparkSession
    .builder
    .appName('Generating and Writing Data')
    .getOrCreate()
)


def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger('org').setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger('akka').setLevel(logger.Level.ERROR)


sc = SparkContext.getOrCreate();
quiet_logs(sc)

s3_endpoint_url = getenv('AWS_S3_ENDPOINT')
s3_access_key_id = getenv('AWS_ACCESS_KEY_ID')
s3_secret_access_key = getenv('AWS_SECRET_ACCESS_KEY')
s3_bucket_name = getenv('AWS_S3_BUCKET')
print(f'S3 endpoint: {s3_endpoint_url}\n'
      f'S3 bucket: {s3_bucket_name}')

s3_storage_options = {
    'key': s3_access_key_id,
    'secret': s3_secret_access_key,
    'client_kwargs': {'endpoint_url': s3_endpoint_url},
}


def _generate_data(generator_func, column_names, n_samples):
    with Pool(cpu_count()) as p:
        data = list(p.map(generator_func, range(n_samples)))

    return DataFrame(data, columns=column_names)


def _save(data, file_name, file_format):
    s3_path = f's3://{s3_bucket_name}/{file_name}'
    if file_format == 'json':
        data.to_json(
            s3_path, orient='records', storage_options=s3_storage_options
        )
    elif file_format == 'csv':
        data.to_csv(
            s3_path,
            index=False,
            header=True,
            storage_options=s3_storage_options
        )
    else:
        print(f'Error: file format {file_format} not supported!')
        return
    print('Write completed')


def generate_and_save(
        generator_func, column_names, file_name, file_format='json', n_samples=5):
    data = _generate_data(generator_func, column_names, n_samples)
    _save(data, file_name, file_format)


def generate_sales_data(i):
    sales_id = f's_{i}'
    product_name = f'Product_{i}'
    price = uniform(1, 100)
    quantity_sold = randint(1, 100)
    date_of_sale = f'2022-{randint(1, 12)}-{randint(1, 28)}'
    customer_id = f'c_{randint(1, 1000000)}'
    return (sales_id, product_name, price, quantity_sold, date_of_sale, customer_id)


sales_column_names = [
    'sales_id',
    'product_name',
    'price',
    'quantity_sold',
    'date_of_sale',
    'customer_id',
]
generate_and_save(
    generate_sales_data,
    sales_column_names,
    file_name='sales.csv',
    file_format='csv',
)


def generate_stock_data(i):
    product_name = f'Product_{i}'
    shelf_life = randint(1, 365)
    contains_promotion = f'{randint(0,10)} % off'
    quantity_in_stock = randint(1, 1000)
    location = f'Location_{randint(1,100)}'
    date_received = f'2022-{randint(1,12)}-{randint(1,28)}'
    return (product_name, shelf_life, contains_promotion, quantity_in_stock, location, date_received)


stock_column_names = [
    'product_name',
    'shelf_life',
    'contains_promotion',
    'quantity_in_stock',
    'location',
    'date_received',
]
generate_and_save(
    generate_stock_data,
    stock_column_names,
    file_name='stock.json',
    file_format='json',
)


def generate_supplier_data(i):
    sup_id = f's_{i}'
    product_name = f'Product_{i}'
    quantity_ordered = randint(1, 1000)
    price = uniform(1, 100)
    date_ordered = f'2022-{randint(1,12)}-{randint(1,28)}'
    return (sup_id, product_name, quantity_ordered, price, date_ordered)


supplier_column_names = [
    'sup_id',
    'product_name',
    'quantity_ordered',
    'price',
    'date_ordered',
]
generate_and_save(
    generate_supplier_data,
    supplier_column_names,
    file_name='supplier.json',
    file_format='json',
)


def generate_customer_data(i):
    customer_id = f'c_{i}'
    customer_name = f'Customer_{i}'
    age = randint(20, 70)
    gender = choice(['male', 'female'])
    purchase_history = randint(1, 100)
    contact_info = f'email_{i}@gmail.com'
    return (customer_id, customer_name, age, gender, purchase_history, contact_info)


customer_column_names = [
    'customer_id',
    'customer_name',
    'age',
    'gender',
    'purchase_history',
    'contact_info',
]
generate_and_save(
    generate_customer_data,
    customer_column_names,
    file_name='customer.csv',
    file_format='csv',
)


def generate_market_data(i):
    product_name = f'Product_{i}'
    competitor_price = uniform(1, 100)
    sales_trend = randint(1, 100)
    demand_forecast = randint(1, 100)
    return (product_name, competitor_price, sales_trend, demand_forecast)


market_column_names = [
    'product_name', 'competitor_price', 'sales_trend', 'demand_forecast'
]
generate_and_save(
    generate_market_data,
    market_column_names,
    file_name='market.csv',
    file_format='csv'
)


def generate_logistic_data(i):
    product_name = f'Product_{i}'
    shipping_cost = uniform(1, 100)
    transportation_cost = uniform(1, 100)
    warehouse_cost = uniform(1, 100)
    return (product_name, shipping_cost, transportation_cost, warehouse_cost)


logistic_column_names = [
    'product_name', 'shipping_cost', 'transportation_cost', 'warehouse_cost'
]
generate_and_save(
    generate_logistic_data,
    logistic_column_names,
    file_name='logistic.csv',
    file_format='csv'
)