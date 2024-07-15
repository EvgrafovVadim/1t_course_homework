from pyspark.sql import SparkSession
import random
from datetime import datetime, timedelta
from typing import List
from pyspark.sql.functions import desc, col
from pyspark.sql.types import StringType, StructField, IntegerType, StructType, DateType

number_of_srings = 1234

# date generator
start_date = datetime(2023, 7, 7)
end_date = datetime(2024, 7, 7)
random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
#date: List[int] = []
date = []
for _ in range(number_of_srings): 
    random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    date.append(random_date)

#UserId gebnerateor
user_id: List[int] = []
for _ in range(number_of_srings): 
    user_id.append(random.randint(0, number_of_srings))

#product generateor
prod_and_price = {
    'butter': 150,
    'bread': 200,
    'noutbook': 1000,
    'pencil': 230,
    'pen': 238,
    'bus': 100000,
    'car': 12345,
    'bicycle': 12345
}
product: List[str] = []
price: List[int] = []
for _ in range(number_of_srings): 
    rand_product = random.choice(list(prod_and_price))
    choice_product_price = prod_and_price[rand_product]
    product.append(rand_product)
    price.append(choice_product_price)

#Quantity generateor
quantity: List[int] = []
for i in range(number_of_srings): 
    quantity.append(random.randint(1, 1000))

spark = SparkSession.builder \
    .appName("example") \
    .getOrCreate()

shema_shop = StructType([
    StructField("date", DateType(), True),
    StructField("user_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", IntegerType(), True)
])

data_shop = zip(date, user_id, product, quantity, price)

df_shop = spark.createDataFrame(data_shop, shema_shop)
df_shop.show()

df_shop \
  .coalesce(1) \
  .write \
  .mode('overwrite') \
  .option('header', 'true') \
  .csv('~\output.csv')

spark.stop()