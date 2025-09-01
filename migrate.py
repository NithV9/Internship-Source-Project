from dotenv import load_dotenv
import os
import mysql.connector
from pymongo import MongoClient
from bson import ObjectId
from collections import defaultdict

#---------------------Database connections
load_dotenv()

#MySQL DB Connection
sql_conn = mysql.connector.connect(
    host=os.getenv('DB_HOST'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD'),
    database=os.getenv('DB_NAME')
)
if(sql_conn):
    print("Successfully connected to sql database")
sql_cursor = sql_conn.cursor(dictionary=True)

#MongoDB Connection
mongo_client = MongoClient(os.getenv('DB_HOST'), os.getenv('DB_PORT'))
mongo_db = mongo_client[os.getenv('DB_NAME')]
mongo_market = mongo_db['market']
mongo_product = mongo_db['product']
mongo_customers = mongo_db['customers']
mongo_forecast = mongo_db['Forecast']
mongo_sales = mongo_db['Sales']

if(mongo_client):
    print("Successfully connected to mongo database")

#indexes
mongo_product.create_index("product_id")
mongo_customers.create_index("customer_id")
mongo_market.create_index("market_id")

#Product documents
product_documents=[]
sql_cursor.execute("SELECT * FROM dim_product")
product_data=sql_cursor.fetchall()

#Adding MongoDB ids to product data
for product in product_data:
    product["_id"]=ObjectId()

#Running sql queries to fetch data and use dictionaries to map relationships between different sets of data
sql_cursor.execute("SELECT * FROM fact_forecast_monthly")
forecast_all = sql_cursor.fetchall()

sql_cursor.execute("SELECT * FROM fact_sales_monthly")
sales_all = sql_cursor.fetchall()

forecast_map = defaultdict(list)
for f in forecast_all:
    f["_id"] = ObjectId()
    forecast_map[f["customer_id"]].append(f["_id"])

sales_map = defaultdict(list)
for s in sales_all:
    s["_id"] = ObjectId()
    sales_map[s["customer_id"]].append(s["_id"])

forecast_product_array=[]
sales_product_array=[]

#Creating Product document schema
for product in product_data:
    product_forecastArray=[]
    product_salesArray=[]
    for forecast in forecast_all:
        if(product["product_id"]==forecast["product_id"]):
            product_forecastArray.append(forecast["_id"])
            forecast_doc={
                "id":forecast["_id"],
                "date":forecast["date"],
                "customer_name":forecast["customer_name"],
                "forecast_quantity":forecast["forecast_quantity"],
                "customer_id":forecast["customer_id"],
                "product_id":forecast["product_id"],
                "forecast_id":forecast["forecast_id"],
                "parent":product["_id"]
            }
            forecast_product_array.append(forecast_doc)
    for sales in sales_all:
        if(product["product_id"]==sales["product_id"]):
            product_salesArray.append(sales["_id"])
            sales_doc={
                "id":sales["_id"],
                "date":sales["date"],
                "customer_name":sales["customer_name"],
                "sold_quantity":sales["sold_quantity"],
                "customer_id":sales["customer_id"],
                "product_id":sales["product_id"],
                "sales_id":sales["sales_id"],
                "parent":product["_id"]
            }
            sales_product_array.append(sales_doc)
    product_doc={
        "_id":product["_id"],
        "product_code":product["product_code"],
        "division":product["division"],
        "segment":product["segment"],
        "category":product["category"],
        "product":product["product"],
        "variant":product["variant"],
        "product_id":product["product_id"],
        "sales_monthly":product_salesArray,
        "forecast_monthly":product_forecastArray,
    }
    product_documents.append(product_doc)

#Forecast and Sales Arrays in Product documents
forecast_product_doc={
    "_id":ObjectId(),
    "product":forecast_product_array,
}
sales_product_doc={
    "_id":ObjectId(),
    "product":sales_product_array,
}

#Inserting Product, Forecast, and Sales documents in their respective collections
mongo_product.insert_many(product_documents)
mongo_forecast.insert_one(forecast_product_doc)
mongo_sales.insert_one(sales_product_doc)

#Market and Customer documents
sql_cursor.execute("SELECT * FROM dim_market")
market_data = sql_cursor.fetchall()

sql_cursor.execute("SELECT * FROM dim_customer")
customer_data = sql_cursor.fetchall()

for customer in customer_data:
    customer["_id"] = ObjectId()


market_documents=[]
customer_documents=[]
#Create new MongoDB ids
for market in market_data:
    market_customers=[]
    marketID=ObjectId()
    for customer in customer_data:
        if(customer["market_id"]==market["market_id"]):
            market_customers.append(customer["_id"])
            customer_doc = {
                "_id": customer["_id"],
                "customer": customer["customer"],
                "platform": customer["platform"],
                "channel": customer["channel"],
                "customer_code": customer["customer_code"],
                "customer_id": customer["customer_id"],
                "market_id": customer["market_id"],
                "forecast_monthly": forecast_map[customer["customer_id"]],
                "sales_monthly": sales_map[customer["customer_id"]],
                "parent":marketID
            }
            customer_documents.append(customer_doc)
    market_doc={
        "_id":marketID,
        "market":market["market"],
        "sub_zone": market['sub_zone'],
        "region": market['region'],
        "market_id": market['market_id'],
        "customers": market_customers,
    }
    market_documents.append(market_doc)


# #-------------------------Forecast and Sales Docs
forecast_market_array=[]
sales_market_array=[]

for customer in customer_documents:
    for forecast in forecast_all:
        if(customer["customer_id"]==forecast["customer_id"]):
            forecast_doc={
            "_id": forecast["_id"],
            "date":forecast["date"],
            "customer_name":forecast["customer_name"],
            "forecast_quantity":forecast["forecast_quantity"],
            "customer_id":forecast["customer_id"],
            "product_id":forecast["product_id"],
            "forecast_id":forecast["forecast_id"],
            "parent":customer["_id"],
            }
            forecast_market_array.append(forecast_doc)

for customer in customer_documents:
    for sales in sales_all:
        if(customer["customer_id"]==sales["customer_id"]):
            sales_doc={
            "_id": sales["_id"],
            "date":sales["date"],
            "customer_name":sales["customer_name"],
            "sold_quantity":sales["sold_quantity"],
            "customer_id":sales["customer_id"],
            "product_id":sales["product_id"],
            "sales_id":sales["sales_id"],
            "parent":customer["_id"],
            }
            sales_market_array.append(sales_doc)

#Forecast Doc containing all forecast records
forecast_market_doc={
    "_id":ObjectId(),
    "market":forecast_market_array,
}
#Sales Doc containing all sales records
sales_market_doc={
    "_id":ObjectId(),
    "market":sales_market_array,
}

#Final documents for insertion into MongoDB database
mongo_market.insert_many(market_documents)
mongo_customers.insert_many(customer_documents)
mongo_forecast.insert_one(forecast_market_doc)
mongo_sales.insert_one(sales_market_doc)
