import os
from dotenv import load_dotenv
import mysql.connector
from pymongo import MongoClient
import redis

# Load .env
load_dotenv()

# --- Config từ ENV ---
MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3336,
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": "github_data"
}

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

REDIS_HOST = os.getenv("REDIS_HOST")
REDIS_PORT = int(os.getenv("REDIS_PORT"))

# --- Dữ liệu mẫu ---
sample_data = {
    "user_id": 123456,
    "login": "test_user",
    "gravatar_id": "",
    "url": "https://api.github.com/users/test_user",
    "avatar_url": "https://avatars.githubusercontent.com/u/123456?v=4"
}

# --- MySQL ---
try:
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = mysql_conn.cursor()
    insert_query = """
        INSERT INTO Users (user_id, login, gravatar_id, url, avatar_url)
        VALUES (%s, %s, %s, %s, %s)
    """
    cursor.execute(insert_query, (
        sample_data["user_id"],
        sample_data["login"],
        sample_data["gravatar_id"],
        sample_data["url"],
        sample_data["avatar_url"]
    ))
    mysql_conn.commit()
    print("✅ Inserted into MySQL")
except Exception as e:
    print("❌ MySQL Error:", e)
finally:
    if cursor:
        cursor.close()
    if mysql_conn:
        mysql_conn.close()

# --- MongoDB ---
try:
    mongo_client = MongoClient(MONGO_URI)
    db = mongo_client[MONGO_DB]
    db.Users.insert_one(sample_data)
    print("✅ Inserted into MongoDB")
except Exception as e:
    print("❌ MongoDB Error:", e)
finally:
    mongo_client.close()

# --- Redis ---
try:
    r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    key = f"user:{sample_data['user_id']}"
    r.hmset(key, sample_data)
    print("✅ Inserted into Redis")
except Exception as e:
    print("❌ Redis Error:", e)
