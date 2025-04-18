from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from Config.database_config import get_database_config
from database.schema_mannager import create_mongodb_schema

# step1: def(get mongodb config)
# step2: def(connect)
# step3: def(disconnect)
# step4: def(reconnect)
# step5: def(exit)

class MongoDBConnect:
# step1:
    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.client = None
        self.db = None
    def connect(self):
        try:
            self.client = MongoClient(self.mongo_uri)
            self.client.server_info() # test connection
            self.db = self.client[self.db_name]
            print( self.client.server_info())
            print(f"-------connected to MtongoDB: {self.db_name}--------")
            return self.db
        except ConnectionFailure as e:
            raise  Exception(f"------Fail to connect MongoDB: {e}-------") from e

    def close(self):
        if self.client:
            self.client.close()
        print("------MongoDB connection closed------")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def main():
    configMongo = get_database_config()
    with MongoDBConnect(configMongo["mongodb"].uri, configMongo["mongodb"].db_name) as mongo_client:
        create_mongodb_schema(mongo_client.connect())
        mongo_client.db.Users.insert_one({
            "user_id": 1,
            "login": "GooglLeCodeExporter",
            "gravatar_id": "",
            "avatar_url": "https://avatars.githubusercontent.com/u/9614759?",
            "url": "https://api.github.com/users/GooglLeCodeExporter"
        })
        print("-----Inserted to MongoDB-----")
if __name__ == "__main__":
    main()