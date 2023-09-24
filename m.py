
import pandas as pd
import psycopg2
import pymongo
from config import db_config
# Excel File Reader
class ExcelFileReader:
    def read_excel(self, file_path):
        try:
            data = pd.read_excel(file_path, skiprows=1)  # Skip the first row
            first_row = data.iloc[0]
            column_names = [col.replace('/', '_').replace('№', 'No') for col in first_row]
        
            data.columns = column_names
            return data
            
        except Exception as e:
            print(f"Error reading Excel file: {str(e)}")
            return None


# Data Processor
class DataProcessor:
    def process_data(self, data):
        # Process data (filter taxpayers based on criteria)
        if data is None:
            return None
        # Example processing: Filtering out taxpayers with certain criteria
        processed_data = data  #[data['criteria_column'] == 'some_criteria']
        return processed_data

# Database Connection Manager (Singleton)
class DatabaseConnectionManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnectionManager, cls).__new__(cls)
            # Create and manage database connections (MongoDB and Postgres)
            cls._instance.mongodb_connection = cls._instance._create_mongodb_connection()
            cls._instance.postgres_connection = cls._instance._create_postgres_connection()
        return cls._instance

    def _create_mongodb_connection(self):
        # Implement logic to create and return a MongoDB connection
        try:
            client = pymongo.MongoClient("mongodb://localhost:27017/")  # Update with your MongoDB connection details
            db = client["mongodb"]  
            return db
        except Exception as e:
            print(f"Error creating MongoDB connection: {str(e)}")
            return None

    def _create_postgres_connection(self):
        # Implement logic to create and return a Postgres connection
        try:
            conn = psycopg2.connect(**db_config)  # Update with your Postgres connection details
            return conn
        except Exception as e:
            print(f"Error creating Postgres connection: {str(e)}")
            return None

    def get_mongodb_connection(self):
        return self.mongodb_connection

    def get_postgres_connection(self):
        return self.postgres_connection

# MongoDB Database Component (Repository)
class MongoDBRepository:
    def __init__(self):
        self.connection_string = "mongodb://localhost:27017/"
        self.database_name = "mongodb"
        # self.collection_name = collection_name
        self.db = None

    def connect(self):
        try:
            client = pymongo.MongoClient(self.connection_string)
            self.db = client[self.database_name]
        except Exception as e:
            print(f"Error connecting to MongoDB: {str(e)}")

    def insert_data(self, data, collection_name):
        if self.db is None:
            self.connect()

        if data is None:
            return

        try:
            # first_row = data.iloc[0]
            # column_names = [col.replace('/', '_').replace('№', 'No') for col in first_row]
        
            # data.columns = column_names
            # Assuming data is a DataFrame, you can convert it to a list of dictionaries
            data_dict_list = data.to_dict(orient='records')
            self.db[collection_name].insert_many(data_dict_list)
            print(f"Data inserted into MongoDB collection '{collection_name}'")
        except Exception as e:
            print(f"Error inserting data into MongoDB: {str(e)}")

    def process_data(self, data):
        if data is None:
            return None
        # Example processing: Filtering out taxpayers with certain criteria
        processed_data = data[data['criteria_column'] == 'some_criteria']
        return processed_data



# Postgres Database Component (Repository)
class PostgresRepository:
    def __init__(self):
        # self.connection_string = connection_string
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(**db_config)
        except Exception as e:
            print(f"Error connecting to Postgres: {str(e)}")

    def insert_data(self, data, table_name):
        if self.conn is None:
            self.connect()

        if data is None:
            return

        try:
            cursor = self.conn.cursor()
            
            # Get column names from the first row of data
            # first_row = data.iloc[0]
            # column_names = [col.replace('/', '_').replace('№', 'No') for col in first_row]

            # Generate placeholders based on the number of columns
            placeholders = ', '.join(['%s'] * len(data.columns))

            # Enclose column names in double quotes
            quoted_column_names = ', '.join(f'"{col}"' for col in data.columns)
            # table_name = 'absent'
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ("
            for column_name in data.columns:
                create_table_sql += f'"{column_name}" TEXT,'  # Assume all columns are of type TEXT, you can adjust as needed
            create_table_sql = create_table_sql.rstrip(",") + ");"  # Remove the trailing comma and add the closing parenthesis
            cursor.execute(create_table_sql)

            # Construct the SQL INSERT statement
            insert_query = f'INSERT INTO {table_name} ({quoted_column_names}) VALUES ({placeholders});'

            data_tuples = [tuple(row) for row in data.to_records(index=False)]

            cursor.executemany(insert_query, data_tuples)
            self.conn.commit()
            print("Data inserted into Postgres table")
        except Exception as e:
            print(f"Error inserting data into Postgres: {str(e)}")

    def process_data(self, data):
        if data is None:
            return None

        # Example processing: Filtering out taxpayers with certain criteria
        processed_data = data[data['criteria_column'] == 'some_criteria']
        return processed_data
    
# Controller
class DataTransferController:
    def __init__(self, file_reader, data_processor, db_manager, db_mongodb, db_postgres):
        self.file_reader = file_reader
        self.data_processor = data_processor
        self.db_manager = db_manager
        self.db_mongodb = db_mongodb
        self.db_postgres = db_postgres

    def transfer_data(self, file_path, list_type):
        data = self.file_reader.read_excel(file_path)
        processed_data = self.data_processor.process_data(data)

        if list_type == 'AbsentLegalAddress':
            self.db_mongodb.insert_data(processed_data, 'absent')
            self.db_postgres.insert_data(processed_data, 'absent')
        elif list_type == 'DeclaredBankrupt':
            # Handle differently for declared bankrupt list
            self.db_mongodb.insert_data(processed_data, 'absent')
            self.db_postgres.insert_data(processed_data, 'bankrupt')
            pass
        elif list_type == 'InvalidRegistration':
            # Handle differently for invalid registration list
            self.db_mongodb.insert_data(processed_data, 'invalid_registration')
            self.db_postgres.insert_data(processed_data, 'invalid_registration')
            pass



# Instantiate and configure components
excel_reader = ExcelFileReader()
data_processor = DataProcessor()
db_manager = DatabaseConnectionManager()  # You need to implement the DB connection logic in DatabaseConnectionManager
db_mongodb = MongoDBRepository()
db_postgres = PostgresRepository()

# Instantiate the controller
controller = DataTransferController(excel_reader, data_processor, db_manager, db_mongodb, db_postgres)

# Trigger data transfer for each list type with appropriate file paths
controller.transfer_data('absent.xlsx', 'AbsentLegalAddress')
controller.transfer_data('bankrupt.xlsx', 'DeclaredBankrupt')
controller.transfer_data('invalid_registration.xlsx', 'InvalidRegistration')
