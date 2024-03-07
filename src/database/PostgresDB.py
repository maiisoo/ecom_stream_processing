import csv

import psycopg2


class PostgresDB:
    def __init__(self):
        db_credentials = {
            'host': 'localhost',
            'user': 'user',
            'password': 'mypassword',
            'port': '5432'
        }
        try:
            conn = psycopg2.connect(**db_credentials)
            print("Connected to DB successfully.")
            self.cursor = conn.cursor()
        except psycopg2.Error as error:
            print("Error connecting to the database:", error)

    def executeScriptsFromFile(self, filename):
        # Open and read the file as a single buffer
        fd = open(filename, 'r')
        sqlFile = fd.read()
        fd.close()
        # all SQL commands (split on ';')
        sqlCommands = sqlFile.split(';')
        # Execute every command from the input file
        for command in sqlCommands:
            try:
                command.strip()
                if command:
                    command = command + ';'
                    self.cursor.execute(command)
                    print("Command executed.")

            except psycopg2.Error as error:
                print("Command skipped: ", error)

    def insertCSVIntoTable(self, filepath, table_name):
        with open(filepath) as file_obj:
            # Skips the heading
            heading = next(file_obj)
            reader_obj = csv.reader(file_obj)
            # Iterate over each row in the csv file
            for row in reader_obj:
                row = ', '.join([f"'{value}'" for value in row])
                command = f"INSERT INTO {table_name} VALUES ({row});"
                self.cursor.execute(command)

# init an instance
db = PostgresDB()
# create schema
db.executeScriptsFromFile('createSchema.sql')
# insert records
db.insertCSVIntoTable('../../data/products.csv', 'products')
db.insertCSVIntoTable('../../data/users.csv', 'users')

