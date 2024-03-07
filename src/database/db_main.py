from PostgresDB import PostgresDB

# init an instance
db = PostgresDB()
# create schema
db.executeScriptsFromFile('createSchema.sql')
# insert records
# db.insertCSVIntoTable('../../data/products.csv', 'products')
# db.insertCSVIntoTable('../../data/users.csv', 'users')
