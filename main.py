import pandas as pd

file = pd.read_csv("data/users.csv")
print(file)

# adding header
headerList = ['user_id', 'name', 'email', 'gender',	'dob', 'country', 'phone_number', 'registration_date', 'daily_time_spend']


# converting data frame to csv
file.to_csv("data/users.csv", header=headerList, index=False)
