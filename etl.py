import requests
import pandas as pd
import urllib.parse as url
from login_data import *
from sqlalchemy import create_engine

def connect_to_db():
    return create_engine(f"mysql://{user}:{url.quote_plus(password)}@{host}/{database}?charset=utf8")

def df_to_sql(df,table_name):
    df.to_sql(table_name, con=connect_to_db(),index=False, if_exists="replace")
    print(f"{table_name} loaded successfully!")

# Extract the data (JSON format) from randomuser.me api service  
data = requests.get("https://randomuser.me/api/?results=4500&noinfo&fmt=prettyjson").json()["results"]

# Normlize the JSON data and load it to pandas dataframe
df = pd.json_normalize(data)

# Split the dataframe to 2 datasets based on the user gender
Noam_test_male, Noam_test_female = df[df["gender"]=="male"], df[df["gender"]=="female"]

# Split the dataframe to 10 datasets based on the user age by decade
# add decade column 
age_range = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
age_range_names = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
df["dob.age.decade"] = pd.cut(df["dob.age"].values, bins=age_range, labels=age_range_names)
# Split the dataframe
df_subsets_by_decade = {}
for subset in age_range_names:
    df_to_load = df[df["dob.age.decade"]==subset].copy()
    df_to_load.drop(columns=["dob.age.decade"],inplace=True)
    df_subsets_by_decade.update({'Noam_test_' + str(subset) : df_to_load.reset_index(drop=True)})

# Load male/female tables to DB
df_to_sql(Noam_test_female,"Noam_test_female")
df_to_sql(Noam_test_male,"Noam_test_male")

# Load splitted dataframes to DB
for dataframe_name, dataframe_data in df_subsets_by_decade.items():
    df_to_sql(dataframe_data,dataframe_name)

# Create a dataset contains the top 20 last registered from both male & female tables
Noam_test_20 = pd.read_sql_query('''
(select *
from interview.Noam_test_male
order by `registered.date` desc
limit 20)
union all
(select *
from interview.Noam_test_female
order by `registered.date` desc 
limit 20)
''',con=connect_to_db())
# Load the combined table to DB
df_to_sql(Noam_test_20,"Noam_test_20")

# Create a dataset combines data from Noam_test_5 table and _20 table - without duplicates
# Write the new dataframe to a json file
Noam_test_5 = pd.read_sql_query('''
select * from interview.Noam_test_5
''',con=connect_to_db())
Noam_test_5_plus_20 = pd.concat([Noam_test_20,Noam_test_5]).drop_duplicates().reset_index().to_json("first.json",orient="records")

# Create a dataset combines data from Noam_test_2 table and _20 table - duplicates ignored
# Write the new dataframe to a json file
Noam_test_2 = pd.read_sql_query('''
select * from interview.Noam_test_2
''',con=connect_to_db())
Noam_test_2_plus_20 = pd.concat([Noam_test_20,Noam_test_2]).reset_index().to_json("second.json",orient="records")