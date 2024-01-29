import pyarrow.parquet as pq 
import pandas as pd 
import numpy as np 
from sqlalchemy import create_engine

#using Pyarrow to pull and read the parquet table
#trips = pq.read_table('/home/jknox86/github/dataTalks/yellow_tripdata_2023-01.parquet')

trips = pd.read_csv(f'/home/jknox86/github/dataTalks/green_tripdata_2019-09.csv')

trips.lpep_pickup_datetime = pd.to_datetime(trips.lpep_pickup_datetime)
trips.lpep_dropoff_datetime = pd.to_datetime(trips.lpep_dropoff_datetime)


#using Pandas to take the trips table from Pyarrow and push into a pandas dataframe
#trips = trips.to_pandas()

#Create the engine connection using sqlalchemy library
engine = create_engine('postgresql://jknox:RedBarn23!@localhost:5432/dataTalks')
engine.connect()

#Using this to create the table and datatypes based on the taxi data that was in the parquet file
print(pd.io.sql.get_schema(trips, name='green_taxi_data', con=engine))

#Get only the schema pushed in from DF head
trips.head(n=0).to_sql(name='green_taxi_data', con=engine, if_exists='replace')

chunk_size = 100000
chunks = [trips[i:i + chunk_size] for i in range(0, trips.shape[0], chunk_size)]

for chunk in chunks:
    chunk.to_sql('green_taxi_data', con=engine, if_exists='append', index=False)




