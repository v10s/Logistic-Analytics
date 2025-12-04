import yaml
from server.connector.postgres import PostgresConnector
import pandas as pd
import dask.dataframe as dd
import matplotlib.pyplot as plt
import numpy as np
from base_pipeline import BaseETL

class NewYorkTaxiETL(BaseETL):

    def __init__(self):
        super().__init__()
        self.df = pd.read_parquet('./.seed_data/yellow_tripdata_2025-01.parquet', engine='pyarrow')


    def run(self):
        super().run()
        self.fastest_vendor()
        self.vendor_expense()
        self.vendor_rides()
        self.vendor_fares()

    def fastest_vendor(self):
        self.df['time_taken'] = self.df['tpep_dropoff_datetime'] - self.df['tpep_pickup_datetime']

        self.df['speed'] = self.df['trip_distance'] / self.df['time_taken'].dt.seconds
        self.df.replace([np.inf, -np.inf], np.nan, inplace=True)
        self.df.dropna(inplace=True)
        self.df.head()
        self.df.sort_values(by='speed', ascending=False)
        self.df['VendorID'] = self.df['VendorID'].astype('str')

        fastest_vendor = self.df.groupby(by='VendorID')['speed'].mean().reset_index()
        self.p_connect.publish_df(df=fastest_vendor,table_name='vendor_by_speed')

    def vendor_expense(self):

        self.df['expense_per_unit'] = self.df['fare_amount'] / self.df['trip_distance']

        self.df.replace([np.inf, -np.inf], inplace=True)
        self.df.dropna(inplace=True)

        vendor_by_expense = self.df.groupby('VendorID')['expense_per_unit'].mean().reset_index()
        vendor_by_expense.sort_values(by='expense_per_unit', ascending=False)
        vendor_by_expense.head()
        self.p_connect.publish_df(df=vendor_by_expense,table_name='vendor_by_expense')


    def vendor_rides(self):

        self.df['pickup_date'] =  pd.to_datetime(self.df['tpep_pickup_datetime']).dt.date


        rides_by_vendor = self.df.groupby(by=['VendorID', 'pickup_date']).size().reset_index()
        self.p_connect.publish_df(df=rides_by_vendor,table_name='vendor_by_rides')

    def vendor_fares(self):

        self.df['pickup_date'] =  pd.to_datetime(self.df['tpep_pickup_datetime']).dt.date

        fare_by_vendor = self.df.groupby(by=['VendorID', 'pickup_date']).agg({'fare_amount':['sum']}).reset_index()
        fare_by_vendor.columns = [_+__ for _,__ in fare_by_vendor.columns]

        self.p_connect.publish_df(df=fare_by_vendor,table_name='fare_by_vendor')
