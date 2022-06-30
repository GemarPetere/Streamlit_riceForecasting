from config import connect, engine
from sklearn import linear_model
from sqlalchemy import create_engine

from psycopg2 import sql
import pandas as pd
import psycopg2

engine = engine()

class Report:

     def convert_df(self,df):
        # IMPORTANT: Cache the conversion to prevent computation on every rerun
        return df.to_csv().encode('utf-8')

     def getPredictedData(self,year):

        try:
            df = pd.read_sql(('SELECT * FROM public.production '),
                            engine)
            return df
        except psycopg2.DatabaseError as error:
            return error

