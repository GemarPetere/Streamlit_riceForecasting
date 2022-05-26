from config import connect, engine
from psycopg2 import sql
import pandas as pd
import psycopg2

connection = connect()
cur = connection.cursor()
engine = engine()

class VisualizeData:

    

    def predictedData(self,year):
        
        
        df = pd.read_sql(('SELECT municipality,crop_batch,production_mt FROM public.predicted '
                          'WHERE year = %(year)s'),
                        engine,params={"year":year})
        return df



    def predictedMuni(self,year,crop):
        total = 0.0

        df = pd.read_sql(('SELECT municipality,production_mt FROM public.predicted '
                          'WHERE year = %(year)s AND crop_batch = %(crop_batch)s'),
                        engine,params={"year":year,"crop_batch":crop})

        #This loop intended for process to get the total production
        for tobeTotal in df['production_mt']:
            total+=tobeTotal

        return df, total


    def selectMunicipality(self):

        try:
            stmt3 = sql.SQL(""" SELECT DISTINCT municipality FROM public.predicted ORDER BY municipality;""")

            cur.execute(stmt3)
            connection.commit()
            fetchdata = cur.fetchall()
            result3 = cur.rowcount
    
            return fetchdata
        except psycopg2.DatabaseError as error:
            return error

    def selectYear(self):
        try:
            stmt3 = sql.SQL(""" SELECT DISTINCT year FROM public.predicted ORDER BY year;""")

            cur.execute(stmt3)
            connection.commit()
            fetchdata = cur.fetchall()
            result3 = cur.rowcount
    
            return fetchdata
        except psycopg2.DatabaseError as error:
            return error

    
    def queryFactor_hybrid(self,crop,muni):

        try:
            df = pd.read_sql(('SELECT year,crop_season,municipality,hybrid FROM public.resources '
                          'WHERE crop_season = %(crop_season)s AND municipality = %(muni)s'),
                        engine,params={"muni":muni,"crop_season":crop})
            return df

        except psycopg2.DatabaseError as error:
            return error

    def queryFactor_inbrid(self,crop,muni):

        try:
            df = pd.read_sql(('SELECT year,crop_season,municipality,inbrid FROM public.resources '
                          'WHERE crop_season = %(crop_season)s AND municipality = %(muni)s'),
                        engine,params={"muni":muni,"crop_season":crop})
            return df

        except psycopg2.DatabaseError as error:
            return error


    def queryFactor_upland(self,crop,muni):

        try:
            df = pd.read_sql(('SELECT year,crop_season,municipality,upland FROM public.resources '
                          'WHERE crop_season = %(crop_season)s AND municipality = %(muni)s'),
                        engine,params={"muni":muni,"crop_season":crop})
            return df

        except psycopg2.DatabaseError as error:
            return error
    
    def queryFactor_lowland(self,crop,muni):

        try:
            df = pd.read_sql(('SELECT year,crop_season,municipality,lowland FROM public.resources '
                          'WHERE crop_season = %(crop_season)s AND municipality = %(muni)s'),
                        engine,params={"muni":muni,"crop_season":crop})
            return df

        except psycopg2.DatabaseError as error:
            return error

    def selectYearTable(self):
        try:
            stmt3 = sql.SQL(""" SELECT DISTINCT year FROM public.production ORDER BY year;""")

            cur.execute(stmt3)
            connection.commit()
            fetchdata = cur.fetchall()
            result3 = cur.rowcount
    
            return fetchdata
        except psycopg2.DatabaseError as error:
            return error

            

    def table(self,year):

        try:
            df = pd.read_sql(('SELECT year,municipality,crop_season,farmer,production_rate,area_harvested FROM public.production '
                          'WHERE year = %(year)s'),
                        engine,params={"year":year})
            return df

        except psycopg2.DatabaseError as error:
            return error
    
    def lineGraphOne(self,municipal):

        try:
            dfActual = pd.read_sql(('SELECT year,production_mt FROM public.resources '
                                'WHERE municipality = %(muni)s AND crop_batch = %(crop)s'),
                            engine,params={"muni":'%s'%municipal,'crop':'1'})


            dfActual['label'] = 'Actual'
            dfActual = dfActual.sort_values(by='year', ascending=True)

            if len(dfActual.index > 0):
                dfPredict = pd.read_sql(('SELECT year,production_mt FROM public.predicted '
                                'WHERE municipality = %(muni)s AND crop_batch = %(crop)s'),
                            engine,params={"muni":'%s'%municipal,'crop':'1'})


                dfPredict['label'] = 'Forecasted'

                dfActual = dfActual.append(dfPredict.iloc[0], ignore_index=True)
                dfActual.at[10,'label'] = 'Actual'

                dfAppended = dfActual.append(dfPredict)

                return dfAppended

        except psycopg2.DatabaseError as error:
            return error
        

    def lineGraphTwo(self,municipal):
        try:
            dfActual = pd.read_sql(('SELECT year,production_mt FROM public.resources '
                                'WHERE municipality = %(muni)s AND crop_batch = %(crop)s'),
                            engine,params={"muni":'%s'%municipal,'crop':'2'})


            dfActual['label'] = 'Actual'
            dfActual = dfActual.sort_values(by='year', ascending=True)

            if len(dfActual.index > 0):
                dfPredict = pd.read_sql(('SELECT year,production_mt FROM public.predicted '
                                'WHERE municipality = %(muni)s AND crop_batch = %(crop)s'),
                            engine,params={"muni":'%s'%municipal,'crop':'2'})


                dfPredict['label'] = 'Forecasted'

                dfActual = dfActual.append(dfPredict.iloc[0], ignore_index=True)
                dfActual.at[10,'label'] = 'Actual'

                dfAppended2 = dfActual.append(dfPredict)

                return dfAppended2

        except psycopg2.DatabaseError as error:
            return error
    

