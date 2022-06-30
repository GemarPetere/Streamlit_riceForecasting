from config import connect, engine
from psycopg2 import sql
import pandas as pd
import psycopg2
import numpy as np
from sklearn import linear_model

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
        
        total = np.sum(df.loc[:,'production_mt':].values)
        df['percent'] = df.loc[:,'production_mt':].sum(axis=1)/total * 100

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
        dfAppended = None
        dfFinal = []

        try:
            dfActual = pd.read_sql(('SELECT year,hybrid,inbrid,lowland,upland,temperature,production_mt FROM public.production '
                                        'WHERE municipality = %(muni)s AND crop_batch = %(crop)s'),
                                    engine,params={"muni":'%s'%municipal,'crop':'1'})

            dfActual['Data Crop 1'] = '0' 
            dfActual = dfActual.sort_values(by='year', ascending=True)

            reg = linear_model.LinearRegression()
            reg.fit(dfActual.drop('production_mt',axis='columns'),dfActual.production_mt)
            reg.coef_
            reg.intercept_

            x = 2
            while(x < 10):
                dfFiltered = dfActual.head(x)

                if x >= 2:
                    year = dfActual['year'].iloc[x]
                    hybrid = dfActual['hybrid'].iloc[x]
                    inbrid = dfActual['inbrid'].iloc[x]
                    lowland = dfActual['lowland'].iloc[x]
                    upland = dfActual['upland'].iloc[x]
                    temperature = dfActual['temperature'].iloc[x]

                    forecasted = reg.predict([[int(year), float(hybrid), float(inbrid),float(lowland),float(upland),float(temperature),0]])

                    actualX = [year,hybrid,inbrid,lowland,upland,temperature,forecasted[0],1]

                    print(actualX)

                    dfFinal.append(actualX)
                x+=1

            dfActuals = pd.DataFrame(dfFinal, columns = ["year","hybrid","inbrid","lowland","upland","temperature","production_mt","Data Crop 1"])
            dFinal = dfActual.append(dfActuals)
            dFinal.sort_values(by=['year'],inplace=True)


            #predicted value processing and appending to actual
            dfPredict = pd.read_sql(('SELECT year,hybrid,inbrid,lowland,upland,temperature,production_mt FROM public.predicted '
                                        'WHERE municipality = %(muni)s AND crop_batch = %(crop)s'),
                                    engine,params={"muni":'%s'%municipal,'crop':'1'})
            dfPredict['Data Crop 1'] = 1

          
                
            dFinal = dFinal.append(dfPredict.iloc[0], ignore_index=True)
            dfAppended = dFinal.append(dfPredict)
            
            y=0
            while(y < len(dfAppended.index)):
                if dfAppended['Data Crop 1'].iloc[y] == '0':
                    dfAppended['Data Crop 1'].iloc[y] = 'Actual'
                else:
                    dfAppended['Data Crop 1'].iloc[y] = 'Forecasted'
                y+=1
            
            dfTable1 = self.toTableActualForecast1(dfAppended)
            return dfAppended, dfTable1

        except psycopg2.DatabaseError as error:
            return error
        

    def lineGraphTwo(self,municipal):
        dfAppended = None
        dfFinal = []

        try:
            dfActual = pd.read_sql(('SELECT year,hybrid,inbrid,lowland,upland,temperature,production_mt FROM public.production '
                                        'WHERE municipality = %(muni)s AND crop_batch = %(crop)s'),
                                    engine,params={"muni":'%s'%municipal,'crop':'2'})

            dfActual['Data Crop 2'] = '0' 
            dfActual = dfActual.sort_values(by='year', ascending=True)

            reg = linear_model.LinearRegression()
            reg.fit(dfActual.drop('production_mt',axis='columns'),dfActual.production_mt)
            reg.coef_
            reg.intercept_

            x = 2
            while(x < 10):
                dfFiltered = dfActual.head(x)

                if x >= 2:
                    year = dfActual['year'].iloc[x]
                    hybrid = dfActual['hybrid'].iloc[x]
                    inbrid = dfActual['inbrid'].iloc[x]
                    lowland = dfActual['lowland'].iloc[x]
                    upland = dfActual['upland'].iloc[x]
                    temperature = dfActual['temperature'].iloc[x]

                    forecasted = reg.predict([[int(year), float(hybrid), float(inbrid),float(lowland),float(upland),float(temperature),0]])

                    actualX = [year,hybrid,inbrid,lowland,upland,temperature,forecasted[0],1]

                    dfFinal.append(actualX)
                x+=1

            dfActuals = pd.DataFrame(dfFinal, columns = ["year","hybrid","inbrid","lowland","upland","temperature","production_mt","Data Crop 2"])
            dFinal = dfActual.append(dfActuals)
            dFinal.sort_values(by=['year'],inplace=True)


            #predicted value processing and appending to actual
            dfPredict = pd.read_sql(('SELECT year,hybrid,inbrid,lowland,upland,temperature,production_mt FROM public.predicted '
                                        'WHERE municipality = %(muni)s AND crop_batch = %(crop)s'),
                                    engine,params={"muni":'%s'%municipal,'crop':'2'})
            dfPredict['Data Crop 2'] = 1

          
                
            dFinal = dFinal.append(dfPredict.iloc[0], ignore_index=True)
            dfAppended = dFinal.append(dfPredict)
            
            y=0
            while(y < len(dfAppended.index)):
                if dfAppended['Data Crop 2'].iloc[y] == '0':
                    dfAppended['Data Crop 2'].iloc[y] = 'Actual'
                else:
                    dfAppended['Data Crop 2'].iloc[y] = 'Forecasted'
                y+=1
            dfTable2 = self.toTableActualForecast2(dfAppended)
            return dfAppended,dfTable2

        except psycopg2.DatabaseError as error:
            return error
    
    def toTableActualForecast2(self,data):
        try:
            data = data.drop(['hybrid','inbrid','lowland','upland','temperature'], axis=1)
            newData = data.pivot_table('production_mt',['year'],'Data Crop 2')
            
            newData['Actual'] = newData['Actual'].replace(np.nan,0)
            newData['Forecasted'] = newData['Forecasted'].replace(np.nan,0)
            
            return newData

        except Exception as e:
            print(e)
    
    def toTableActualForecast1(self,data):
        try:
            data = data.drop(['hybrid','inbrid','lowland','upland','temperature'], axis=1)
            newData = data.pivot_table('production_mt',['year'],'Data Crop 1')
            
            newData['Actual'] = newData['Actual'].replace(np.nan,0)
            newData['Forecasted'] = newData['Forecasted'].replace(np.nan,0)
            
            return newData

        except Exception as e:
            print(e)

