from config import connect, engine
from sklearn import linear_model
from sqlalchemy import create_engine

from psycopg2 import sql
import pandas as pd
import psycopg2

engine = engine()

class Prediction:
    def getResource(self):
        result = 0
        fetchdata = []
        try:
            conn = connect()
            cur = conn.cursor()
            hybrid = []
            inbrid = []
            lowland = []
            upland = []
            crop = 1

            stmt = sql.SQL("""DELETE FROM public.predicted WHERE index >= 0;""")
            cur.execute(stmt)
            conn.commit()

            municipality = ['Banaybanay','Lupon','Gov. Gen.','Baganga','Boston','Caraga','Cateel','Manay','Mati']

            while crop <= 2:
                for mun in  municipality:
                    stmt = sql.SQL(""" SELECT hybrid FROM public.resources WHERE crop_batch = '{crop}'
                                    AND municipality = '{muni}' 
                                ;""".format(muni = mun,crop = crop))

                    cur.execute(stmt)
                    conn.commit()
                    fetchdata = cur.fetchall()
                    result = cur.rowcount
                    
                    if result:
                        #print('1')
                        hybrid.append(self.movingAverage(fetchdata))

                        stmt1 = sql.SQL(""" SELECT inbrid FROM public.resources WHERE crop_batch = '{crop}'
                                    AND municipality = '{muni}' 
                                ;""".format(muni = mun,crop = crop))

                        cur.execute(stmt1)
                        conn.commit()
                        fetchdata = cur.fetchall()
                        result1 = cur.rowcount

                        if result1:
                            inbrid.append(self.movingAverage(fetchdata))
                            stmt2 = sql.SQL(""" SELECT lowland FROM public.resources WHERE crop_batch = '{crop}'
                                    AND municipality = '{muni}' 
                                ;""".format(muni = mun,crop = crop))

                            cur.execute(stmt2)
                            conn.commit()
                            fetchdata = cur.fetchall()
                            result2 = cur.rowcount
                            if result2:
                                lowland.append(self.movingAverage(fetchdata))
                                stmt3 = sql.SQL(""" SELECT upland FROM public.resources WHERE crop_batch = '{crop}'
                                    AND municipality = '{muni}' 
                                ;""".format(muni = mun,crop = crop))

                                cur.execute(stmt3)
                                conn.commit()
                                fetchdata = cur.fetchall()
                                result3 = cur.rowcount
                                if result3:
                                    upland.append(self.movingAverage(fetchdata))

                self.postPredictedTable(hybrid,inbrid,lowland,upland,crop)
                hybrid = []
                inbrid = []
                lowland = []
                upland = []
                crop += 1        
                            
        except psycopg2.DatabaseError as error:
            print(error)
            return error


    def postPredictedTable(self,hybrid,inbrid,lowland,upland,crop):
        conn = connect()
        cur = conn.cursor()
    
        try:

            stmt2 = sql.SQL("""SELECT year FROM public.production
                        WHERE crop_batch = '1';""")
            cur.execute(stmt2)
            conn.commit()
            fetchdata = cur.fetchall()
            result0 = cur.rowcount
        
            if result0:
            
                list = []
                for fetch in fetchdata:
                    for f in fetch:
                        list.append(f)

                ma_x = int(max(list))

                m = ma_x+5
                year = []
                while ma_x < m:
                    ma_x = ma_x+1
                    year.append(ma_x)

    
            if crop == 1:
                temperature = 31.42
            elif crop == 2:
                temperature = 27.73


            municipality = ['Banaybanay','Lupon','Gov. Gen.','Baganga','Boston','Caraga','Cateel','Manay','Mati']
            index = 0

            while index < len(municipality):
                indey = 0                    

                while indey < len(hybrid[0]):
                    stmt2 = sql.SQL('''INSERT INTO public.predicted(year,crop_batch, municipality, 
                                                                    temperature, hybrid,inbrid,
                                                                    lowland,upland)

                                        VALUES ('{year}','{crop_batch}','{municipality}','{temperature}',
                                                '{hybrid}','{inbrid}','{lowland}','{upland}'
                                                
                                    )''' .format(year = year[indey],crop_batch = crop,municipality = municipality[index],
                                                temperature = temperature,hybrid = hybrid[index][indey],
                                                inbrid = inbrid[index][indey], lowland = lowland[index][indey],
                                                upland = upland[index][indey]))
                    cur.execute(stmt2)
                    conn.commit()
                    result1 = cur.rowcount
                    indey += 1
                index += 1
            
            if result1:
                self.multipleRegression()

            cur.close()
             
        except psycopg2.DatabaseError as error:
            print(error)
            return error 
    

    def movingAverage(self,data):
        arr = []

        for fromList in data:
            for fromTuple in fromList:
                arr.append(fromTuple)
        #print(arr)

        window_size = 6
        # Convert array of integers to pandas series
        numbers_series = pd.Series(arr)
        
        # Get the window of series
        # of observations of specified window size
        windows = numbers_series.rolling(window_size)

        # Create a series of moving
        # averages of each window
        moving_averages = windows.mean()
        
        # Convert pandas series back to list
        moving_averages_list = moving_averages.tolist()
        
        # Remove null entries from the list
        final_list = moving_averages_list[window_size - 1:]

        newlist= []
        for x in final_list:
            newlist.append(round(x,1))

        return newlist


    def multipleRegression(self):
        connection = connect()
        cur = connection.cursor()

        df = pd.read_sql("""SELECT hybrid,inbrid,lowland,upland,temperature,production_mt FROM public.production
                   WHERE production_id > 0""",connection)
        
        reg = linear_model.LinearRegression()
        reg.fit(df.drop('production_mt',axis='columns'),df.production_mt)

        reg.coef_
        reg.intercept_

        
        df1 = pd.read_sql("""SELECT year,crop_batch,municipality,hybrid,inbrid,lowland,upland,temperature,production_mt FROM public.predicted
                             WHERE index > 0""",connection)
        
        index = 0
        predicted = []
        predictedToDF = []
        while(index < len(df1)):
            val1 = df1['hybrid'].values[index]
            val2 = df1['inbrid'].values[index]
            val3 = df1['lowland'].values[index]
            val4 = df1['upland'].values[index]
            val5 = df1['temperature'].values[index]
            predicted.append(reg.predict([[val1, val2, val3, val4,val5]]))
            predictedToDF.append(predicted[index][0].round())
            index+=1
        
        df1['production_mt'] = predictedToDF


        stmt = sql.SQL("""DELETE FROM public.predicted WHERE index >= 0;""")
        cur.execute(stmt)
        connection.commit()
        result1 = cur.rowcount
        cur.close()

        engine = create_engine('postgresql://postgres:postgres123@localhost:5432/postgres')
        df1.to_sql(name='predicted',con=engine,if_exists='append')
    
    def mapping(self,year,crop):

        try:
            df = pd.read_sql(('SELECT index,municipality,production_mt FROM public.predicted '
                                'WHERE year = %(year)s AND crop_batch = %(crop)s'),
                            engine,params={"year":'%s'%year,'crop':'%s'%crop})

            return df

        except Exception as e:
            return e
    
    def newDataFrameForMap(self,df):
        x = 0
        values = []
        banaybanay = 0.0
        manay = 0.0
        mati = 0.0
        lupon = 0.0
        govgen = 0.0
        baganga = 0.0
        boston = 0.0
        caraga = 0.0
        cateel = 0.0

        while x < len(df.index):
            if df.loc[x,'municipality'] == 'Banaybanay':
                banaybanay+=df.loc[x,'production_mt']
            if df.loc[x,'municipality'] == 'Manay':
                manay+=df.loc[x,'production_mt']
            elif df.loc[x,'municipality'] == 'Mati':
                mati+=df.loc[x,'production_mt']
            elif df.loc[x,'municipality'] == 'Lupon':
                lupon+=df.loc[x,'production_mt']
            elif df.loc[x,'municipality'] == 'Gov. Gen.':
                govgen+=df.loc[x,'production_mt']
            elif df.loc[x,'municipality'] == 'Baganga':
                baganga+=df.loc[x,'production_mt']
            elif df.loc[x,'municipality'] == 'Boston':
                boston+=df.loc[x,'production_mt']
            elif df.loc[x,'municipality'] == 'Caraga':
                caraga+=df.loc[x,'production_mt']
            elif df.loc[x,'municipality'] == 'Cateel':
                cateel+=df.loc[x,'production_mt']
            
            x+=1
            
        value = pd.Series([banaybanay,lupon,govgen,mati,manay,caraga,baganga,cateel,boston])
        iD = pd.Series([1,2,4,5,7,8,9,10,11])
        df_new = pd.DataFrame({'values': value, 'iDs': iD})

        return df_new


       
    


    


