from config import connect, engine
from psycopg2 import sql
import pandas as pd
import psycopg2

connection = connect()
cur = connection.cursor()
engine = engine()

class ActualValue:

    
    def farmersGrowth(self,year):
        calculation = 0.0
        try:
            stmt3 = sql.SQL(""" SELECT year,farmer FROM public.production 
                                WHERE year = '{year}';
                            """.format(year = year))

            cur.execute(stmt3)
            connection.commit()
            fetchdata = cur.fetchall()
            result = cur.rowcount

            farmerCurrent = 0.0
            selectedYear = 0
            if result:
                for data in fetchdata:
                    farmerCurrent += data[1]
                    selectedYear = data[0]
            
            
            if farmerCurrent:
                prevYear = int(selectedYear)-1
                prev = sql.SQL(""" SELECT farmer FROM public.production 
                                    WHERE year = '{prevYear}';
                                """.format(prevYear = prevYear))

                cur.execute(prev)
                connection.commit()
                fetchPrev = cur.fetchall()
                resultPrev = cur.rowcount

                farmerPrev = 0.0
                if resultPrev:
                    for dataPrev in fetchPrev:
                        farmerPrev += float(dataPrev[0])
                
                calculation = round((farmerCurrent - farmerPrev) / farmerCurrent, 2)
                farmerValue = round(farmerCurrent - farmerPrev, 2)
                
            return calculation, farmerValue
        except psycopg2.DatabaseError as error:
            return error


    def productionGrowth(self,year):
        calculation = 0.0
        try:
            stmt3 = sql.SQL(""" SELECT year,production_mt FROM public.production 
                                WHERE year = '{year}';
                            """.format(year = year))

            cur.execute(stmt3)
            connection.commit()
            fetchdata = cur.fetchall()
            result = cur.rowcount

            farmerCurrent = 0.0
            selectedYear = 0
            if result:
                for data in fetchdata:
                    farmerCurrent += data[1]
                    selectedYear = data[0]
    
            
            if farmerCurrent:
                prevYear = int(selectedYear)-1
                prev = sql.SQL(""" SELECT production_mt FROM public.production 
                                    WHERE year = '{prevYear}';
                                """.format(prevYear = prevYear))

                cur.execute(prev)
                connection.commit()
                fetchPrev = cur.fetchall()
                resultPrev = cur.rowcount

                farmerPrev = 0.0
                if resultPrev:
                    for dataPrev in fetchPrev:
                        farmerPrev += float(dataPrev[0])
                
                calculation = round((farmerCurrent - farmerPrev) / farmerCurrent, 2)
                productionValue = round(farmerCurrent - farmerPrev, 1)
                
            return calculation, productionValue
        except psycopg2.DatabaseError as error:
            return error
    
    def areaHarvestedGrowth(self,year):
        calculation = 0.0
        try:
            stmt3 = sql.SQL(""" SELECT year,area_harvested FROM public.production 
                                WHERE year = '{year}';
                            """.format(year = year))

            cur.execute(stmt3)
            connection.commit()
            fetchdata = cur.fetchall()
            result = cur.rowcount

            farmerCurrent = 0.0
            selectedYear = 0
            if result:
                for data in fetchdata:
                    farmerCurrent += data[1]
                    selectedYear = data[0]
            
            if farmerCurrent:
                prevYear = int(selectedYear)-1
                prev = sql.SQL(""" SELECT area_harvested FROM public.production 
                                    WHERE year = '{prevYear}';
                                """.format(prevYear = prevYear))

                cur.execute(prev)
                connection.commit()
                fetchPrev = cur.fetchall()
                resultPrev = cur.rowcount

                farmerPrev = 0.0
                if resultPrev:
                    for dataPrev in fetchPrev:
                        farmerPrev += float(dataPrev[0])
                
                calculation = round((farmerCurrent - farmerPrev) / farmerCurrent, 2)
                areaValue = round(farmerCurrent - farmerPrev, 1)
                
            return calculation, areaValue
        except psycopg2.DatabaseError as error:
            return error
    
    def productionRate(self,year):
        calculation = 0.0
        try:
            stmt3 = sql.SQL(""" SELECT production_rate FROM public.production 
                                WHERE year = '{year}';
                            """.format(year = year))

            cur.execute(stmt3)
            connection.commit()
            fetchdata = cur.fetchall()
            result = cur.rowcount

            productionRate = 0.0
            if result:
                for data in fetchdata:
                    productionRate += data[0]

            calculation = productionRate / result
            return round(calculation,2)
        except psycopg2.DatabaseError as error:
            return error

    def queryFactorArea(self,muni):
        try:
            df = pd.read_sql(('SELECT year,lowland,upland FROM public.resources '
                                'WHERE municipality = %(muni)s'),
                            engine,params={"muni":muni})
            return df

        except psycopg2.DatabaseError as error:
            return error
    
    def queryFactorVariety(self,muni):
        try:
            df = pd.read_sql(('SELECT year,hybrid,inbrid FROM public.resources '
                                'WHERE municipality = %(muni)s'),
                            engine,params={"muni":muni})
            return df
        except psycopg2.DatabaseError as error:
            return error
    
