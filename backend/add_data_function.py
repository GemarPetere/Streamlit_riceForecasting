from dateutil.relativedelta import relativedelta
from backend.store_to_resources_table import StoreResources
from config import connect, engine
from psycopg2 import sql
import psycopg2

engine = engine()


class AddData:
    """
        Adding data from the input to database

    """
    """ def __init__(self,municipality:str, dFrom:str, dTo:str, crop_batch:str,
                 hybrid: float, inbrid:float, upland:float, 
                 lowland:float, farmer:int,production_rate):

        self.municipality = municipality
        self.dFrom = dFrom
        self.dTo = dTo
        self.crop_batch = crop_batch
        self.hybrid = hybrid
        self.inbrid = inbrid
        self.upland = upland
        self.lowland = lowland
        self.farmer = farmer
        self.production_rate = production_rate """


    def AddDataCsV(self,dataFrame):
        dataFrame.to_sql('test_production', engine,index=False,if_exists='append')

        return True


    """  def addData(self):

        municipality = self.municipality
        dFrom = self.dFrom
        dTo = self.dTo
        crop = self.crop_batch
        hybrid = float(self.hybrid)
        inbrid = float(self.inbrid)
        upland = float(self.upland)
        lowland = float(self.lowland)
        farmer = int(self.farmer)
        production_rate = float(self.production_rate)

        #Calculate the total
        production = hybrid + inbrid + upland + lowland


        #process to get year from dFrom and dTo variable
        year = dTo.year

        #process the crop_season and temperature depend in the crop batch 
        if crop == 1:
            crop_season = "Dry Cropping"
            temperature = 31.42
        else:
            crop_season = "Wet Cropping"
            temperature = 27.73

        '''#process to get the weather average of the crop range timeperiod
        months = []
        collected_month = []
        x = 0
        while dFrom <= dTo:
            months.append(dFrom)
            dFrom += relativedelta(months = 1)
            collected_month.append(months[x].month)
            x+=1''' 

        fetchdata = []
        sample = ()
        s = 0.0
        y=0
        count = 0
        try:
            conn = connect()
            cur = conn.cursor()
            #select the weather datas and process to store in crop_season
            
            '''while y < len(collected_month):
                count+=1
                getMonth = collected_month[y]
                stmt1 = sql.SQL(SELECT temperature
                                       FROM public.weather WHERE weather_id = '{}';
                                    .format(getMonth))
                cur.execute(stmt1)
                conn.commit()
                fetchdata.append(cur.fetchall())
                result = cur.rowcount
                y += 1      
                print(getMonth)  
            
            #get the data from fetched datas which are the temperature average per month
            for z in fetchdata:
                sample += z[0]
                s += sample[0]
            
            weather_data = s / count'''

            #if result:
            stmt2 = sql.SQL(INSERT INTO
                                public.production(municipality, year, crop_batch, crop_season, hybrid, 
                                                  inbrid, lowland, upland, farmer, production_mt,
                                                  production_rate,temperature, date_from,date_to)
                                SELECT '{municipality}', '{year}', '{crop}','{crop_season}', '{hybrid}', 
                                       '{inbred}', '{lowland}', '{upland}', '{farmer}','{production_mt}',
                                       '{production_rate}','{temperature}','{dFrom}','{dTo}'
                                WHERE
                                    NOT EXISTS (
                                        SELECT production_id FROM public.production  WHERE municipality = '{municipality}' AND year = '{year}'
                                );.format(municipality = municipality, year = year, 
                                            crop = crop, crop_season = crop_season, 
                                            hybrid = hybrid, inbred = inbrid, 
                                            upland = upland, lowland = lowland, 
                                            farmer = farmer,production_mt= production,
                                            temperature = temperature, dFrom = dFrom, dTo = dTo,
                                            production_rate = production_rate))
            cur.execute(stmt2)
            conn.commit()
            result1 = cur.rowcount
            cur.close()

            if result1:
                storeResource = StoreResources(municipality,year,crop,crop_season,temperature,
                                               hybrid,inbrid,upland,lowland,farmer,production)
                storeResource.getProduction()
                return result1
        except psycopg2.DatabaseError as error:
            print(error)
            return error
        except Exception as e:
            return e """
        

        



    