from config import connect
from psycopg2 import DatabaseError, sql
import psycopg2


class StoreResources:

    """ def __init__(self, municipality,year,crop_batch,crop_season,weather,hybrid,
                 inbrid,lowland,upland,production_mt):

        self.municipality = municipality
        self.year = year
        self.crop_batch = crop_batch
        self.crop_season = crop_season
        self.weather = weather
        self.hybrid = hybrid
        self.inbrid = inbrid
        self.lowland = lowland
        self.upland = upland
        self.production_mt = production_mt

    """
    def getProduction(self):
        result = []
        fetchdata = []
        print("Sulod Kaayo")
        try:
            conn = connect()
            cur = conn.cursor()

            stmt1 = sql.SQL(""" SELECT municipality,year,crop_batch,crop_season,
                                    temperature,hybrid,inbrid,lowland,upland,production_mt
                                FROM public.datatemporary ORDER BY year DESC;""")
            cur.execute(stmt1)
            conn.commit()
            fetchdata.append(cur.fetchall())
            result = cur.rowcount

            if result:
                
                municipal = ""
                year = ""
                cropBatch = ""
                cropSeason = ""
                hybrid = 0.0
                inbrid = 0.0
                lowland = 0.0
                upland = 0.0
                production = 0.0
                weather = 0.0

                for datas in fetchdata:
                    x = 0
                    while x < len(datas):
                        y = 0
                        while y < len(datas[x]):
                            if y == 0:
                                
                                municipal = datas[x][y]
                                
                            elif y == 1:
                                year = datas[x][y]
                                
                            elif y == 2:
                                cropBatch = datas[x][y]
                                
                            elif y == 3:
                                cropSeason = datas[x][y]
                                
                            elif y == 4:
                                weather = datas[x][y]
                                
                            elif y == 5:
                                hybrid = datas[x][y]
                                
                            elif y == 6:
                                inbrid = datas[x][y]
                                
                            elif y == 7:
                                lowland = datas[x][y]
                               
                            elif y == 8:
                                upland = datas[x][y]  
                            elif y == 9:
                                production = datas[x][y]  
                            y+=1

                        stmt2 = sql.SQL("""INSERT INTO
                                            public.resources(municipality, year, crop_batch,crop_season, weather,
                                                            hybrid, inbrid, lowland, upland,production_mt)
                                                            
                                        SELECT '{municipality}', '{year}', '{crop_batch}','{crop_season}', 
                                            '{weather_data}', '{hybrid}', '{inbred}', '{lowland}', '{upland}','{total}'
                                        WHERE
                                            NOT EXISTS (
                                                SELECT resources_id FROM public.test_resources  
                                                WHERE municipality = '{municipality}' AND year = '{year}' AND crop_batch = '{crop_batch}'
                                        ); """.format(municipality = municipal,year = year,
                                                    crop_batch = cropBatch,crop_season = cropSeason,
                                                    weather_data = weather, hybrid = hybrid, 
                                                    inbred = inbrid,lowland = lowland, 
                                                    upland = upland,total = production))
                        cur.execute(stmt2)
                        conn.commit()
                        result2 = cur.rowcount

                        if result2:
                            print(result2)
                            self.deleteDataTemporary()
                            
                        x+=1
                cur.close()

        except psycopg2.DatabaseError as error:
            print(error)
            return error

    def deleteDataTemporary(self):
        try:
            conn = connect()
            cur = conn.cursor()

            stmt1 = sql.SQL(""" DELETE FROM public.datatemporary""")
            cur.execute(stmt1)
            conn.commit()

            print("Successfully Deleted!")
        except psycopg2.DatabaseError as error:
            return error