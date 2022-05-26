from config import connect
from psycopg2 import sql
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

        try:
            conn = connect()
            cur = conn.cursor()

            
            '''stmt1 = sql.SQL(""" SELECT municipality,year,crop_batch,crop_season,
                                    temperature,hybrid,inbrid,
                                    lowland,upland,production_mt
                                FROM public.production ORDER BY year DESC;""")
            cur.execute(stmt1)
            conn.commit()
            fetchdata.append(cur.fetchall())
            result = cur.rowcount

            if result:
                c = 0
                for x in fetchdata:
                    for y in x:
                        stmt2 = sql.SQL("""INSERT INTO
                                            public.resources(municipality, year, crop_batch,crop_season, weather,hybrid, inbrid, lowland, upland,production_mt)
                                        SELECT '{municipality}', '{year}', '{crop_batch}','{crop_season}', '{weather_data}', '{hybrid}', '{inbred}', '{lowland}', '{upland}','{total}'
                                        WHERE
                                            NOT EXISTS (
                                                SELECT resources_id FROM public.resources  
                                                WHERE municipality = '{municipality}' AND year = '{year}' AND crop_batch = '{crop_batch}'
                                        );""" .format(municipality = y[0],year = y[1],
                                                    crop_batch = y[2],  crop_season = y[3],
                                                    weather_data = y[4], hybrid = y[5], 
                                                    inbred = y[6],lowland = y[7], 
                                                    upland = y[8],total = y[9]))
                        cur.execute(stmt2)
                        conn.commit()
                        result2 = cur.rowcount
                    
            cur.close()'''

            stmt1 = sql.SQL(""" SELECT municipality,year,crop_batch,crop_season,
                                    temperature,hybrid,inbrid,
                                    lowland,upland,production_mt
                                FROM public.test_production ORDER BY year DESC;""")
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
                    print(datas[0])
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
                                            public.test_resources(municipality, year, crop_batch,crop_season, weather,
                                                            hybrid, inbrid, lowland, upland,production_mt)
                                                            
                                        SELECT '{municipality}', '{year}', '{crop_batch}','{crop_season}', 
                                            '{weather_data}', '{hybrid}', '{inbred}', '{lowland}', '{upland}','{total}'
                                        WHERE
                                            NOT EXISTS (
                                                SELECT resources_id FROM public.test_resources  
                                                WHERE municipality = '{municipality}' AND year = '{year}' AND crop_batch = '{crop_batch}'
                                        );""" .format(municipality = municipal,year = year,
                                                    crop_batch = cropBatch,crop_season = cropSeason,
                                                    weather_data = weather, hybrid = hybrid, 
                                                    inbred = inbrid,lowland = lowland, 
                                                    upland = upland,total = production))
                        cur.execute(stmt2)
                        conn.commit()
                        result2 = cur.rowcount
                            
                        x+=1
                cur.close()

        except psycopg2.DatabaseError as error:
            print(error)
            return error