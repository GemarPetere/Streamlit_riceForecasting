from backend.add_data_function import AddData
from backend.store_to_resources_table import StoreResources
from backend.prediction_process import Prediction
from backend.visualize import VisualizeData
from backend.visualizeActualValue import ActualValue
from backend.generateReport import Report
from streamlit_folium import folium_static 


import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.figure_factory as ff
import plotly.express as px

import datetime

from urllib.request import urlopen

import altair as alt
import pydeck as pdk
import folium
import json
import numpy as np

def main():
    visualize = VisualizeData()
    visualizeActual = ActualValue()
    predict = Prediction()
    predict.getResource()
    addData = AddData()
    storeResource = StoreResources()
    report = Report()
    year = visualize.selectYear()


    menu = ['Dashboard', 'Add Data','Generate  Report']
    choice = st.sidebar.selectbox("Menu", menu)

    match (choice):

        case 'Dashboard':
            st.header('Province Actual Values')

            with st.container():

                year = st.slider('Select Year', 2011, 2020, 2012)
                st.write("You select", year)

                farmer = visualizeActual.farmersGrowth(str(year))
                production = visualizeActual.productionGrowth(str(year))
                areaHarvest = visualizeActual.areaHarvestedGrowth(str(year))
                productionRate = visualizeActual.productionRate(str(year))

                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Farmers Growth", "{farmerValue}".format(farmerValue = farmer[1]),  "{farmer} %".format(farmer = farmer[0]))
                col2.metric("Area Harvested Growth", "{value} Ha".format(value = areaHarvest[1]), "{area}%".format(area = areaHarvest[0]))
                col3.metric("Production Growth", "{value}mt".format(value = production[1]), "{production}%".format(production = production[0]))
                col4.metric("Production Ave. Rate", "(Mt/Ha)", "{productionRate}%".format(productionRate = productionRate)) 

            st.markdown("""<hr style="height:2px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)


            # Within expander is the factor actual values
            with st.expander("Actual Factors Trend"):
                col_1, col_2 = st.columns(2)

                
                with col_1:
                    
                    muni = st.selectbox(
                    'Select Municipality',
                        ('Banaybanay','Lupon','Gov. Gen.','Mati','Manay','Baganga','Caraga','Boston','Cateel'))
                    st.write('You selected:', muni)

            

                area = visualizeActual.queryFactorArea(muni)
                variety = visualizeActual.queryFactorVariety(muni)

                st.subheader('Area Factor')
                st.line_chart(area.rename(columns={'year':'index'}).set_index('index'))
                st.subheader('Variety Factor')
                st.line_chart(variety.rename(columns={'year':'index'}).set_index('index'))

            
                

            st.markdown("""<hr style="height:2px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)

            st.header('Predicted Values')

            # =============== Line Graph for forecating Trend ==================== 
            with st.expander('Forecasted Trend'):

                colu1, colu2 = st.columns(2)
                with colu1:
                    trendProduction = st.selectbox(
                    'Select Municipal',
                    ('Banaybanay','Lupon','Gov. Gen.','Mati','Manay','Baganga','Caraga','Boston','Cateel'))
                
                    trendProductionBatch = st.radio(
                        "Select Variety",
                        ('Hybrid', 'Inbrid'))

                    trend = visualize.lineGraphOne(trendProduction,trendProductionBatch)
                    trend2 = visualize.lineGraphTwo(trendProduction,trendProductionBatch)
                
                    
                    match (trendProductionBatch):
                        case 'Hybrid':
                            print(trendProduction)
                            #======================= Crop Batch 1===========================
                            f = px.line(trend, x="year", y="hybrid", title="Crop batch 1",
                                    color='label', height=400)

                            f.update_xaxes(title="Year")
                            f.update_yaxes(title="Rice Production metric tons")
                            st.plotly_chart(f)
                            #==================== Crop batch 2 ===============================
                            s = px.line(trend2, x="year", y="hybrid", title="Crop batch 2",
                                    color='label', height=400)

                            s.update_xaxes(title="Year")
                            s.update_yaxes(title="Rice Production metric tons")
                            st.plotly_chart(s)
                        case 'Inbrid':
                            #======================= Crop Batch 1===========================
                            f = px.line(trend, x="year", y="inbrid", title="Crop batch 1",
                                    color='label', height=400)

                            f.update_xaxes(title="Year")
                            f.update_yaxes(title="Rice Production metric tons")
                            st.plotly_chart(f)
                            #==================== Crop batch 2 ===============================
                            s = px.line(trend2, x="year", y="inbrid", title="Crop batch 2",
                                    color='label', height=400)

                            s.update_xaxes(title="Year")
                            s.update_yaxes(title="Rice Production metric tons")
                            st.plotly_chart(s)

            
            # ================= End of Line Graph for forecasting Trend ===================



            col1, col2 = st.columns(2)

            with col1:
                years = visualize.selectYear()
                option = st.selectbox(
                    'Select Municipality',
                    (years[0][0],years[1][0],years[2][0],years[3][0],years[4][0]))
                st.write('You selected:', option)

            df = visualize.predictedData(option)
            
            f = px.histogram(df, x="municipality", y="production_mt", title="Rice Forecasting Per Municipality",
                            color='crop_batch', barmode='group',
                            height=400)

            f.update_xaxes(title="Year")
            f.update_yaxes(title="Rice Production metric tons")
            st.plotly_chart(f)

            cropYear = st.radio(
            "Select Crop Batch",
            ('1', '2'))

            dfYear = visualize.predictedMuni(option,cropYear)
                
            f = px.pie(dfYear[0], values='production_mt', names='municipality')
            f.update_xaxes(title="Year")
            f.update_yaxes(title="Rice Production")
            st.plotly_chart(f)
            st.subheader("Total Metric ton: {mt} mt".format(mt = dfYear[1]))



            #==============================================================
            st.markdown("""<hr style="height:2px;border:none;color:#333;background-color:#333;" /> """, unsafe_allow_html=True)


            dfMap = predict.mapping('2022',cropYear)
            
            df_new = predict.newDataFrameForMap(dfMap)

            davOr = json.load(open('davOr.geojson','r'))

            m = folium.Map(location=[7.3172, 126.5420],
                    zoom_start=9,
                    control_scale=True)

            threshold_scale = np.linspace(df_new['values'].min(),
                                        df_new['values'].max(),
                                        6, dtype=int)

            threshold_scale = threshold_scale.tolist() # change the numpy array to a list
            threshold_scale[-1] = threshold_scale[-1] + 1 # make sure that the last value of the list is greater than the maximum immigration

            folium.Choropleth(geo_data=davOr,
                            name='choropleth',
                            data=df_new,
                            columns=['iDs', 'values'],
                            key_on='feature.properties.ID',
                            threshold_scale=threshold_scale,
                            fill_color='YlGn',
                            fill_opacity=0.7,
                            line_opacity=0.2,
                            legend_name='Production Values',).add_to(m)

            folium.LayerControl().add_to(m)
            folium_static(m)
        
        case 'Add Data':
            st.subheader('File Upload & Saved file to Database')
            datafile = st.file_uploader("Upload CSV",type=['csv'])

            st.warning('Note! Make sure to have required attributes.')

            if datafile is not None:
                file_details = {"FileName":datafile.name, "FileType":datafile.type}
                df = pd.read_csv(datafile)
                st.write(df)

                if st.button('Save'):
                    success = addData.AddDataCsV(df)
                    if success:
                        storeResource.getProduction()
        
        case 'Generate  Report':
            
            st.subheader('Generate Data Either CSV or PDF format')
            fileFormat = st.selectbox(
                    'Select Format',
                    ('CSV Format','PDF Format'))
            st.write('You selected:', fileFormat)

            if fileFormat == 'CSV Format':
                yearGenerateData = st.selectbox(
                    'Select Year',
                    (year[0][0],year[1][0],year[2][0],year[3][0],year[4][0]))
                st.write('You selected:', yearGenerateData)

                reportDf = report.getPredictedData(yearGenerateData)
                csv = report.convert_df(reportDf)

                st.download_button(
                    label="Download data as CSV",
                    data=csv,
                    file_name='predicted_data.csv',
                    mime='text/csv',
                )
            elif fileFormat == 'Export PDF':
                st.write('Export PDF')
        
if __name__=='__main__':
    main()