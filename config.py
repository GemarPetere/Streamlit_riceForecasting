#!/usr/bin/python
from sqlalchemy import create_engine
import streamlit as st
import psycopg2
import json


@st.experimental_singleton
def connect():
    connection = None
    try:
        return psycopg2.connect(**st.secrets["postgres"])
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
        return connection

@st.experimental_singleton
def engine():
    try:
        engine = create_engine('postgresql://postgres:postgres123@localhost:5432/postgres')
        return engine
    except Exception as e:
        return e

