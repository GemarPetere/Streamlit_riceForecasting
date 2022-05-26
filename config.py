#!/usr/bin/python
from sqlalchemy import create_engine
import psycopg2
import json


def connect():
    connection = None
    try:
        connection = psycopg2.connect(user = 'postgres',
                                        password = 'postgres123',
                                        host = 'localhost',
                                        port = '5432',
                                        database = 'postgres')
        return connection
    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
        return connection


def engine():

    try:
        engine = create_engine('postgresql://postgres:postgres123@localhost:5432/postgres')
        return engine
    except Exception as e:
        return e
