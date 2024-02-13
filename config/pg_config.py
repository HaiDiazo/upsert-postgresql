from config.base import settings

import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import URL


class PostgresConfig(object):
    @staticmethod
    def connect():
        conn = psycopg2.connect(database=settings.MP_PG_DB,
                                host=settings.MP_PG_HOST,
                                user=settings.MP_PG_USER,
                                password=settings.MP_PG_PASS,
                                port=settings.MP_PG_PORT)
        return conn

    @staticmethod
    def conn_sql_alchemy():
        conn_string = f'postgresql://{settings.MP_PG_USER}:{settings.MP_PG_PASS}@{settings.MP_PG_HOST}/{settings.MP_PG_DB}'
        db = create_engine(conn_string)
        return db.connect()

    @staticmethod
    def prod_conn_sql_alchemy():
        url_object = URL.create(
            "postgresql",
            username=settings.MP_PG_USER,
            password=settings.MP_PG_PASS,  # plain (unescaped) text
            host=settings.MP_PG_HOST,
            database=settings.MP_PG_DB,
        )
        db = create_engine(url_object)
        return db.connect()


client = PostgresConfig()
