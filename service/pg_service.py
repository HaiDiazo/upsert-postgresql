import json
import traceback

from config.pg_config import PostgresConfig


class PgService:

    def __init__(self, data: dict):
        self.__data: dict = data
        self.__table: str = 'tb_posts'
        self.__primary_field: str = 'id'

    @staticmethod
    def _open_connection():
        config = PostgresConfig()
        return config.connect()

    @staticmethod
    def generate_update_values(data: dict):
        keys = [f"{key} = EXCLUDED.{key}" for key in data]
        return ','.join(keys)

    def _value_map(self):
        result = {}
        for key, value in self.__data.items():
            if isinstance(value, (list, dict)):
                result[key] = json.dumps(value)
            else:
                result[key] = value
        return result

    def upsert(self):
        try:
            connect = self._open_connection()
            cursor = connect.cursor()
            query_upsert = f"""
                INSERT INTO {self.__table} ({",".join([field for field in self.__data])}) VALUES ({",".join(["%s" for _ in self.__data])})
                ON CONFLICT ({self.__primary_field}) 
                DO 
                UPDATE SET {self.generate_update_values(data=self.__data)};
            """
            param = tuple([value for _, value in self._value_map().items()])
            cursor.execute(query_upsert, param)

            connect.commit()
            print(f"Data upsert into table {self.__table}")
        except Exception as e:
            traceback.print_exc()
            raise e
