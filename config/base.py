from pydantic import BaseSettings


class Settings(BaseSettings):
    MP_PG_HOST: str
    MP_PG_USER: str
    MP_PG_PASS: str
    MP_PG_PORT: str
    MP_PG_DB: str

    MP_KAFKA_AI: str
    MP_KAFKA_TOPIC: str
    MP_KAFKA_GROUP_ID: str
    MP_KAFKA_AUTO_OFFSET_RESET: str
    MP_KAFKA_MAX_POLL_RECORDS: int

    class Config:
        env_file = '.env'


settings = Settings()
