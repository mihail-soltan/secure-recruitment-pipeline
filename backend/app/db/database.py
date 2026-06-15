
import oracledb
import app.core.config as config
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
pool = None

def init_db_pool():
    global pool
    pool = oracledb.create_pool(
        min=2,          
        max=10,         
        increment=1,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
        dsn=config.CONNECT_STRING,
        config_dir=config.WALLET_LOCATION,
        wallet_location=config.WALLET_LOCATION,
        wallet_password=config.WALLET_PASSWORD
    )

def close_db_pool():
    global pool
    if pool:
        pool.close()


def get_db_connection():
    if pool is None:
        raise RuntimeError("Database pool has not been initialized.")
    
    connection = pool.acquire()

    try:     
        yield connection

    finally:
        connection.close()