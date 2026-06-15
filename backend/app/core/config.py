import os
import dotenv
dotenv.load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
WALLET_LOCATION = os.getenv("WALLET_LOCATION")
WALLET_PASSWORD = os.getenv("WALLET_PASSWORD")
CONNECT_STRING = os.getenv("CONNECT_STRING")
SECRET_KEY = os.getenv("SECRET_KEY", "super-secret-key")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 60