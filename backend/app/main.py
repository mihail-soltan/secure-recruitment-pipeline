from app.db import database
import logging
from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.routers import users, jobs


@asynccontextmanager
async def lifespan(app: FastAPI):
    database.init_db_pool()
    yield
    database.close_db_pool()

app = FastAPI(lifespan=lifespan)

app.include_router(users.router)
app.include_router(jobs.router)

@app.get("/")
def read_root():
    return {"message": "Recruitment API is Secure and Active"}