from pydantic import BaseModel, EmailStr
from datetime import date
from typing import Optional

class UserLogin(BaseModel):
    username: str
    password: str

class JobResponse(BaseModel):
    job_id: int
    title: str
    description: str
    min_salary: Optional[float]
    max_salary: Optional[float]
    deadline: Optional[date]

    class Config:
        from_attributes = True