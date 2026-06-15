from sqlalchemy import Column, Integer, String, Date, ForeignKey, BLOB, CLOB, TIMESTAMP, text
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

class AppUser(Base):
    __tablename__ = 'APP_USER'
    user_id = Column(Integer, primary_key=True)
    username = Column(String(50), unique=True, nullable=False)
    password_hash = Column(String(64))
    role = Column(String(20))
    is_active = Column(Integer, default=1)

class Candidate(Base):
    __tablename__ = 'CANDIDATE'
    candidate_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('APP_USER.user_id'))
    first_name = Column(String(50))
    last_name = Column(String(50))
    email = Column(String(100))
    phone = Column(String(20))
    resume_file = Column(BLOB)

class Job(Base):
    __tablename__ = 'JOB'
    job_id = Column(Integer, primary_key=True)
    title = Column(String(100))
    description = Column(CLOB)
    min_salary = Column(Integer)
    max_salary = Column(Integer)
    deadline = Column(Date)
