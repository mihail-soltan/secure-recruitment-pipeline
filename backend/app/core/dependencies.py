from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import jwt, JWTError
from app.core.config import SECRET_KEY, ALGORITHM
from app.db.database import get_db_connection
import oracledb

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="users/login")

def get_current_user(token: str = Depends(oauth2_scheme)):
    """decode JWT si returneaza datele utilizatorului"""
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        role: str = payload.get("role")
        candidate_id: int = payload.get("candidate_id")
        
        if username is None or role is None:
            raise credentials_exception
            
        return {"username": username, "role": role, "candidate_id": candidate_id}
    except JWTError:
        raise credentials_exception

def get_secure_db(
    current_user: dict = Depends(get_current_user),
    conn: oracledb.Connection = Depends(get_db_connection)
):
    """seteaza contextul VPD in Oracle pentru conexiunea curenta"""
    cursor = conn.cursor()
    try:
        cursor.callproc("recruit_owner.security_pkg.set_session_context", [current_user["username"], current_user["role"]])
    except Exception as e:
        raise HTTPException(status_code=500, detail="VPD Security Context could not be set.")
    finally:
        cursor.close()
    
    return conn