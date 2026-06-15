from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordRequestForm
from app.db.database import get_db_connection
from app.services.user_service import authenticate_user_db, get_candidate_id
from app.core.security import create_access_token
from app.core.dependencies import get_current_user

router = APIRouter(prefix="/users", tags=["Users"])

@router.post("/login")
def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(), 
    conn = Depends(get_db_connection)
):
    try:
        #auth la nivel de db
        role = authenticate_user_db(conn, form_data.username, form_data.password)
        
        #get candidate_id
        candidate_id = None
        if role == 'CANDIDATE':
            candidate_id = get_candidate_id(conn, form_data.username)

        #creare token
        access_token = create_access_token(
            data={"sub": form_data.username, "role": role, "candidate_id": candidate_id}
        )
        
        return {"access_token": access_token, "token_type": "bearer"}
        
    except RuntimeError as e:
        raise HTTPException(status_code=401, detail=str(e))

@router.get("/me")
def read_users_me(current_user: dict = Depends(get_current_user)):
    """test pt a verifica cine e logat."""
    return current_user