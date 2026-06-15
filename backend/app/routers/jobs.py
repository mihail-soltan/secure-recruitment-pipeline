import logging
import base64
from fastapi import APIRouter, Depends, UploadFile, File, Form, HTTPException
from app.core.dependencies import get_secure_db, get_current_user
from app.services.job_service import process_job_application
from app.services.kafka_service import kafka_service
## temporar 
import traceback
import json

logger = logging.getLogger("uvicorn.error")
logger.setLevel(logging.DEBUG)

router = APIRouter(prefix="/jobs", tags=["Jobs"])

@router.get("/")
def get_open_jobs(conn = Depends(get_secure_db)):
    """afiseaza toate joburile active. Conexiunea are contextul setat."""
    cursor = conn.cursor()
    cursor.execute("SELECT job_id, title, min_salary, max_salary FROM recruit_owner.JOB WHERE deadline > SYSDATE")
    
    # oracle db returneaza tupluri, le transformam in dict pt JSON
    columns = [col[0] for col in cursor.description]
    cursor.rowfactory = lambda *args: dict(zip(columns, args))
    
    jobs = cursor.fetchall()
    cursor.close()
    return jobs

@router.post("/apply/{job_id}")
async def apply_to_job(
    job_id: int,
    email: str = Form(...),
    resume: UploadFile = File(...),
    conn = Depends(get_secure_db),                  # db Conn cu VPD activ
    current_user: dict = Depends(get_current_user)
):
    if current_user["role"] != "CANDIDATE":
        raise HTTPException(status_code=403, detail="Only candidates can apply for jobs.")
        
    candidate_id = current_user.get("candidate_id")
    logger.debug(f"Candidate ID: {candidate_id}")
    

    if not candidate_id:
        raise HTTPException(status_code=400, detail="Candidate profile not found.")

    try:
        file_content_bytes = await resume.read()
        file_content_b64 = base64.b64encode(file_content_bytes).decode('utf-8')
        
        logger.info("Attempting to dispatch message to Kafka...")
        
        app_id = await process_job_application(
            conn=conn,
            job_id=job_id,
            candidate_id=candidate_id,
            email=email,
            file_content=file_content_b64
        )
        logger.info("--- DISPATCH SUCCESSFUL ---")

        return {"status": "Success", "application_id": app_id, "info": "CV sent for secure processing"}
    except Exception as e:
        logger.debug(f"{len(json.dumps(file_content_b64).encode('utf-8')) / (1024 * 1024)}")
        logger.error(f"====== FASTAPI CRASH ======\n{traceback.format_exc()}\n===========================")
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/test-kafka")
def test_kafka_connection():
    try:
        kafka_service.producer.produce(
            topic="test_topic",
            key="test_key",
            value="Hello from Secure FastAPI!"
        )
        kafka_service.producer.flush()
        return {"status": "success", "message": "message successfully dispatched to kafka"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}