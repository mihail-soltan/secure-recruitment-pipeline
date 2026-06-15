from app.services.kafka_service import kafka_service
import oracledb

async def process_job_application(conn: oracledb.Connection, job_id: int, candidate_id: int, email: str, file_content: bytes) -> int:
    """
    gestioneaza logica de business pentru aplicarea la un job. salveaza metadata in Oracle db si trimite PDF catre Kafka
    """
    cursor = conn.cursor()
    try:
        # check daca candidatul a aplicat deja la acest job
        check_sql = """
            SELECT COUNT(*) FROM recruit_owner.JOB_APPLICATION 
            WHERE job_id = :1 AND candidate_id = :2
        """
        cursor.execute(check_sql, [job_id, candidate_id])
        count = cursor.fetchone()[0]
        if count > 0:
            raise Exception("Candidate has already applied to this job")
        sql = """
            INSERT INTO recruit_owner.JOB_APPLICATION (job_id, candidate_id, status) 
            VALUES (:1, :2, 'RECEIVED') 
            RETURNING app_id INTO :3
        """
        app_id_var = cursor.var(int)
        cursor.execute(sql, [job_id, candidate_id, app_id_var])
        app_id = app_id_var.getvalue()[0]
        
        conn.commit()

       
        kafka_service.send_cv_to_pipeline(
            app_id=app_id, 
            candidate_email=email, 
            file_content=file_content
        )

        return app_id

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()