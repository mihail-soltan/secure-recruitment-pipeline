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


def get_job_applications_for_recruiter(conn, job_id):
    """returneaza aplicatiile procesate pentru un job"""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT app_id, secure_resume_text, status, applied_at
            FROM recruit_owner.JOB_APPLICATION
            WHERE job_id = :1 AND status = 'PROCESSED_SECURE'
        """, [job_id])
        
        columns = [col[0].lower() for col in cursor.description]
        rows = cursor.fetchall()
        applications = []
        for row in rows:
            app_dict = {}
            for col_name, val in zip(columns, row):

                if val is not None and hasattr(val, "read"):
                    app_dict[col_name] = val.read()
                else:
                    app_dict[col_name] = val
            applications.append(app_dict)
            
        return applications
    finally:
        cursor.close()

def get_candidate_email_and_update_status(conn, app_id):
    """extrage email-ul si schimba statusul in INTERVIEW"""
    cursor = conn.cursor()
    try:
        # reidentificarea candidatului prin JOIN la nivel de DB
        cursor.execute("""
            SELECT c.email 
            FROM recruit_owner.CANDIDATE c
            JOIN recruit_owner.JOB_APPLICATION ja ON c.candidate_id = ja.candidate_id
            WHERE ja.app_id = :1
        """, [app_id])
        row = cursor.fetchone()
        email = row[0] if row else None
        
        if email:
            cursor.execute("UPDATE recruit_owner.JOB_APPLICATION SET status='INTERVIEW' WHERE app_id=:1", [app_id])
            conn.commit()
            
        return email
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()

