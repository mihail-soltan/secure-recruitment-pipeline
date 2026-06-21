from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import base64
import requests
import fitz  # PyMuPDF
import oracledb
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

KAFKA_PASSWORD = os.getenv("KAFKA_PASSWORD")
KAFKA_USER = os.getenv("KAFKA_USER")
KAFKA_TRUSTSTORE_PASSWORD = os.getenv("KAFKA_TRUSTSTORE_PASSWORD")

error_json = {
    "summary": "ERROR_PROCESSING_CV",
}
json_error_str = json.dumps(error_json)

def process_partition(partition):
    """ 
    aceasta functie va fi executata de fiecare worker pentru fiecare partitie de date primita de la Master
    """
    import os
    import base64
    import fitz  # PyMuPDF
    import requests
    import oracledb

    # workerii preiau env variables
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    DB_DSN = os.getenv("DB_DSN")
    WALLET_LOCATION = os.getenv("WALLET_LOCATION")
    WALLET_PASSWORD = os.getenv("WALLET_PASSWORD")
    
    # stabilim o singura conexiune / partitie
    try:
        
        conn = oracledb.connect(
            user=DB_USER, 
            password=DB_PASS, 
            dsn=DB_DSN,
            config_dir=WALLET_LOCATION,
            wallet_location=WALLET_LOCATION,
            wallet_password=WALLET_PASSWORD
        )
        cursor = conn.cursor()
        
    except Exception as e:
        print(f"[ERROR] Worker failed to connect to Oracle: {e}")
        return 

    # procesam fiecare rand din partitie
    for row in partition:
        app_id = row['app_id']
        pdf_b64 = row['file_content']
        email = row['email']

        if not pdf_b64:
            continue

        try:
            # Decode si extrage textul din PDF
            pdf_bytes = base64.b64decode(pdf_b64)
            raw_text = ""
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            for page in doc:
                raw_text += page.get_text()
            doc.close()
            
            # anonimizare cu Mistral prin Ollama
            prompt = (
            f"""
            You are an expert data extraction and privacy AI. Process the following resume text into a standardized JSON profile.
            
            STRICT RULES:
            1. OMIT PII: Completely exclude the candidate's Name, Phone, Email, Physical Address, Company Names, and University Names. Do NOT use placeholders like "[REDACTED]". Just drop the identifying information entirely.
            2. ROLES & DEGREES: For the 'role' field, extract ONLY the job title (e.g., "Cloud Support Engineer"). For 'education', extract ONLY the degree and major (e.g., "M.Sc. Databases and Software Technologies"). Do not include the employer or school.
            3. DURATION FORMAT: Standardize all dates in the 'duration' field to the exact format "MM/YYYY - MM/YYYY" or "MM/YYYY - Present" (e.g., "10/2022 - Present").
            
            You must output your response in valid JSON matching this exact structure:
            {{
                "summary": "A brief, professional overview of the candidate without any identifying details.",
                "skills": ["Skill 1", "Skill 2"],
                "experience": [
                    {{
                        "role": "Clean Job Title Only", 
                        "duration": "MM/YYYY - MM/YYYY", 
                        "responsibilities": "Description of work"
                    }}
                ],
                "education": ["Clean Degree Name Only"]
            }}
            
            Resume Text:
            {raw_text}
            """
            )
        
            ollama_payload = {
                "model": "mistral",
                "prompt": prompt,
                "stream": False,
                "format": "json"
            }
            llm_response = requests.post('http://ollama:11434/api/generate', json=ollama_payload, timeout=600)
            if llm_response.status_code == 200:
                anonymized_text = llm_response.json().get('response', '').strip()
                if not anonymized_text:
                    handle_processing_error(cursor, app_id, json_error_str, email)
                    raise Exception("Mistral returned an empty string.")
                print(f"[OLLAMA SUCCESS] Generated {len(anonymized_text)} characters.")
            else:
                print(f"[OLLAMA ERROR] {llm_response.status_code} - {llm_response.text}")
                anonymized_text = "ERROR_GENERATING_TEXT"    
                handle_processing_error(cursor, app_id, json_error_str, email)
                raise Exception(f"Mistral API error: {llm_response.status_code} - {llm_response.text}")
            # salvam in Oracle
            cursor.execute(
                """
                UPDATE recruit_owner.JOB_APPLICATION 
                SET status='PROCESSED_SECURE', 
                    secure_resume_text=:1 
                WHERE app_id=:2
                """,
                [anonymized_text, app_id]
            )
            
            print(f"[SUCCESS] App ID {app_id} processed by worker.")

        except Exception as e:
            handle_processing_error(cursor, app_id, json_error_str, email)
            print(f"[ERROR] Worker failed processing App ID {app_id}: {str(e)}")
    
    # commit o singura data la finalul partitiei
    try:
        conn.commit()
    except Exception as e:
        print(f"[ERROR] Failed to commit partition batch: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def handle_processing_error(cursor, app_id, error_message, email):
    """ 
    in caz de eroare, actualizam statusul aplicatiei in Oracle si logam eroarea
    """
    try:
        cursor.execute(
            """
            UPDATE recruit_owner.JOB_APPLICATION 
            SET status='PROCESSING_ERROR', 
                secure_resume_text=:1 
            WHERE app_id=:2
            """,
            [error_message, app_id]
        )
        cursor.connection.commit()
        notify_candidate_error(email, app_id)
    except Exception as e:
        print(f"[ERROR] Failed to log error for App ID {app_id}: {e}")


def write_to_oracle(df, epoch_id):
    """ 
    masterul va apela aceasta functie pentru fiecare batch de date primit de la Kafka, 
    iar aceasta va delega procesarea fiecarui rand catre workerii disponibili folosind process_partition
    """
    df.foreachPartition(process_partition)

def notify_candidate_error(email, app_id):
    """ 
    in caz de eroare, se trimite un email candidatului pentru a-l anunta ca a intervenit o problema
    """
    SMTP_SERVER = os.getenv("SMTP_SERVER")
    SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
    EMAIL_SENDER = os.getenv("EMAIL_SENDER")
    EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

    subject = "Issue with Your Job Application"
    body = f"""
    Buna ziua,
    
    Am intampinat o eroare la procesarea securizata a CV-ului dumneavoastra 
    pentru aplicatia cu numarul {app_id}. Formatul fisierului PDF nu a putut fi citit 
    sau a aparut o eroare interna. 
    
    Va rugam sa reincarcati documentul. Ne cerem scuze pentru neplaceri!

    Echipa de recrutare
    """

    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))

    try:
        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.send_message(msg)
        print(f"[EMAIL SENT] Notification sent to {email} for App ID {app_id}")
    except Exception as e:
        print(f"[EMAIL ERROR] Failed to send notification to {email} for App ID {app_id}: {e}")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SecureRecruitmentPipeline") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("app_id", IntegerType(), True),
        StructField("email", StringType(), True),
        StructField("file_content", StringType(), True) # PDF-ul transmis ca Base64 String
    ])

    # configuratii  Kafka SASL_SSL pentru Java/Spark
    kafka_options = {
        "kafka.bootstrap.servers": "kafka:29092",
        "subscribe": "cv_uri_brute",
        "startingOffsets": "latest",
        
        # Securitate SASL_SSL
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "SCRAM-SHA-256",
        
        "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.scram.ScramLoginModule required username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";',
        
        "kafka.ssl.truststore.location": "/opt/spark/secrets/kafka.server.truststore.jks",
        "kafka.ssl.truststore.password": KAFKA_TRUSTSTORE_PASSWORD,

        "kafka.max.partition.fetch.bytes": "10485760",  # 10 MB
        "kafka.fetch.max.bytes": "10485760"  # 10 MB
    }
    
    # Citire streaming din Kafka
    df = spark.readStream \
        .format("kafka") \
        .options(**kafka_options) \
        .load()

    # Parsare JSON
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # Procesare foreachBatch
    query = parsed_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_oracle) \
        .start()

    print("Spark Streaming Pipeline started. Waiting for CVs...")
    query.awaitTermination()