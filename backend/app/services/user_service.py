import oracledb
from app.core.security import hash_password

def authenticate_user_db(connection, username, plain_password):
    cursor = connection.cursor()
    p_hash = hash_password(plain_password)
    role_var = cursor.var(str)
    
    try:
        cursor.callproc("recruit_owner.security_pkg.app_login", [username, p_hash, role_var])
        return role_var.getvalue()
    except oracledb.DatabaseError as e:
        error_obj, = e.args
        raise RuntimeError(f"Oracle Error: {error_obj.message}")
    finally:
        cursor.close()

def get_candidate_id(connection, username):
    """Extrage candidate_id pentru utilizatorul curent"""
    cursor = connection.cursor()
    try:
        cursor.execute("""
            SELECT c.candidate_id 
            FROM recruit_owner.CANDIDATE c
            JOIN recruit_owner.APP_USER u ON c.user_id = u.user_id
            WHERE u.username = :1
        """, [username])
        row = cursor.fetchone()
        return row[0] if row else None
    finally:
        cursor.close()