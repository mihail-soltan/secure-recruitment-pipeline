import streamlit as st
import os
import requests


API_URL = os.getenv("BACKEND_URL", "http://backend_service:8000")
if "access_token" not in st.session_state:
    st.session_state.access_token = None

def get_auth_header():
    return {"Authorization": f"Bearer {st.session_state.access_token}"}

def login(username, password):

    response = requests.post(
        f"{API_URL}/users/login",
        data={"username": username, "password": password}
    )
    if response.status_code == 200:
        st.session_state.access_token = response.json().get("access_token")
        st.rerun()

    else:
        st.error("Invalid credentials")

def fetch_me():
    response = requests.get(f"{API_URL}/users/me", headers=get_auth_header())
    return response.json() if response.status_code == 200 else None

def fetch_jobs():
    response = requests.get(f"{API_URL}/jobs/", headers=get_auth_header())
    return response.json() if response.status_code == 200 else []

def apply_to_job(job_id, email, resume_file):
    files = {"resume": (resume_file.name, resume_file.getvalue(), resume_file.type)}
    data = {"email": email}
    
    response = requests.post(
        f"{API_URL}/jobs/apply/{job_id}",
        headers=get_auth_header(),
        data=data,
        files=files
    )
    return response.status_code == 200

if not st.session_state.access_token:
    st.title("System Login")
    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Login")
        
        if submitted:
            login(username, password)
else:
    st.sidebar.title("Navigation")
    user_data = fetch_me()
    
    if user_data:
        st.sidebar.write(f"Logged in as: {user_data.get('username', 'User')}")
        
    if st.sidebar.button("Logout"):
        st.session_state.access_token = None
        st.rerun()

    st.title("Open Jobs Dashboard")
    
    jobs = fetch_jobs()
    if not jobs:
        st.info("No active jobs available or failed to fetch.")
    
    for job in jobs:
        job_id = job.get("JOB_ID")
        job_title = job.get("title", f"Job #{job_id}")
        
        with st.expander(f"Apply for: {job_title}"):
            with st.form(f"apply_form_{job_id}"):
                email = st.text_input("Applicant Email")
                resume = st.file_uploader("Upload Resume (PDF/DOCX)")
                
                submitted = st.form_submit_button("Submit Application")
                if submitted:
                    if not email or not resume:
                        st.warning("Please provide both email and a resume file.")
                    else:
                        success = apply_to_job(job_id, email, resume)
                        if success:
                            st.success("Application submitted successfully!")
                        else:
                            st.error("Failed to submit application.")