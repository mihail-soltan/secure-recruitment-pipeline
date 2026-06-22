import streamlit as st
import os
import requests
import json

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

def fetch_candidate_applications():
    response = requests.get(f"{API_URL}/users/applications/me", headers=get_auth_header())
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

# recruiter logic
def fetch_job_applicants(job_id):
    response = requests.get(f"{API_URL}/jobs/{job_id}/applications", headers=get_auth_header())
    return response.json() if response.status_code == 200 else []

def invite_candidate_to_interview(app_id):
    response = requests.post(f"{API_URL}/jobs/applications/{app_id}/invite", headers=get_auth_header())
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
    user_data = fetch_me()
    role = user_data.get("role", "CANDIDATE")
    st.sidebar.title("Navigation")
    
    if user_data:
        st.sidebar.success(f"Logged in as: {user_data.get('username', 'User')}")
        

    if st.sidebar.button("Logout"):
        st.session_state.access_token = None
        st.rerun()

    if role == "CANDIDATE":
        page = st.sidebar.radio("Go to:", ["Open Jobs", "My Applications"])

        if page == "Open Jobs":
            jobs = fetch_jobs()
            st.title("Open Jobs Dashboard")
            if not jobs:
                st.info("No active jobs available or failed to fetch.")
            else:
                col1, col2 = st.columns(2)
                with col1:
                    search_term = st.text_input("🔍 Search by Job Title", "")
                with col2:
                    sort_option = st.selectbox("↕️ Sort By", ["Default", "Salary: High to Low", "Deadline: Soonest"])
                if search_term:
                    jobs = [j for j in jobs if search_term.lower() in j.get("title", "").lower()]

                if sort_option == "Salary: High to Low":
                    jobs = sorted(jobs, key=lambda x: x.get("max_salary") or 0, reverse=True)

                st.write(f"Showing {len(jobs)} jobs")   

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
        elif page == "My Applications":
            st.title("My Applications")    
            applications = fetch_candidate_applications()
            if not applications:
                st.info("You haven't applied to any jobs yet.")
            else:
                for app in applications:
                    st.card = st.container(border=True)
                    with st.card:
                        st.subheader(app.get("title", "Unknown Job"))
                        st.write(f"**Applied on:** {app.get('applied_at', 'N/A')}")
                        status = app.get("status", "UNKNOWN")
                        if status == "PROCESSED_SECURE":
                            st.success("Status: Securely Processed")
                        elif status == "PROCESSING_ERROR":
                            st.error("Status: Processing Error - Please reapply")
                        else:
                            st.info(f"Status: {status}")

    elif role in ["RECRUITER", "HR"]:
        st.title("Recruiter Dashboard")
        st.info("Profilurile candidatilor anonimizate/procesate de AI")
        
        jobs = fetch_jobs()
        job_options = {j.get("title", f"Job #{j.get('JOB_ID')}"): j.get("JOB_ID") for j in jobs}
        selected_job_title = st.selectbox("Select a Job to view applicants:", list(job_options.keys()))
        
        if selected_job_title:
            selected_job_id = job_options[selected_job_title]
            applicants = fetch_job_applicants(selected_job_id)
            
            if not applicants:
                st.warning("No securely processed applications yet for this job.")
            else:
                for app in applicants:
                    app_id = app.get("app_id") or app.get("APP_ID")
                    secure_text = app.get("secure_resume_text") or app.get("SECURE_RESUME_TEXT") or "{}"
                    status = app.get("status") or app.get("STATUS")
                    
                    with st.expander(f"Candidate Profile (App #{app_id}) - Status: {app.get('status')}"):
                        try:
                            # parsam JSON-ul generat de Mistral
                            ai_data = json.loads(secure_text)
                            st.write(f"**Summary:** {ai_data.get('summary', 'N/A')}")
                            st.write("**Skills:**")
                            st.write(", ".join(ai_data.get('skills', [])))
                            
                            st.write("**Experience:**")
                            for exp in ai_data.get('experience', []):
                                st.write(f"- *{exp.get('role')}* ({exp.get('duration')}): {exp.get('responsibilities')}")
                                
                            st.write("**Education:**")
                            for edu in ai_data.get('education', []):
                                st.write(f"- {edu}")
                        except:                         
                            st.text(secure_text)
                            
                        if st.button("📧 Invite to Interview (Re-identify)", key=f"invite_{app_id}"):
                            if invite_candidate_to_interview(app_id):
                                st.success("Interview Invitation sent! The candidate's email was automatically extracted from the secure database.")
                            else:
                                st.error("Failed to send invitation.")