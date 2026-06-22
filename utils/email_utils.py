import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

SMTP_SERVER = os.getenv("SMTP_SERVER")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")

def send_email(email, subject, body):
    """trimite email"""
    msg = MIMEMultipart()
    msg['From'] = EMAIL_SENDER
    msg['To'] = email
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    try:
        print("SMTP_SERVER",SMTP_SERVER)
        print("SMTP_PORT", SMTP_PORT)
        print("EMAIL_SENDER", EMAIL_SENDER)
        print("EMAIL_PASSWORD", EMAIL_PASSWORD)
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)
        server.quit()
        print(f"[EMAIL SENT] Notification sent to {email}")
    except Exception as e:
        print(f"[EMAIL ERROR] Failed to send notification to {email}: {e}")