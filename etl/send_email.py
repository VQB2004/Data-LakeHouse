
from prefect import task
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()

@task
def send_email_notification(success=True, error=None):
    # Email configuration
    sender_email = os.getenv("EMAIL")
    receiver_email = os.getenv("EMAIL")
    password = os.getenv("PASS")

    # Nội dung email
    now =datetime.now()
    if success:
        subject = "[ETL thành công] Pipeline đã hoàn tất"
        body_html = f"""
        <html>
          <body>
            <h2 style="color:green;">✅  Pipeline ETL Hoàn Thành</h2>
            <p><b>Thời gian:</b> {now}</p>
          </body>
        </html>
        """
    else:
        subject = "[ETL THẤT BẠI] Pipeline gặp sự cố"
        body_html = f"""
        <html>
          <body>
            <h2 style="color:red;">❌ ETL Pipeline thất bại</h2>
            <p><b>Thời gian:</b> {now}</p>
            <p><b>Lỗi:</b> {error}</p>
          </body>
        </html>
        """

    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject
    
    msg.attach(MIMEText(body_html, 'html'))

    try:
        # Connect to the SMTP server and send the email
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
        print("Đã gửi email thành công.")
    except Exception as e:
        print(f"Gửi email thất bại {e}")
