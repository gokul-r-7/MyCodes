import smtplib
import logging
from datetime import datetime
# import sys
# sys.path.append(r"C:\Users\chandragiri.sandeep\GitHub\Repos\integration-platform\infra\src\helpers")

from worley_helper.utils.constants import TIMESTAMP_FORMAT, TIMEZONE_SYDNEY, TIMEZONE_UTC
from worley_helper.utils.date_utils import generate_timestamp_string
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from worley_helper.utils.logger import get_logger


# Init the logger
logger = get_logger(__name__)

def send_email(sender_email, receiver_email, subject, body, smtp_server, smtp_port, file_content=None, file_name=None, attach_file=False):
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = ", ".join(receiver_email)
    msg["Subject"] = subject
    msg.attach(MIMEText(body, "plain"))

    if attach_file:
        logger.info(f"Attaching the {file_name=} with the email")
        msg.attach(MIMEApplication(file_content.getvalue(), Name=file_name))

    try:
        logger.info("Connecting to the SMTP server...")
        server = smtplib.SMTP(smtp_server, smtp_port)
        
        server.sendmail(sender_email, receiver_email, msg.as_string())
        logger.info("Email sent successfully.")

    except Exception as e:
        logger.error(f"Error sending email: {e}")
        raise

    finally:
        if 'server' in locals():
            server.quit()


def generate_failure_email_body(env, source_sys, target_sys, functional_area, request_run_id, vouchers_data, error_message, interface_name=''):

    subject = f"Failure Notification: AWS Data Platform {env.upper()} - RIL MIN Integration - {functional_area}"
    email_body = f"""
    Hi,

    This is an email from AWS Data Platform - {env.upper()} to confirm that the following process on data transfer from ({source_sys} to {target_sys}) interface has completed with a Failure.

    Failure Details:
        Process ID: {request_run_id}
        Time of Failure:{generate_timestamp_string()}
        Processed MIN Number(s) - {vouchers_data[0] if vouchers_data[0] else 'Nil'}
        UnProcessed MIN Number(s) - {vouchers_data[1] if vouchers_data[1] else 'Nil'}
        Time Zone: UTC

    Failure Reason: {error_message}
 
    Thanks,
    AWS Data Platform Support
    AWSDataPlatformSupport@Worley.com
    """
    return subject, email_body


def generate_success_email_body(env, source_sys, target_sys, functional_area, request_run_id, time_stamp, success_reason, interface_name=''):

    subject = f"Success Notification: AWS Data Platform {env.upper()} - RIL MIN Integration - {functional_area}"

    email_body = f"""
    Hi,

    The Data Pipeline run for {source_sys} to {target_sys} Interface got completed Successfully.

    Details:
        Process ID: {request_run_id}
        Status: SUCCESS
        Reason: {success_reason}
        timestamp: {time_stamp}
        timezon: UTC

    Thanks,
    AWS Data Platform Support
    AWSDataPlatformSupport@Worley.com
    """
    return subject, email_body

def generate_generic_success_body(env, source_sys, target_sys, functional_area, request_run_id, message):
    subject = f"Success Notification: AWS Data Platform {env.upper()} - {functional_area}"

    formatted_msg = "\n".join([f"    • **{k}**: {v}" for k, v in message.items()])

    email_body = f"""
    Hi,

    The Data Pipeline run for {source_sys} to {target_sys} Interface got completed Successfully.

    Details:
        • **Process ID**: {request_run_id}
        • **Status**: SUCCESS
        • **Timestamp**: {generate_timestamp_string()}
        • **Time Zone**: UTC

    Run Details:
    {message}

    Thanks,
    AWS Data Platform Support
    AWSDataPlatformSupport@Worley.com
    """
    return subject, email_body

def generate_generic_failure_body(env, source_sys, target_sys, functional_area, request_run_id, error_message):

    subject = f"Failure Notification: AWS Data Platform {env.upper()} - {functional_area}"
    formatted_msg = "\n".join([f"    • **{k}**: {v}" for k, v in error_message.items()])
    email_body = f"""
    Hi,

    This is an email from AWS Data Platform - {env.upper()} to confirm that the following process on data transfer from ({source_sys} to {target_sys}) interface has Failed.

    Failure Details:
        • **Process ID**: {request_run_id}
        • **Status**: FAILED
        • **Timestamp**: {generate_timestamp_string()}
        • **Time Zone**: UTC

    Failure Reason: 
    {error_message}
 
    Thanks,
    AWS Data Platform Support
    AWSDataPlatformSupport@Worley.com
    """
    return subject, email_body

def generate_generic_warning_body(env, source_sys, target_sys, functional_area, request_run_id, error_message):
    subject = f"Warning Notification: AWS Data Platform {env.upper()} - {functional_area}"
    formatted_msg = "\n".join([f"    • **{k}**: {v}" for k, v in error_message.items()])
    email_body = f"""
    Hi,

    This is an email from AWS Data Platform - {env.upper()} to confirm that the following process on data transfer from ({source_sys} to {target_sys}) interface has Finished with Warning.

    Warning Details:
        • **Process ID**: {request_run_id}
        • **Status**: Finished with Warning
        • **Timestamp**: {generate_timestamp_string()}
        • **Time Zone**: UTC

    Warning Reason: 
    {error_message}
 
    Thanks,
    AWS Data Platform Support
    AWSDataPlatformSupport@Worley.com
    """
    return subject, email_body



# if __name__ == "__main__":

#     sender_email="chandragiri.sandeep@worley.com"
#     receiver_email=["chandragiri.sandeep@worley.com"]

#     subject, body=generate_failure_email_body("DEV", "INT9999", "source1", "target1", "Ingest", "abcdefghijk", "testing functionality")
#     smtp_server="smtp.worley.com"
#     smtp_port=25

#     # generate stringIO input
#     import io
#     import csv

    # data = [[u'cell one', u'cell two'], [u'cell three', u'cell four']]

    # file_content = io.StringIO()
    # writer = csv.writer(file_content, delimiter=',')
    # writer.writerows(data)

    # file_name="errors_collection_data.csv"
#     attach_file=True

#     send_email(sender_email, receiver_email, subject, body, smtp_server, smtp_port, file_content, file_name, attach_file)